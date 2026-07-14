"""Strict v1 Feed validation and observation construction."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping

from .canonical import (
    canonical_file_bytes,
    canonical_jsonl_line,
    canonicalize_categories,
    normalize_categories,
    normalize_multiline,
    normalize_single_line,
    require_sha256,
    sha256_bytes,
)
from .identity import (
    base_id_from_oai_identifier,
    logical_record_key,
    observation_id,
    parse_arxiv_id,
    producer_retract_observation_id,
    record_content_sha256,
)


CONTRACT_VERSION = "1.0"
RECORD_SCHEMA_VERSION = "1.0"
SCOPE_SCHEMA_VERSION = "1.0"
FEED_ID = "physical-ai-arxiv-facts-v1"
MAX_RECORD_LINE_BYTES = 2 * 1024 * 1024

OAI_OPERATIONS = {"upsert", "scope_exit", "source_delete"}
ALL_OPERATIONS = OAI_OPERATIONS | {"producer_retract"}
FIELD_PROVENANCE_VALUES = {"oai_arxiv_raw", "search_api_optional"}


class ContractValidationError(ValueError):
    """Raised when a value violates the Feed contract."""


def _fail(message: str) -> None:
    raise ContractValidationError(message)


def _object(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(f"{field} must be an object")
    return value


def _exact_keys(
    value: Mapping[str, Any],
    *,
    field: str,
    required: Iterable[str],
    optional: Iterable[str] = (),
) -> None:
    required_set = set(required)
    optional_set = set(optional)
    missing = required_set - set(value)
    extra = set(value) - required_set - optional_set
    if missing:
        _fail(f"{field} missing keys: {sorted(missing)}")
    if extra:
        _fail(f"{field} has unknown keys: {sorted(extra)}")


def _nonempty_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or value == "":
        _fail(f"{field} must be a non-empty string")
    return value


def _nullable_normalized(
    value: Any, *, field: str, multiline: bool = False
) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        _fail(f"{field} must be a string or null")
    expected = normalize_multiline(value) if multiline else normalize_single_line(value)
    if expected != value:
        _fail(f"{field} is not contract-normalized")
    return value


def _iso_date(value: Any, *, field: str) -> str:
    text = _nonempty_string(value, field=field)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ContractValidationError(f"{field} must be YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        _fail(f"{field} must be canonical YYYY-MM-DD")
    return text


def _utc_timestamp(value: Any, *, field: str) -> str:
    text = _nonempty_string(value, field=field)
    if not text.endswith("Z"):
        _fail(f"{field} must be RFC 3339 UTC ending in Z")
    try:
        datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise ContractValidationError(f"{field} must be an RFC 3339 timestamp") from exc
    return text


def _normalized_string_list(value: Any, *, field: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        _fail(f"{field} must be an array of strings")
    expected = normalize_categories(value)
    if expected != value:
        _fail(f"{field} must be normalized, unique, and sorted")
    return value


def _validate_version_history(value: Any, *, current_version: int) -> None:
    if not isinstance(value, list) or not value:
        _fail("record.version_history must be a non-empty array")
    versions: list[int] = []
    for index, item in enumerate(value):
        item = _object(item, field=f"record.version_history[{index}]")
        _exact_keys(
            item,
            field=f"record.version_history[{index}]",
            required=("version", "submitted_at"),
        )
        version = item["version"]
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            _fail("version history version must be a positive integer")
        _utc_timestamp(
            item["submitted_at"],
            field=f"record.version_history[{index}].submitted_at",
        )
        versions.append(version)
    if versions != sorted(set(versions)):
        _fail("version history must be strictly increasing")
    if versions[-1] != current_version:
        _fail("version history must end at current_version")


def _validate_metadata(value: Any) -> None:
    metadata = _object(value, field="record.metadata")
    _exact_keys(
        metadata,
        field="record.metadata",
        required=(
            "title",
            "abstract",
            "authors_raw",
            "authors",
            "primary_category",
            "categories",
            "comment",
            "journal_ref",
            "doi",
            "license",
        ),
    )
    _nullable_normalized(metadata["title"], field="record.metadata.title")
    _nullable_normalized(
        metadata["abstract"], field="record.metadata.abstract", multiline=True
    )
    _nullable_normalized(metadata["authors_raw"], field="record.metadata.authors_raw")
    _nullable_normalized(
        metadata["primary_category"], field="record.metadata.primary_category"
    )
    categories = _normalized_string_list(
        metadata["categories"], field="record.metadata.categories"
    )
    if not categories:
        _fail("record.metadata.categories cannot be empty for upsert")
    _nullable_normalized(
        metadata["comment"], field="record.metadata.comment", multiline=True
    )
    _nullable_normalized(metadata["journal_ref"], field="record.metadata.journal_ref")
    _nullable_normalized(metadata["doi"], field="record.metadata.doi")
    _nullable_normalized(metadata["license"], field="record.metadata.license")

    authors = metadata["authors"]
    if authors is not None:
        if not isinstance(authors, list):
            _fail("record.metadata.authors must be an array or null")
        for index, author in enumerate(authors):
            author = _object(author, field=f"record.metadata.authors[{index}]")
            _exact_keys(
                author,
                field=f"record.metadata.authors[{index}]",
                required=("name",),
            )
            name = _nonempty_string(
                author["name"], field=f"record.metadata.authors[{index}].name"
            )
            if normalize_single_line(name) != name:
                _fail(f"record.metadata.authors[{index}].name is not normalized")


def _validate_field_provenance(value: Any) -> None:
    provenance = _object(value, field="record.field_provenance")
    _exact_keys(
        provenance,
        field="record.field_provenance",
        required=("version_history", "authors_raw", "authors", "primary_category"),
    )
    for field, source in provenance.items():
        if source not in FIELD_PROVENANCE_VALUES:
            _fail(f"unsupported provenance for {field}: {source!r}")
    if provenance["version_history"] != "oai_arxiv_raw":
        _fail("version_history provenance must be oai_arxiv_raw")
    if provenance["authors_raw"] != "oai_arxiv_raw":
        _fail("authors_raw provenance must be oai_arxiv_raw")


def _validate_oai_record(record: Mapping[str, Any]) -> None:
    _exact_keys(
        record,
        field="record",
        required=(
            "operation",
            "logical_record_key",
            "oai_identifier",
            "source_datestamp",
            "source_sets",
            "base_arxiv_id",
            "versioned_arxiv_id",
            "current_version",
            "version_history",
            "metadata",
            "field_provenance",
        ),
    )
    operation = record["operation"]
    if operation not in OAI_OPERATIONS:
        _fail(f"unsupported OAI operation: {operation!r}")
    oai_identifier = _nonempty_string(
        record["oai_identifier"], field="record.oai_identifier"
    )
    base_id = base_id_from_oai_identifier(oai_identifier)
    if record["base_arxiv_id"] != base_id:
        _fail("base_arxiv_id does not match oai_identifier")
    if record["logical_record_key"] != logical_record_key(oai_identifier):
        _fail("logical_record_key does not match oai_identifier")
    _iso_date(record["source_datestamp"], field="record.source_datestamp")
    _normalized_string_list(record["source_sets"], field="record.source_sets")

    if operation == "upsert":
        current_version = record["current_version"]
        if (
            not isinstance(current_version, int)
            or isinstance(current_version, bool)
            or current_version < 1
        ):
            _fail("record.current_version must be a positive integer")
        versioned = parse_arxiv_id(record["versioned_arxiv_id"])
        if versioned.base_id != base_id or versioned.version != current_version:
            _fail("versioned_arxiv_id does not match base/current version")
        _validate_version_history(
            record["version_history"], current_version=current_version
        )
        _validate_metadata(record["metadata"])
        _validate_field_provenance(record["field_provenance"])
    else:
        if record["versioned_arxiv_id"] is not None:
            versioned = parse_arxiv_id(record["versioned_arxiv_id"])
            if versioned.base_id != base_id:
                _fail("terminal versioned_arxiv_id has the wrong base id")
        if record["current_version"] is not None and (
            not isinstance(record["current_version"], int)
            or isinstance(record["current_version"], bool)
            or record["current_version"] < 1
        ):
            _fail("terminal current_version must be a positive integer or null")
        if record["version_history"] not in (None, []):
            _fail("terminal version_history must be null or empty")
        if record["metadata"] is not None:
            _validate_metadata(record["metadata"])
        if record["field_provenance"] is not None:
            _validate_field_provenance(record["field_provenance"])


def _validate_retract_record(record: Mapping[str, Any]) -> None:
    _exact_keys(
        record,
        field="record",
        required=(
            "operation",
            "logical_record_key",
            "producer_correction_key",
            "target_observation_id",
            "target_source_date",
            "producer_observed_at",
            "reason_code",
            "reason",
            "replacement_observation_id",
        ),
    )
    if record["operation"] != "producer_retract":
        _fail("retract record operation must be producer_retract")
    require_sha256(record["logical_record_key"], field="record.logical_record_key")
    _nonempty_string(
        record["producer_correction_key"], field="record.producer_correction_key"
    )
    require_sha256(
        record["target_observation_id"], field="record.target_observation_id"
    )
    _iso_date(record["target_source_date"], field="record.target_source_date")
    _utc_timestamp(record["producer_observed_at"], field="record.producer_observed_at")
    _nonempty_string(record["reason_code"], field="record.reason_code")
    reason = _nonempty_string(record["reason"], field="record.reason")
    if normalize_multiline(reason) != reason or len(reason) > 1000:
        _fail("record.reason must be normalized and at most 1000 characters")
    replacement = record["replacement_observation_id"]
    if replacement is not None:
        require_sha256(replacement, field="record.replacement_observation_id")


def validate_record(record: Any) -> dict[str, Any]:
    record = _object(record, field="record")
    operation = record.get("operation")
    if operation not in ALL_OPERATIONS:
        _fail(f"unsupported operation: {operation!r}")
    if operation == "producer_retract":
        _validate_retract_record(record)
    else:
        _validate_oai_record(record)
    return record


def build_observation(
    *,
    scope_id: str,
    record: dict[str, Any],
    supersedes_observation_id: str | None,
) -> dict[str, Any]:
    validate_record(record)
    if supersedes_observation_id is not None:
        require_sha256(
            supersedes_observation_id, field="supersedes_observation_id"
        )
    content_hash = record_content_sha256(record)
    if record["operation"] == "producer_retract":
        identity = producer_retract_observation_id(
            scope_id=scope_id,
            producer_correction_key=record["producer_correction_key"],
            target_observation_id=record["target_observation_id"],
            record_content_hash=content_hash,
        )
    else:
        identity = observation_id(
            scope_id=scope_id,
            oai_identifier=record["oai_identifier"],
            source_datestamp=record["source_datestamp"],
            operation=record["operation"],
            record_content_hash=content_hash,
            supersedes_observation_id=supersedes_observation_id,
        )
    envelope = {
        "record_schema_version": RECORD_SCHEMA_VERSION,
        "observation_id": identity,
        "record_content_sha256": content_hash,
        "supersedes_observation_id": supersedes_observation_id,
        "record": record,
    }
    if len(canonical_jsonl_line(envelope)) > MAX_RECORD_LINE_BYTES:
        _fail("observation exceeds the 2 MiB canonical-line limit")
    return envelope


def validate_observation(value: Any, *, scope_id: str) -> dict[str, Any]:
    envelope = _object(value, field="observation")
    _exact_keys(
        envelope,
        field="observation",
        required=(
            "record_schema_version",
            "observation_id",
            "record_content_sha256",
            "supersedes_observation_id",
            "record",
        ),
    )
    if envelope["record_schema_version"] != RECORD_SCHEMA_VERSION:
        _fail("unsupported record_schema_version")
    require_sha256(envelope["observation_id"], field="observation.observation_id")
    require_sha256(
        envelope["record_content_sha256"],
        field="observation.record_content_sha256",
    )
    supersedes = envelope["supersedes_observation_id"]
    if supersedes is not None:
        require_sha256(supersedes, field="observation.supersedes_observation_id")
    record = validate_record(envelope["record"])
    expected = build_observation(
        scope_id=scope_id,
        record=record,
        supersedes_observation_id=supersedes,
    )
    if expected != envelope:
        _fail("observation hashes or identity do not match canonical content")
    return envelope


def validate_scope(value: Any) -> dict[str, Any]:
    scope = _object(value, field="scope")
    _exact_keys(
        scope,
        field="scope",
        required=(
            "contract_version",
            "scope_schema_version",
            "feed_id",
            "scope_id",
            "categories",
            "category_aliases",
            "include_crosslists",
            "keywords",
            "normalizer_version",
        ),
    )
    if scope["contract_version"] != CONTRACT_VERSION:
        _fail("unsupported contract_version")
    if scope["scope_schema_version"] != SCOPE_SCHEMA_VERSION:
        _fail("unsupported scope_schema_version")
    if scope["feed_id"] != FEED_ID:
        _fail("unexpected feed_id")
    _nonempty_string(scope["scope_id"], field="scope.scope_id")
    categories = _normalized_string_list(scope["categories"], field="scope.categories")
    if not categories:
        _fail("scope.categories cannot be empty")
    aliases = _object(scope["category_aliases"], field="scope.category_aliases")
    for key, target in aliases.items():
        if normalize_single_line(key) != key or normalize_single_line(target) != target:
            _fail("scope category aliases must be normalized")
    if canonicalize_categories(categories, aliases) != categories:
        _fail("scope.categories must already use canonical alias targets")
    if scope["include_crosslists"] is not True:
        _fail("Feed v1 requires include_crosslists=true")
    if scope["keywords"] != []:
        _fail("remote Feed scope must not contain keywords")
    if scope["normalizer_version"] != "arxiv-feed-normalizer-v1":
        _fail("unsupported normalizer_version")
    return scope


def scope_exact_sha256(scope: Mapping[str, Any]) -> str:
    validate_scope(dict(scope))
    return sha256_bytes(canonical_file_bytes(scope))
