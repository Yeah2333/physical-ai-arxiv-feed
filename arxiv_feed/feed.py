"""Feed snapshot assembly, loading, and full hash-closure validation."""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .canonical import (
    canonical_file_bytes,
    canonical_jsonl_line,
    normalize_multiline,
    normalize_single_line,
    require_sha256,
    sha256_bytes,
)
from .contract import (
    CONTRACT_VERSION,
    FEED_ID,
    build_observation,
    scope_exact_sha256,
    validate_observation,
    validate_scope,
)
from .enrichment import (
    EnrichmentError,
    SearchOutcome,
    apply_search_result,
    backlog_oldest_age_hours,
    build_task,
    target_material_sha256,
    transition_failure,
    validate_task,
)
from .oai import HarvestResult, SOURCE_SCHEMA_SHA256, SOURCE_SCHEMA_URL
from .projection import Head, Membership, fold_observations, project_records
from .writer import build_data_parts, build_jsonl_shards, write_canonical_file


INDEX_SCHEMA_VERSION = "1.0"
MANIFEST_SCHEMA_VERSION = "1.0"
STATE_SCHEMA_VERSION = "1.0"
FEED_ROOT_SCHEMA_VERSION = "1.0"


class FeedValidationError(ValueError):
    """A snapshot is incomplete, inconsistent, non-canonical, or hash-invalid."""


@dataclass(frozen=True)
class ProducerIdentity:
    code_sha: str
    config_sha256: str
    schema_sha256: str
    workflow_run_id: str


@dataclass(frozen=True)
class BuiltSnapshot:
    feed_root_sha256: str
    index_sha256: str
    closed_complete_through: str | None
    provisional_dates: list[str]
    changed_partitions: list[str]
    enrichment_backlog_count: int = 0
    enrichment_oldest_age_hours: float | None = None
    source_text_repairs: tuple[dict[str, object], ...] = ()


def _load_json(path: Path, *, canonical: bool = True) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise FeedValidationError(f"cannot read {path}") from exc
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FeedValidationError(f"invalid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise FeedValidationError(f"expected object: {path}")
    if canonical and canonical_file_bytes(value) != payload:
        raise FeedValidationError(f"non-canonical JSON bytes: {path}")
    return value


def _verify_file(root: Path, relative_path: str, expected_hash: str) -> bytes:
    require_sha256(expected_hash, field="expected_hash")
    path = root / relative_path
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise FeedValidationError(f"missing referenced file: {relative_path}") from exc
    if sha256_bytes(payload) != expected_hash:
        raise FeedValidationError(f"hash mismatch: {relative_path}")
    return payload


def _validate_relative_path(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value or value.startswith("/"):
        raise FeedValidationError(f"{field} must be a non-empty relative path")
    path = Path(value)
    if ".." in path.parts or path.as_posix() != value:
        raise FeedValidationError(f"unsafe {field}")
    return value


def _date(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise FeedValidationError(f"{field} must be a date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise FeedValidationError(f"invalid {field}") from exc
    if parsed.isoformat() != value:
        raise FeedValidationError(f"non-canonical {field}")
    return value


def _timestamp(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise FeedValidationError(f"{field} must be an RFC3339 UTC timestamp")
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise FeedValidationError(f"invalid {field}") from exc
    return value


def _non_negative(value: Any, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise FeedValidationError(f"{field} must be a non-negative integer")
    return value


def _exact_keys(value: Mapping[str, Any], *, required: set[str], field: str) -> None:
    missing = required - set(value)
    extra = set(value) - required
    if missing or extra:
        raise FeedValidationError(
            f"{field} keys mismatch; missing={sorted(missing)} extra={sorted(extra)}"
        )


def canonical_tree_sha256(paths: Iterable[Path], *, base: Path) -> str:
    entries: list[dict[str, str]] = []
    for path in sorted(paths, key=lambda item: item.relative_to(base).as_posix()):
        entries.append(
            {
                "path": path.relative_to(base).as_posix(),
                "sha256": sha256_bytes(path.read_bytes()),
            }
        )
    return sha256_bytes(canonical_file_bytes(entries))


def load_part_observations(root: Path, data_parts: list[dict[str, Any]], *, scope_id: str) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    expected_ordinal = 0
    previous_key: tuple[str, str] | None = None
    for descriptor in data_parts:
        required = {
            "ordinal", "path", "compressed_size", "compressed_sha256",
            "uncompressed_size", "uncompressed_sha256", "record_count",
        }
        if not isinstance(descriptor, dict):
            raise FeedValidationError("data part descriptor must be an object")
        _exact_keys(descriptor, required=required, field="data_part")
        if descriptor["ordinal"] != expected_ordinal:
            raise FeedValidationError("data part ordinals must be contiguous")
        expected_ordinal += 1
        relative = _validate_relative_path(descriptor["path"], field="data_part.path")
        compressed = _verify_file(root, relative, descriptor["compressed_sha256"])
        if len(compressed) != _non_negative(descriptor["compressed_size"], field="compressed_size"):
            raise FeedValidationError("compressed size mismatch")
        try:
            uncompressed = gzip.decompress(compressed)
        except (OSError, EOFError) as exc:
            raise FeedValidationError("invalid gzip data part") from exc
        if len(uncompressed) != _non_negative(descriptor["uncompressed_size"], field="uncompressed_size"):
            raise FeedValidationError("uncompressed size mismatch")
        if sha256_bytes(uncompressed) != descriptor["uncompressed_sha256"]:
            raise FeedValidationError("uncompressed hash mismatch")
        if uncompressed and not uncompressed.endswith(b"\n"):
            raise FeedValidationError("JSONL part is missing final LF")
        lines = uncompressed.splitlines(keepends=True)
        if len(lines) != _non_negative(descriptor["record_count"], field="record_count"):
            raise FeedValidationError("record count mismatch")
        for line in lines:
            try:
                observation = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise FeedValidationError("invalid JSONL observation") from exc
            if canonical_jsonl_line(observation) != line:
                raise FeedValidationError("non-canonical JSONL observation")
            validate_observation(observation, scope_id=scope_id)
            key = (
                observation["record"]["logical_record_key"],
                observation["observation_id"],
            )
            if previous_key is not None and key <= previous_key:
                raise FeedValidationError("observations are not globally strictly sorted")
            previous_key = key
            observations.append(observation)
    return observations


def validate_manifest(manifest: dict[str, Any], *, scope_id: str, source_date: str) -> None:
    _exact_keys(
        manifest,
        field="manifest",
        required={
            "contract_version", "manifest_schema_version", "feed_id", "scope_id",
            "source_date", "partition_state", "projection_base_hash",
            "projection_result_hash", "query", "counts", "enrichment", "data_parts",
            "producer", "previous_manifest_sha256",
        },
    )
    if manifest["contract_version"] != CONTRACT_VERSION or manifest["manifest_schema_version"] != MANIFEST_SCHEMA_VERSION:
        raise FeedValidationError("unsupported manifest contract")
    if manifest["feed_id"] != FEED_ID or manifest["scope_id"] != scope_id:
        raise FeedValidationError("manifest feed/scope mismatch")
    if _date(manifest["source_date"], field="source_date") != source_date:
        raise FeedValidationError("manifest source_date mismatch")
    if manifest["partition_state"] not in {"provisional", "closed_complete"}:
        raise FeedValidationError("invalid partition state")
    require_sha256(manifest["projection_base_hash"], field="projection_base_hash")
    require_sha256(manifest["projection_result_hash"], field="projection_result_hash")
    previous = manifest["previous_manifest_sha256"]
    if previous is not None:
        require_sha256(previous, field="previous_manifest_sha256")
    query = manifest["query"]
    if not isinstance(query, dict):
        raise FeedValidationError("query must be an object")
    _exact_keys(query, field="query", required={
        "from", "until", "metadata_prefix", "set", "first_response_date",
        "last_response_date", "page_count", "token_exhausted", "source_schema_url",
        "source_schema_sha256", "observed_complete_at",
    })
    if _date(query["from"], field="query.from") != source_date or query["until"] != source_date:
        raise FeedValidationError("query window must equal source date")
    if query["metadata_prefix"] != "arXivRaw" or query["set"] is not None:
        raise FeedValidationError("query must use the global arXivRaw change stream")
    _timestamp(query["first_response_date"], field="first_response_date")
    _timestamp(query["last_response_date"], field="last_response_date")
    _timestamp(query["observed_complete_at"], field="observed_complete_at")
    if _non_negative(query["page_count"], field="page_count") < 1 or query["token_exhausted"] is not True:
        raise FeedValidationError("query pagination is incomplete")
    if query["source_schema_url"] != SOURCE_SCHEMA_URL or query["source_schema_sha256"] != SOURCE_SCHEMA_SHA256:
        raise FeedValidationError("unrecognized source schema")
    counts = manifest["counts"]
    if not isinstance(counts, dict):
        raise FeedValidationError("counts must be an object")
    count_fields = {
        "fresh", "retained", "new", "changed", "superseded", "upsert",
        "scope_exit", "source_delete", "producer_retract",
        "fresh_emitted_operation_count", "effective_source_observation_count",
        "observation_count",
    }
    _exact_keys(counts, field="counts", required=count_fields)
    for field in count_fields:
        _non_negative(counts[field], field=f"counts.{field}")
    enrichment = manifest["enrichment"]
    if not isinstance(enrichment, dict):
        raise FeedValidationError("enrichment must be an object")
    _exact_keys(enrichment, field="enrichment", required={"status", "succeeded", "failed", "deferred"})
    if enrichment["status"] not in {"not_requested", "complete", "partial"}:
        raise FeedValidationError("invalid enrichment status")
    for field in ("succeeded", "failed", "deferred"):
        _non_negative(enrichment[field], field=f"enrichment.{field}")
    if not isinstance(manifest["data_parts"], list):
        raise FeedValidationError("data_parts must be an array")


def _manifest_path(scope_id: str, source_date: str, manifest_hash: str) -> str:
    parsed = date.fromisoformat(source_date)
    digest = manifest_hash.split(":", 1)[1]
    return (
        f"scopes/{scope_id}/partitions/{parsed:%Y/%m}/{source_date}/"
        f"manifests/{digest}.json"
    )


def write_state_root(
    root: Path,
    *,
    scope_id: str,
    state_kind: str,
    coverage_end: str | None,
    records: Iterable[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, Any]]:
    if state_kind not in {"closed_membership", "enrichment_backlog"}:
        raise ValueError("invalid state kind")
    segment = "membership" if state_kind == "closed_membership" else "enrichment"
    key_field = "logical_record_key" if state_kind == "closed_membership" else "task_key"
    shards = build_jsonl_shards(
        root=root,
        records=records,
        key_field=key_field,
        object_prefix=f"scopes/{scope_id}/state/{segment}/objects",
    )
    document = {
        "state_schema_version": STATE_SCHEMA_VERSION,
        "state_kind": state_kind,
        "scope_id": scope_id,
        "coverage_end": coverage_end,
        "shards": shards,
    }
    digest = sha256_bytes(canonical_file_bytes(document))
    relative = f"scopes/{scope_id}/state/{segment}/roots/{digest.split(':', 1)[1]}.json"
    written = write_canonical_file(root, relative, document)
    if written != digest:
        raise AssertionError("state root hash changed while writing")
    return {"path": relative, "sha256": digest}, document


def _load_manifest(root: Path, reference: dict[str, Any], *, scope_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_date = _date(reference.get("source_date"), field="partition.source_date")
    relative = _validate_relative_path(reference.get("manifest_path"), field="manifest_path")
    expected = reference.get("manifest_sha256")
    payload = _verify_file(root, relative, expected)
    manifest = json.loads(payload)
    if canonical_file_bytes(manifest) != payload:
        raise FeedValidationError("manifest is not canonical")
    validate_manifest(manifest, scope_id=scope_id, source_date=source_date)
    observations = load_part_observations(root, manifest["data_parts"], scope_id=scope_id)
    for observation in observations:
        record = observation["record"]
        if record["operation"] == "producer_retract":
            if record["target_source_date"] != source_date:
                raise FeedValidationError("producer retract is stored in the wrong partition")
        elif record["source_datestamp"] != source_date:
            raise FeedValidationError("source observation is stored in the wrong partition")
    if len(observations) != manifest["counts"]["observation_count"]:
        raise FeedValidationError("manifest observation count mismatch")
    operation_counts = {name: 0 for name in ("upsert", "scope_exit", "source_delete", "producer_retract")}
    for observation in observations:
        operation_counts[observation["record"]["operation"]] += 1
    for operation, count in operation_counts.items():
        if manifest["counts"][operation] != count:
            raise FeedValidationError(f"manifest {operation} count mismatch")
    return manifest, observations


def validate_snapshot(root: Path, *, validate_status: bool = True) -> BuiltSnapshot:
    feed_path = root / "feed.json"
    feed = _load_json(feed_path)
    _exact_keys(feed, field="feed", required={"contract_version", "feed_root_schema_version", "feed_id", "scopes"})
    if feed["contract_version"] != CONTRACT_VERSION or feed["feed_root_schema_version"] != FEED_ROOT_SCHEMA_VERSION or feed["feed_id"] != FEED_ID:
        raise FeedValidationError("unsupported Feed root")
    if not isinstance(feed["scopes"], list) or len(feed["scopes"]) != 1:
        raise FeedValidationError("Feed v1 requires exactly one scope")
    reference = feed["scopes"][0]
    _exact_keys(reference, field="scope_reference", required={"scope_id", "index_path", "index_sha256"})
    scope_id = reference["scope_id"]
    index_path = _validate_relative_path(reference["index_path"], field="index_path")
    index_payload = _verify_file(root, index_path, reference["index_sha256"])
    index = json.loads(index_payload)
    if canonical_file_bytes(index) != index_payload:
        raise FeedValidationError("scope index is not canonical")
    required_index = {
        "contract_version", "index_schema_version", "feed_id", "scope_id",
        "scope_sha256", "generated_at", "snapshot_kind", "coverage", "partitions",
        "closed_membership_root", "enrichment_root", "producer", "previous_index_sha256",
    }
    _exact_keys(index, field="index", required=required_index)
    if index["contract_version"] != CONTRACT_VERSION or index["index_schema_version"] != INDEX_SCHEMA_VERSION or index["feed_id"] != FEED_ID or index["scope_id"] != scope_id:
        raise FeedValidationError("scope index identity mismatch")
    if index["snapshot_kind"] not in {"ordinary", "full_projection_correction"}:
        raise FeedValidationError("unsupported snapshot kind")
    _timestamp(index["generated_at"], field="index.generated_at")
    scope_payload = _verify_file(root, f"scopes/{scope_id}/scope.json", index["scope_sha256"])
    scope = json.loads(scope_payload)
    if canonical_file_bytes(scope) != scope_payload:
        raise FeedValidationError("scope is not canonical")
    validate_scope(scope)
    coverage = index["coverage"]
    if not isinstance(coverage, dict):
        raise FeedValidationError("coverage must be an object")
    _exact_keys(coverage, field="coverage", required={"start_date", "closed_complete_through", "membership_closed_through", "pending_gaps", "provisional_dates"})
    start = _date(coverage["start_date"], field="coverage.start_date")
    closed = coverage["closed_complete_through"]
    membership_closed = coverage["membership_closed_through"]
    if closed is not None:
        _date(closed, field="closed_complete_through")
    if closed != membership_closed:
        raise FeedValidationError("closed and membership frontiers differ")
    for field in ("pending_gaps", "provisional_dates"):
        if not isinstance(coverage[field], list) or coverage[field] != sorted(set(coverage[field])):
            raise FeedValidationError(f"coverage.{field} must be sorted and unique")
        for item in coverage[field]:
            _date(item, field=f"coverage.{field}")

    if not isinstance(index["partitions"], list):
        raise FeedValidationError("partitions must be an array")
    if [item.get("source_date") for item in index["partitions"]] != sorted(item.get("source_date") for item in index["partitions"]):
        raise FeedValidationError("partitions must be sorted")
    all_closed_observations: list[dict[str, Any]] = []
    provisional_values: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    provisional_dates: list[str] = []
    previous_result_hash: str | None = None
    for partition_ref in index["partitions"]:
        manifest, observations = _load_manifest(root, partition_ref, scope_id=scope_id)
        if manifest["partition_state"] == "closed_complete":
            if previous_result_hash is not None and manifest["projection_base_hash"] != previous_result_hash:
                raise FeedValidationError("closed partition projection chain is broken")
            previous_result_hash = manifest["projection_result_hash"]
            all_closed_observations.extend(observations)
        else:
            provisional_dates.append(manifest["source_date"])
            provisional_values.append((manifest, observations))

    memberships, _ = fold_observations(scope_id=scope_id, observations=all_closed_observations)
    expected_membership_ref, membership_document = _state_root_in_memory(
        scope_id=scope_id,
        state_kind="closed_membership",
        coverage_end=closed,
        records=[membership.as_dict() for membership in memberships.values()],
        root=root,
    )
    if expected_membership_ref != index["closed_membership_root"]:
        raise FeedValidationError("closed membership root does not match observation fold")
    _validate_state_reference(root, index["closed_membership_root"], expected_kind="closed_membership", expected_scope=scope_id, expected_coverage=closed)
    enrichment_records = load_state_records(
        root,
        index["enrichment_root"],
        expected_kind="enrichment_backlog",
        expected_scope=scope_id,
        expected_coverage=None,
    )
    if closed is not None and previous_result_hash != index["closed_membership_root"]["sha256"]:
        raise FeedValidationError("closed frontier projection result does not match authoritative root")
    for manifest, observations in provisional_values:
        if manifest["projection_base_hash"] != index["closed_membership_root"]["sha256"]:
            raise FeedValidationError("provisional partition does not use the closed root")
        provisional_memberships, _ = fold_observations(
            scope_id=scope_id,
            observations=all_closed_observations + observations,
        )
        provisional_ref, _ = _state_root_in_memory(
            scope_id=scope_id,
            state_kind="closed_membership",
            coverage_end=manifest["source_date"],
            records=[item.as_dict() for item in provisional_memberships.values()],
            root=root,
        )
        if provisional_ref["sha256"] != manifest["projection_result_hash"]:
            raise FeedValidationError("provisional projection result does not match observation fold")
    if provisional_dates != coverage["provisional_dates"]:
        raise FeedValidationError("provisional date list mismatch")
    feed_hash = sha256_bytes(feed_path.read_bytes())
    if validate_status:
        _validate_status_if_present(
            root,
            scope_id=scope_id,
            feed_hash=feed_hash,
            index_hash=reference["index_sha256"],
            enrichment_records=enrichment_records,
        )
    return BuiltSnapshot(
        feed_root_sha256=feed_hash,
        index_sha256=reference["index_sha256"],
        closed_complete_through=closed,
        provisional_dates=provisional_dates,
        changed_partitions=[],
    )


def _validate_status_if_present(
    root: Path,
    *,
    scope_id: str,
    feed_hash: str,
    index_hash: str,
    enrichment_records: list[dict[str, Any]],
) -> None:
    path = root / f"scopes/{scope_id}/status/latest.json"
    if not path.exists():
        return
    status = _load_json(path)
    required = {
        "status_schema_version", "feed_id", "scope_id",
        "last_workflow_success_at", "latest_query_complete_at",
        "closed_complete_through", "membership_closed_through", "pending_gaps",
        "provisional_dates", "changed_partitions", "snapshot_kind",
        "enrichment_backlog_count", "enrichment_oldest_age_hours",
        "reachable_pack_size_bytes", "estimated_reachable_pack_size_bytes",
        "capacity_status", "operation_counts", "feed_root_sha256",
        "index_sha256", "workflow_run_url",
    }
    _exact_keys(status, field="status", required=required)
    if status["status_schema_version"] != "1.0" or status["feed_id"] != FEED_ID or status["scope_id"] != scope_id:
        raise FeedValidationError("status identity mismatch")
    _timestamp(status["last_workflow_success_at"], field="status.last_workflow_success_at")
    _timestamp(status["latest_query_complete_at"], field="status.latest_query_complete_at")
    if status["closed_complete_through"] is not None:
        _date(status["closed_complete_through"], field="status.closed_complete_through")
    if status["closed_complete_through"] != status["membership_closed_through"]:
        raise FeedValidationError("status frontiers differ")
    for field in ("pending_gaps", "provisional_dates", "changed_partitions"):
        if not isinstance(status[field], list) or status[field] != sorted(set(status[field])):
            raise FeedValidationError(f"status.{field} must be sorted and unique")
        for value in status[field]:
            _date(value, field=f"status.{field}")
    if status["snapshot_kind"] not in {"ordinary", "full_projection_correction"}:
        raise FeedValidationError("invalid status snapshot kind")
    _non_negative(status["enrichment_backlog_count"], field="status.enrichment_backlog_count")
    if status["enrichment_backlog_count"] != len(enrichment_records):
        raise FeedValidationError("status enrichment backlog count mismatch")
    oldest = status["enrichment_oldest_age_hours"]
    if oldest is not None and (not isinstance(oldest, (int, float)) or isinstance(oldest, bool) or oldest < 0):
        raise FeedValidationError("invalid enrichment backlog age")
    if not enrichment_records and oldest is not None:
        raise FeedValidationError("empty enrichment backlog must have null oldest age")
    if enrichment_records:
        if oldest is None:
            raise FeedValidationError("non-empty enrichment backlog must expose oldest age")
        expected_age = backlog_oldest_age_hours(
            enrichment_records,
            now=datetime.fromisoformat(
                status["last_workflow_success_at"][:-1] + "+00:00"
            ),
        )
        if expected_age is None or abs(float(oldest) - expected_age) > 0.01:
            raise FeedValidationError("status enrichment oldest age mismatch")
    reachable = _non_negative(status["reachable_pack_size_bytes"], field="status.reachable_pack_size_bytes")
    estimated = _non_negative(status["estimated_reachable_pack_size_bytes"], field="status.estimated_reachable_pack_size_bytes")
    if estimated < reachable or status["capacity_status"] not in {"ok", "warning", "blocked"}:
        raise FeedValidationError("invalid capacity status")
    if not isinstance(status["operation_counts"], dict):
        raise FeedValidationError("status operation_counts must be an object")
    _exact_keys(
        status["operation_counts"],
        field="status.operation_counts",
        required={"upsert", "scope_exit", "source_delete", "producer_retract"},
    )
    for key, value in status["operation_counts"].items():
        _non_negative(value, field=f"status.operation_counts.{key}")
    if status["feed_root_sha256"] != feed_hash or status["index_sha256"] != index_hash:
        raise FeedValidationError("status root/index hash mismatch")
    if not isinstance(status["workflow_run_url"], str) or not status["workflow_run_url"]:
        raise FeedValidationError("status workflow URL is missing")


def _state_root_in_memory(
    *, scope_id: str, state_kind: str, coverage_end: str | None,
    records: Iterable[dict[str, Any]], root: Path,
) -> tuple[dict[str, str], dict[str, Any]]:
    segment = "membership" if state_kind == "closed_membership" else "enrichment"
    key_field = "logical_record_key" if state_kind == "closed_membership" else "task_key"
    # Recompute exact shard descriptors without mutating an already-published tree.
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        temporary = Path(directory)
        shards = build_jsonl_shards(
            root=temporary,
            records=records,
            key_field=key_field,
            object_prefix=f"scopes/{scope_id}/state/{segment}/objects",
        )
    document = {
        "state_schema_version": STATE_SCHEMA_VERSION,
        "state_kind": state_kind,
        "scope_id": scope_id,
        "coverage_end": coverage_end,
        "shards": shards,
    }
    digest = sha256_bytes(canonical_file_bytes(document))
    relative = f"scopes/{scope_id}/state/{segment}/roots/{digest.split(':', 1)[1]}.json"
    return {"path": relative, "sha256": digest}, document


def _validate_state_reference(root: Path, reference: dict[str, Any], *, expected_kind: str, expected_scope: str, expected_coverage: str | None) -> None:
    _exact_keys(reference, field="state_reference", required={"path", "sha256"})
    relative = _validate_relative_path(reference["path"], field="state_reference.path")
    payload = _verify_file(root, relative, reference["sha256"])
    document = json.loads(payload)
    if canonical_file_bytes(document) != payload:
        raise FeedValidationError("state root is not canonical")
    _exact_keys(document, field="state_root", required={"state_schema_version", "state_kind", "scope_id", "coverage_end", "shards"})
    if document["state_schema_version"] != STATE_SCHEMA_VERSION or document["state_kind"] != expected_kind or document["scope_id"] != expected_scope or document["coverage_end"] != expected_coverage:
        raise FeedValidationError("state root identity mismatch")
    if not isinstance(document["shards"], list):
        raise FeedValidationError("state shards must be an array")
    prefixes = [item.get("prefix") for item in document["shards"]]
    if prefixes != sorted(set(prefixes)):
        raise FeedValidationError("state shard prefixes must be sorted and unique")
    previous_key: str | None = None
    for descriptor in document["shards"]:
        relative_shard = _validate_relative_path(descriptor["path"], field="state shard path")
        compressed = _verify_file(root, relative_shard, descriptor["compressed_sha256"])
        if len(compressed) != descriptor["compressed_size"]:
            raise FeedValidationError("state compressed size mismatch")
        uncompressed = gzip.decompress(compressed)
        if len(uncompressed) != descriptor["uncompressed_size"] or sha256_bytes(uncompressed) != descriptor["uncompressed_sha256"]:
            raise FeedValidationError("state uncompressed proof mismatch")
        lines = uncompressed.splitlines(keepends=True)
        if len(lines) != descriptor["record_count"]:
            raise FeedValidationError("state record count mismatch")
        key_field = "logical_record_key" if expected_kind == "closed_membership" else "task_key"
        for line in lines:
            try:
                record = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise FeedValidationError("invalid state JSONL record") from exc
            if canonical_jsonl_line(record) != line:
                raise FeedValidationError("state JSONL record is not canonical")
            if expected_kind == "closed_membership":
                _validate_membership_state_record(record)
            else:
                try:
                    validate_task(record)
                except Exception as exc:
                    raise FeedValidationError(f"invalid enrichment state record: {exc}") from exc
            key = str(record.get(key_field) or "")
            if not key or (previous_key is not None and key <= previous_key):
                raise FeedValidationError("state records are not globally strictly sorted")
            if key[7:9] != descriptor["prefix"]:
                raise FeedValidationError("state record is stored in the wrong shard")
            previous_key = key


def _validate_membership_state_record(record: Any) -> None:
    if not isinstance(record, dict) or set(record) != {
        "logical_record_key", "ever_in_scope", "active_in_scope", "source_deleted"
    }:
        raise FeedValidationError("invalid closed-membership state record")
    require_sha256(record["logical_record_key"], field="logical_record_key")
    for field in ("ever_in_scope", "active_in_scope", "source_deleted"):
        if not isinstance(record[field], bool):
            raise FeedValidationError(f"membership {field} must be boolean")
    if record["active_in_scope"] and not record["ever_in_scope"]:
        raise FeedValidationError("active membership must have ever_in_scope=true")
    if record["active_in_scope"] and record["source_deleted"]:
        raise FeedValidationError("deleted membership cannot be active")


def load_state_records(
    root: Path,
    reference: dict[str, Any],
    *,
    expected_kind: str,
    expected_scope: str,
    expected_coverage: str | None,
) -> list[dict[str, Any]]:
    _validate_state_reference(
        root,
        reference,
        expected_kind=expected_kind,
        expected_scope=expected_scope,
        expected_coverage=expected_coverage,
    )
    document = json.loads((root / reference["path"]).read_bytes())
    values: list[dict[str, Any]] = []
    for descriptor in document["shards"]:
        payload = gzip.decompress((root / descriptor["path"]).read_bytes())
        values.extend(json.loads(line) for line in payload.splitlines())
    return values


def _counts(existing: list[dict[str, Any]], fresh: list[dict[str, Any]], cumulative: list[dict[str, Any]]) -> dict[str, int]:
    operations = {name: 0 for name in ("upsert", "scope_exit", "source_delete", "producer_retract")}
    retracted: set[str] = set()
    for item in cumulative:
        operation = item["record"]["operation"]
        operations[operation] += 1
        if operation == "producer_retract":
            retracted.add(item["record"]["target_observation_id"])
    source_observations = [
        item for item in cumulative
        if item["record"]["operation"] != "producer_retract" and item["observation_id"] not in retracted
    ]
    changed = sum(item["supersedes_observation_id"] is not None for item in fresh)
    return {
        "fresh": len(fresh),
        "retained": len(existing),
        "new": len(fresh) - changed,
        "changed": changed,
        "superseded": changed,
        **operations,
        "fresh_emitted_operation_count": len(fresh),
        "effective_source_observation_count": len(source_observations),
        "observation_count": len(cumulative),
    }


def _partition_reference(scope_id: str, source_date: str, manifest: dict[str, Any], root: Path) -> dict[str, str]:
    manifest_hash = sha256_bytes(canonical_file_bytes(manifest))
    path = _manifest_path(scope_id, source_date, manifest_hash)
    written = write_canonical_file(root, path, manifest)
    if written != manifest_hash:
        raise AssertionError("manifest hash changed while writing")
    return {"source_date": source_date, "manifest_path": path, "manifest_sha256": manifest_hash}


def _rechain_partition(
    *,
    scope_id: str,
    observations: list[dict[str, Any]],
    base_heads: Mapping[str, Head],
) -> tuple[list[dict[str, Any]], bool]:
    """Rebind a partition's chains to corrected prior heads without guessing order."""
    all_source_items = [
        item for item in observations if item["record"]["operation"] != "producer_retract"
    ]
    retracts = [
        item for item in observations if item["record"]["operation"] == "producer_retract"
    ]
    retracted_ids = {item["record"]["target_observation_id"] for item in retracts}
    retracted_source_items = [
        item for item in all_source_items if item["observation_id"] in retracted_ids
    ]
    source_items = [
        item for item in all_source_items if item["observation_id"] not in retracted_ids
    ]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in source_items:
        grouped.setdefault(item["record"]["logical_record_key"], []).append(item)
    rewritten: list[dict[str, Any]] = []
    changed = False
    for logical_key, items in sorted(grouped.items()):
        by_id = {item["observation_id"]: item for item in items}
        if len(by_id) != len(items):
            raise FeedValidationError("partition contains duplicate observation ids")
        children: dict[str, list[dict[str, Any]]] = {}
        roots: list[dict[str, Any]] = []
        for item in items:
            predecessor = item["supersedes_observation_id"]
            if predecessor in by_id:
                children.setdefault(predecessor, []).append(item)
            else:
                roots.append(item)
        if len(roots) != 1:
            raise FeedValidationError("partition supersedes chain has multiple roots or a cycle")
        current = roots[0]
        visited: set[str] = set()
        expected_predecessor = (
            base_heads[logical_key].observation_id if logical_key in base_heads else None
        )
        while True:
            old_id = current["observation_id"]
            if old_id in visited:
                raise FeedValidationError("partition supersedes chain is cyclic")
            visited.add(old_id)
            if current["supersedes_observation_id"] == expected_predecessor:
                replacement = current
            else:
                replacement = build_observation(
                    scope_id=scope_id,
                    record=current["record"],
                    supersedes_observation_id=expected_predecessor,
                )
                changed = True
            rewritten.append(replacement)
            expected_predecessor = replacement["observation_id"]
            next_items = children.get(old_id, [])
            if not next_items:
                break
            if len(next_items) != 1:
                raise FeedValidationError("partition supersedes chain forks")
            current = next_items[0]
        if len(visited) != len(items):
            raise FeedValidationError("partition supersedes chain is disconnected")
    # Retracted source facts and correction envelopes remain in the immutable
    # cumulative set, but neither participates in the active supersedes chain.
    rewritten.extend(retracted_source_items)
    rewritten.extend(retracts)
    return rewritten, changed


def build_producer_correction(
    root: Path,
    *,
    scope_id: str,
    spec: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    required = {
        "producer_correction_key", "target_observation_id", "producer_observed_at",
        "reason_code", "reason", "replacement_observation_id",
    }
    if set(spec) != required:
        raise FeedValidationError("producer correction spec keys do not match v1")
    if not (root / "feed.json").exists():
        raise FeedValidationError("a producer correction requires an existing Feed")
    validate_snapshot(root)
    feed = _load_json(root / "feed.json")
    index = _load_json(root / feed["scopes"][0]["index_path"])
    all_observations: list[dict[str, Any]] = []
    for reference in index["partitions"]:
        _, observations = _load_manifest(root, reference, scope_id=scope_id)
        all_observations.extend(observations)
    correction_key = normalize_single_line(str(spec["producer_correction_key"]))
    reason_code = normalize_single_line(str(spec["reason_code"]))
    reason = normalize_multiline(str(spec["reason"]))
    if not correction_key or not reason_code or not reason or len(reason) > 1000:
        raise FeedValidationError("producer correction text fields are invalid")
    producer_observed_at = str(spec["producer_observed_at"])
    _timestamp(producer_observed_at, field="producer_observed_at")
    target_id = str(spec["target_observation_id"])
    require_sha256(target_id, field="target_observation_id")
    targets = [item for item in all_observations if item["observation_id"] == target_id]
    if len(targets) != 1 or targets[0]["record"]["operation"] == "producer_retract":
        raise FeedValidationError("producer correction target must be one source observation")
    if any(
        item["record"]["operation"] == "producer_retract"
        and item["record"]["target_observation_id"] == target_id
        for item in all_observations
    ):
        raise FeedValidationError("producer correction target is already retracted")
    if any(
        item["record"]["operation"] == "producer_retract"
        and item["record"]["producer_correction_key"] == correction_key
        for item in all_observations
    ):
        raise FeedValidationError("producer_correction_key is already published")
    target = targets[0]
    replacement = spec["replacement_observation_id"]
    if replacement is not None:
        replacement = str(replacement)
        require_sha256(replacement, field="replacement_observation_id")
        replacements = [
            item for item in all_observations if item["observation_id"] == replacement
        ]
        if len(replacements) != 1:
            raise FeedValidationError("producer correction replacement is absent")
        if (
            replacements[0]["record"]["logical_record_key"]
            != target["record"]["logical_record_key"]
        ):
            raise FeedValidationError("producer correction replacement has wrong logical record")
    source_date = target["record"]["source_datestamp"]
    record = {
        "operation": "producer_retract",
        "logical_record_key": target["record"]["logical_record_key"],
        "producer_correction_key": correction_key,
        "target_observation_id": target_id,
        "target_source_date": source_date,
        "producer_observed_at": producer_observed_at,
        "reason_code": reason_code,
        "reason": reason,
        "replacement_observation_id": replacement,
    }
    observation = build_observation(
        scope_id=scope_id,
        record=record,
        supersedes_observation_id=None,
    )
    return {source_date: [observation]}


def _apply_partition_enrichment(
    *,
    scope_id: str,
    source_date: str,
    existing: list[dict[str, Any]],
    fresh: list[dict[str, Any]],
    tasks: dict[str, dict[str, Any]],
    outcomes: Mapping[str, SearchOutcome],
    consumed_outcomes: set[str],
    enabled: bool,
    generated_at: str,
    today_utc: date,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_observations = [
        item
        for item in existing + fresh
        if item["record"]["operation"] == "upsert"
        and item["record"]["source_datestamp"] == source_date
    ]
    by_id = {item["observation_id"]: item for item in source_observations}
    latest_by_base: dict[str, dict[str, Any]] = {}
    for item in source_observations:
        latest_by_base[item["record"]["base_arxiv_id"]] = item

    relevant = False
    if enabled:
        for base_id, target in latest_by_base.items():
            metadata = target["record"]["metadata"]
            provenance = target["record"]["field_provenance"]
            already_enriched = (
                metadata.get("authors") is not None
                and metadata.get("primary_category") is not None
                and provenance.get("authors") == "search_api_optional"
                and provenance.get("primary_category") == "search_api_optional"
            )
            if already_enriched:
                tasks.pop(base_id, None)
                continue
            relevant = True
            current_task = tasks.get(base_id)
            if (
                current_task is None
                or current_task["target_observation_id"] != target["observation_id"]
            ):
                tasks[base_id] = build_task(
                    target,
                    scope_id=scope_id,
                    first_enqueued_at=generated_at,
                )

    partition_tasks = [
        task for task in tasks.values() if task["target_source_date"] == source_date
    ]
    emitted = list(fresh)
    succeeded_now = 0
    for task in sorted(partition_tasks, key=lambda item: item["task_key"]):
        base_id = str(task["base_arxiv_id"])
        relevant = True
        target = by_id.get(task["target_observation_id"])
        if target is None:
            # A full projection correction may have rebound the predecessor and
            # therefore changed its deterministic observation id. Rebind only
            # when base/version/material still identify one exact target.
            candidates = [
                item
                for item in source_observations
                if item["record"]["base_arxiv_id"] == base_id
                and item["record"]["current_version"] == task["target_current_version"]
                and target_material_sha256(item["record"])
                == task["target_material_sha256"]
            ]
            if len(candidates) == 1:
                target = candidates[0]
                task = dict(task)
                task["target_observation_id"] = target["observation_id"]
                tasks[base_id] = validate_task(task)
            else:
                raise FeedValidationError("enrichment task target cannot be rebound uniquely")
        outcome = outcomes.get(base_id)
        if outcome is None or base_id in consumed_outcomes:
            continue
        consumed_outcomes.add(base_id)
        if not outcome.succeeded:
            tasks[base_id] = transition_failure(
                task,
                attempted_at=generated_at,
                today_utc=today_utc,
                error_kind=str(outcome.error_kind or "search_failure"),
                error=str(outcome.error or "Search enrichment failed"),
            )
            continue
        try:
            assert outcome.record is not None
            enriched = apply_search_result(
                scope_id=scope_id,
                target_observation=target,
                result=outcome.record,
            )
        except EnrichmentError as exc:
            tasks[base_id] = transition_failure(
                task,
                attempted_at=generated_at,
                today_utc=today_utc,
                error_kind=str(exc),
                error=str(exc),
            )
            continue
        emitted.append(enriched)
        by_id[enriched["observation_id"]] = enriched
        tasks.pop(base_id, None)
        succeeded_now += 1

    outstanding = [
        task for task in tasks.values() if task["target_source_date"] == source_date
    ]
    enriched_count = sum(
        1
        for item in existing + emitted
        if item["record"]["operation"] == "upsert"
        and (item["record"].get("field_provenance") or {}).get("authors")
        == "search_api_optional"
    )
    failed = sum(
        1
        for task in outstanding
        if task["attempt_count"] > 0 and task["status"] == "pending"
    )
    deferred = sum(1 for task in outstanding if task["status"] == "deferred")
    status = (
        "partial"
        if outstanding
        else ("complete" if relevant or enriched_count or succeeded_now else "not_requested")
    )
    return emitted, {
        "status": status,
        "succeeded": enriched_count,
        "failed": failed,
        "deferred": deferred,
    }


class SnapshotBuilder:
    def __init__(self, *, root: Path, scope: dict[str, Any], producer: ProducerIdentity) -> None:
        self.root = root
        self.scope = validate_scope(scope)
        self.scope_id = scope["scope_id"]
        self.producer = producer

    def build(
        self,
        *,
        today_utc: date,
        generated_at: str,
        coverage_start: date,
        harvests: Mapping[str, HarvestResult],
        snapshot_kind: str = "ordinary",
        enrichment_enabled: bool = False,
        enrichment_outcomes: Mapping[str, SearchOutcome] | None = None,
        producer_corrections: Mapping[str, list[dict[str, Any]]] | None = None,
    ) -> BuiltSnapshot:
        _timestamp(generated_at, field="generated_at")
        if snapshot_kind not in {"ordinary", "full_projection_correction"}:
            raise FeedValidationError("invalid snapshot kind")
        previous_index: dict[str, Any] | None = None
        old_references: dict[str, dict[str, Any]] = {}
        old_manifests: dict[str, dict[str, Any]] = {}
        old_observations: dict[str, list[dict[str, Any]]] = {}
        enrichment_tasks: dict[str, dict[str, Any]] = {}
        previous_enrichment_ref: dict[str, str] | None = None
        if (self.root / "feed.json").exists():
            validate_snapshot(self.root)
            feed = _load_json(self.root / "feed.json")
            index_ref = feed["scopes"][0]
            previous_index = _load_json(self.root / index_ref["index_path"])
            previous_enrichment_ref = previous_index["enrichment_root"]
            enrichment_tasks = {
                str(item["base_arxiv_id"]): item
                for item in load_state_records(
                    self.root,
                    previous_enrichment_ref,
                    expected_kind="enrichment_backlog",
                    expected_scope=self.scope_id,
                    expected_coverage=None,
                )
            }
            for reference in previous_index["partitions"]:
                manifest, observations = _load_manifest(self.root, reference, scope_id=self.scope_id)
                source_date = reference["source_date"]
                old_references[source_date] = reference
                old_manifests[source_date] = manifest
                old_observations[source_date] = observations
            existing_start = date.fromisoformat(previous_index["coverage"]["start_date"])
            if coverage_start != existing_start:
                raise FeedValidationError("coverage start cannot change")

        for source_date, corrections in (producer_corrections or {}).items():
            if source_date not in old_observations:
                raise FeedValidationError("producer correction target partition is absent")
            by_id = {
                item["observation_id"]: item
                for item in old_observations[source_date]
            }
            for correction in corrections:
                validate_observation(correction, scope_id=self.scope_id)
                if correction["record"]["operation"] != "producer_retract":
                    raise FeedValidationError("producer correction input is not a retract")
                if correction["record"]["target_source_date"] != source_date:
                    raise FeedValidationError("producer correction is assigned to the wrong partition")
                current = by_id.get(correction["observation_id"])
                if current is not None and current != correction:
                    raise FeedValidationError("producer correction id conflicts")
                by_id[correction["observation_id"]] = correction
            old_observations[source_date] = list(by_id.values())

        scope_path = f"scopes/{self.scope_id}/scope.json"
        written_scope_hash = write_canonical_file(self.root, scope_path, self.scope)
        expected_scope_hash = scope_exact_sha256(self.scope)
        if written_scope_hash != expected_scope_hash:
            raise AssertionError("scope hash mismatch")

        partition_dates = set(old_references) | set(harvests)
        current_date = today_utc.isoformat()
        # Closed coverage must remain contiguous from start; later queried dates stay gaps.
        contiguous: list[str] = []
        cursor = coverage_start
        while cursor < today_utc:
            cursor_text = cursor.isoformat()
            old_is_closed = (
                cursor_text in old_manifests
                and old_manifests[cursor_text]["partition_state"] == "closed_complete"
            )
            if cursor_text not in harvests and not old_is_closed:
                break
            contiguous.append(cursor.isoformat())
            cursor += timedelta(days=1)
        closed_through = contiguous[-1] if contiguous else None

        memberships: dict[str, Membership] = {}
        heads: dict[str, Head] = {}
        references: dict[str, dict[str, Any]] = {}
        reference_manifests: dict[str, dict[str, Any]] = {}
        changed_partitions: list[str] = []
        source_text_repairs: list[dict[str, object]] = []
        cascading_rechain = False
        closed_history: list[dict[str, Any]] = []
        prior_root_ref, _ = write_state_root(
            self.root,
            scope_id=self.scope_id,
            state_kind="closed_membership",
            coverage_end=None,
            records=[],
        )
        outcomes = dict(enrichment_outcomes or {})
        consumed_outcomes: set[str] = set()
        for source_date in contiguous:
            existing, rechained = _rechain_partition(
                scope_id=self.scope_id,
                observations=old_observations.get(source_date, []),
                base_heads=heads,
            )
            cascading_rechain = cascading_rechain or rechained
            existing_memberships, existing_heads = fold_observations(
                scope_id=self.scope_id,
                observations=closed_history + existing,
            )
            fresh: list[dict[str, Any]] = []
            if source_date in harvests:
                projected = project_records(
                    scope=self.scope,
                    records=harvests[source_date].records,
                    memberships=existing_memberships,
                    heads=existing_heads,
                )
                fresh = projected.observations
                source_text_repairs.extend(
                    repair.as_dict() for repair in projected.source_text_repairs
                )
            fresh, enrichment_summary = _apply_partition_enrichment(
                scope_id=self.scope_id,
                source_date=source_date,
                existing=existing,
                fresh=fresh,
                tasks=enrichment_tasks,
                outcomes=outcomes,
                consumed_outcomes=consumed_outcomes,
                enabled=enrichment_enabled,
                generated_at=generated_at,
                today_utc=today_utc,
            )
            cumulative_by_id = {item["observation_id"]: item for item in existing}
            for item in fresh:
                current = cumulative_by_id.get(item["observation_id"])
                if current is not None and current != item:
                    raise FeedValidationError("same observation id has conflicting content")
                cumulative_by_id[item["observation_id"]] = item
            cumulative = list(cumulative_by_id.values())
            all_through = closed_history + cumulative
            memberships, heads = fold_observations(
                scope_id=self.scope_id, observations=all_through
            )
            result_root_ref, _ = write_state_root(
                self.root,
                scope_id=self.scope_id,
                state_kind="closed_membership",
                coverage_end=source_date,
                records=[item.as_dict() for item in memberships.values()],
            )
            if source_date in harvests:
                query = self._query(harvests[source_date], source_date)
            else:
                query = old_manifests[source_date]["query"]
            data_parts = build_data_parts(root=self.root, observations=cumulative)
            manifest = {
                "contract_version": CONTRACT_VERSION,
                "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
                "feed_id": FEED_ID,
                "scope_id": self.scope_id,
                "source_date": source_date,
                "partition_state": "closed_complete",
                "projection_base_hash": prior_root_ref["sha256"],
                "projection_result_hash": result_root_ref["sha256"],
                "query": query,
                "counts": _counts(existing, fresh, cumulative),
                "enrichment": enrichment_summary,
                "data_parts": data_parts,
                "producer": self._manifest_producer(),
                "previous_manifest_sha256": old_references.get(source_date, {}).get("manifest_sha256"),
            }
            old_manifest = old_manifests.get(source_date)
            can_retain = (
                old_manifest is not None
                and old_manifest["partition_state"] == "closed_complete"
                and old_manifest["projection_base_hash"] == prior_root_ref["sha256"]
                and old_manifest["projection_result_hash"] == result_root_ref["sha256"]
                and old_observations.get(source_date, []) == cumulative
                and old_manifest["enrichment"] == manifest["enrichment"]
                and not fresh
            )
            new_reference = (
                old_references[source_date]
                if can_retain
                else _partition_reference(self.scope_id, source_date, manifest, self.root)
            )
            references[source_date] = new_reference
            reference_manifests[source_date] = (
                old_manifest if can_retain and old_manifest is not None else manifest
            )
            if old_references.get(source_date) != new_reference:
                changed_partitions.append(source_date)
            old_observations[source_date] = cumulative
            closed_history.extend(cumulative)
            prior_root_ref = result_root_ref

        # A provisional partition is always recomputed from the authoritative closed root.
        if current_date in partition_dates and closed_through == (today_utc - timedelta(days=1)).isoformat():
            existing, rechained = _rechain_partition(
                scope_id=self.scope_id,
                observations=old_observations.get(current_date, []),
                base_heads=heads,
            )
            cascading_rechain = cascading_rechain or rechained
            provisional_memberships, provisional_heads = fold_observations(
                scope_id=self.scope_id,
                observations=closed_history + existing,
            )
            fresh = []
            if current_date in harvests:
                projected = project_records(
                    scope=self.scope,
                    records=harvests[current_date].records,
                    memberships=provisional_memberships,
                    heads=provisional_heads,
                )
                fresh = projected.observations
                source_text_repairs.extend(
                    repair.as_dict() for repair in projected.source_text_repairs
                )
            fresh, enrichment_summary = _apply_partition_enrichment(
                scope_id=self.scope_id,
                source_date=current_date,
                existing=existing,
                fresh=fresh,
                tasks=enrichment_tasks,
                outcomes=outcomes,
                consumed_outcomes=consumed_outcomes,
                enabled=enrichment_enabled,
                generated_at=generated_at,
                today_utc=today_utc,
            )
            cumulative_by_id = {item["observation_id"]: item for item in existing}
            for item in fresh:
                cumulative_by_id[item["observation_id"]] = item
            cumulative = list(cumulative_by_id.values())
            provisional_memberships, _ = fold_observations(
                scope_id=self.scope_id,
                observations=closed_history + cumulative,
            )
            provisional_ref, _ = write_state_root(
                self.root,
                scope_id=self.scope_id,
                state_kind="closed_membership",
                coverage_end=current_date,
                records=[item.as_dict() for item in provisional_memberships.values()],
            )
            query = self._query(harvests[current_date], current_date) if current_date in harvests else old_manifests[current_date]["query"]
            manifest = {
                "contract_version": CONTRACT_VERSION,
                "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
                "feed_id": FEED_ID,
                "scope_id": self.scope_id,
                "source_date": current_date,
                "partition_state": "provisional",
                "projection_base_hash": prior_root_ref["sha256"],
                "projection_result_hash": provisional_ref["sha256"],
                "query": query,
                "counts": _counts(existing, fresh, cumulative),
                "enrichment": enrichment_summary,
                "data_parts": build_data_parts(root=self.root, observations=cumulative),
                "producer": self._manifest_producer(),
                "previous_manifest_sha256": old_references.get(current_date, {}).get("manifest_sha256"),
            }
            old_manifest = old_manifests.get(current_date)
            can_retain = (
                old_manifest is not None
                and old_manifest["partition_state"] == "provisional"
                and old_manifest["projection_base_hash"] == prior_root_ref["sha256"]
                and old_manifest["projection_result_hash"] == provisional_ref["sha256"]
                and old_observations.get(current_date, []) == cumulative
                and old_manifest["enrichment"] == manifest["enrichment"]
                and not fresh
            )
            new_reference = (
                old_references[current_date]
                if can_retain
                else _partition_reference(self.scope_id, current_date, manifest, self.root)
            )
            references[current_date] = new_reference
            reference_manifests[current_date] = (
                old_manifest if can_retain and old_manifest is not None else manifest
            )
            if old_references.get(current_date) != new_reference:
                changed_partitions.append(current_date)

        if cascading_rechain and snapshot_kind != "full_projection_correction":
            raise FeedValidationError(
                "historical correction rewrites downstream supersedes; rerun as full_projection_correction"
            )

        # Retain queried partitions beyond a gap only as old references; a new builder never creates them.
        for source_date, reference in old_references.items():
            references.setdefault(source_date, reference)
            reference_manifests.setdefault(source_date, old_manifests[source_date])

        membership_ref = prior_root_ref
        enrichment_ref, _ = write_state_root(
            self.root,
            scope_id=self.scope_id,
            state_kind="enrichment_backlog",
            coverage_end=None,
            records=enrichment_tasks.values(),
        )
        last_closed = date.fromisoformat(closed_through) if closed_through else coverage_start - timedelta(days=1)
        pending_gaps: list[str] = []
        cursor = last_closed + timedelta(days=1)
        while cursor < today_utc:
            pending_gaps.append(cursor.isoformat())
            cursor += timedelta(days=1)
        provisional_dates = sorted(
            source_date
            for source_date, manifest in reference_manifests.items()
            if manifest["partition_state"] == "provisional"
        )
        enrichment_changed = previous_enrichment_ref != enrichment_ref
        oldest_enrichment_age = backlog_oldest_age_hours(
            enrichment_tasks.values(),
            now=datetime.fromisoformat(generated_at[:-1] + "+00:00"),
        )
        if previous_index is not None and not changed_partitions and not enrichment_changed:
            previous = validate_snapshot(self.root)
            return BuiltSnapshot(
                feed_root_sha256=previous.feed_root_sha256,
                index_sha256=previous.index_sha256,
                closed_complete_through=previous.closed_complete_through,
                provisional_dates=previous.provisional_dates,
                changed_partitions=[],
                enrichment_backlog_count=len(enrichment_tasks),
                enrichment_oldest_age_hours=oldest_enrichment_age,
                source_text_repairs=tuple(source_text_repairs),
            )
        previous_index_sha = None
        if previous_index is not None:
            previous_index_sha = sha256_bytes(canonical_file_bytes(previous_index))
        index = {
            "contract_version": CONTRACT_VERSION,
            "index_schema_version": INDEX_SCHEMA_VERSION,
            "feed_id": FEED_ID,
            "scope_id": self.scope_id,
            "scope_sha256": expected_scope_hash,
            "generated_at": generated_at,
            "snapshot_kind": snapshot_kind,
            "coverage": {
                "start_date": coverage_start.isoformat(),
                "closed_complete_through": closed_through,
                "membership_closed_through": closed_through,
                "pending_gaps": pending_gaps,
                "provisional_dates": provisional_dates,
            },
            "partitions": [references[key] for key in sorted(references)],
            "closed_membership_root": membership_ref,
            "enrichment_root": enrichment_ref,
            "producer": {
                "code_sha": self.producer.code_sha,
                "config_sha256": self.producer.config_sha256,
                "schema_sha256": self.producer.schema_sha256,
            },
            "previous_index_sha256": previous_index_sha,
        }
        index_path = f"scopes/{self.scope_id}/index.json"
        index_sha = write_canonical_file(self.root, index_path, index)
        feed = {
            "contract_version": CONTRACT_VERSION,
            "feed_root_schema_version": FEED_ROOT_SCHEMA_VERSION,
            "feed_id": FEED_ID,
            "scopes": [{"scope_id": self.scope_id, "index_path": index_path, "index_sha256": index_sha}],
        }
        feed_sha = write_canonical_file(self.root, "feed.json", feed)
        result = validate_snapshot(self.root, validate_status=False)
        return BuiltSnapshot(
            feed_root_sha256=feed_sha,
            index_sha256=index_sha,
            closed_complete_through=closed_through,
            provisional_dates=provisional_dates,
            changed_partitions=sorted(changed_partitions),
            enrichment_backlog_count=len(enrichment_tasks),
            enrichment_oldest_age_hours=oldest_enrichment_age,
            source_text_repairs=tuple(source_text_repairs),
        )

    def _manifest_producer(self) -> dict[str, str]:
        return {
            "code_sha": self.producer.code_sha,
            "config_sha256": self.producer.config_sha256,
            "schema_sha256": self.producer.schema_sha256,
            "normalizer_version": self.scope["normalizer_version"],
            "workflow_run_id": self.producer.workflow_run_id,
        }

    @staticmethod
    def _query(harvest: HarvestResult, source_date: str) -> dict[str, Any]:
        if not harvest.token_exhausted or harvest.page_count < 1:
            raise FeedValidationError("harvest is not query-complete")
        return {
            "from": source_date,
            "until": source_date,
            "metadata_prefix": "arXivRaw",
            "set": None,
            "first_response_date": harvest.first_response_date,
            "last_response_date": harvest.last_response_date,
            "page_count": harvest.page_count,
            "token_exhausted": True,
            "source_schema_url": SOURCE_SCHEMA_URL,
            "source_schema_sha256": SOURCE_SCHEMA_SHA256,
            "observed_complete_at": harvest.observed_complete_at,
        }
