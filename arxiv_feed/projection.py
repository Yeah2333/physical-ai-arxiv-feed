"""Deterministic source-scope projection and membership fold."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .canonical import canonicalize_categories
from .contract import build_observation, validate_observation
from .identity import logical_record_key, record_content_sha256
from .oai import RawRecord, SourceTextRepair, normalize_record_text


class ProjectionError(ValueError):
    """Raised for a fork, invalid transition, or inconsistent membership state."""


@dataclass(frozen=True)
class Membership:
    logical_record_key: str
    ever_in_scope: bool
    active_in_scope: bool
    source_deleted: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "logical_record_key": self.logical_record_key,
            "ever_in_scope": self.ever_in_scope,
            "active_in_scope": self.active_in_scope,
            "source_deleted": self.source_deleted,
        }


@dataclass(frozen=True)
class Head:
    observation_id: str
    record_content_sha256: str
    operation: str


@dataclass(frozen=True)
class ProjectionResult:
    observations: list[dict[str, Any]]
    memberships: dict[str, Membership]
    heads: dict[str, Head]
    ignored_count: int
    unchanged_count: int
    source_text_repairs: list[SourceTextRepair]


def _in_scope(categories: Iterable[str], scope: Mapping[str, Any]) -> bool:
    canonical = set(canonicalize_categories(categories, scope["category_aliases"]))
    return bool(canonical.intersection(scope["categories"]))


def record_for_operation(raw: RawRecord, *, operation: str) -> dict[str, Any]:
    key = logical_record_key(raw.oai_identifier)
    if operation == "source_delete":
        versioned = None
        current_version = None
        history = None
        metadata = None
        provenance = None
    elif operation == "scope_exit":
        versioned = raw.versioned_arxiv_id
        current_version = raw.current_version
        history = []
        metadata = raw.metadata
        provenance = raw.field_provenance
    else:
        versioned = raw.versioned_arxiv_id
        current_version = raw.current_version
        history = raw.version_history
        metadata = raw.metadata
        provenance = raw.field_provenance
    return {
        "operation": operation,
        "logical_record_key": key,
        "oai_identifier": raw.oai_identifier,
        "source_datestamp": raw.source_datestamp,
        "source_sets": raw.source_sets,
        "base_arxiv_id": raw.base_arxiv_id,
        "versioned_arxiv_id": versioned,
        "current_version": current_version,
        "version_history": history,
        "metadata": metadata,
        "field_provenance": provenance,
    }


def project_records(
    *,
    scope: Mapping[str, Any],
    records: Iterable[RawRecord],
    memberships: Mapping[str, Membership],
    heads: Mapping[str, Head],
) -> ProjectionResult:
    next_memberships = dict(memberships)
    next_heads = dict(heads)
    observations: list[dict[str, Any]] = []
    ignored = 0
    unchanged = 0
    source_text_repairs: list[SourceTextRepair] = []
    for raw in sorted(records, key=lambda item: item.oai_identifier):
        key = logical_record_key(raw.oai_identifier)
        membership = next_memberships.get(
            key,
            Membership(key, ever_in_scope=False, active_in_scope=False, source_deleted=False),
        )
        if membership.source_deleted and not raw.deleted:
            raise ProjectionError("source_delete is terminal in contract v1")
        current_in_scope = not raw.deleted and raw.metadata is not None and _in_scope(
            raw.metadata["categories"], scope
        )
        if raw.deleted:
            if not membership.ever_in_scope:
                ignored += 1
                continue
            operation = "source_delete"
        elif current_in_scope:
            operation = "upsert"
        elif membership.ever_in_scope:
            operation = "scope_exit"
        else:
            ignored += 1
            continue

        if operation != "source_delete":
            raw, repairs = normalize_record_text(raw)
            source_text_repairs.extend(repairs)
        record = record_for_operation(raw, operation=operation)
        content_hash = record_content_sha256(record)
        current_head = next_heads.get(key)
        if current_head is not None and current_head.record_content_sha256 == content_hash:
            unchanged += 1
        else:
            observation = build_observation(
                scope_id=scope["scope_id"],
                record=record,
                supersedes_observation_id=(
                    current_head.observation_id if current_head is not None else None
                ),
            )
            observations.append(observation)
            next_heads[key] = Head(
                observation_id=observation["observation_id"],
                record_content_sha256=observation["record_content_sha256"],
                operation=operation,
            )

        next_memberships[key] = Membership(
            logical_record_key=key,
            ever_in_scope=membership.ever_in_scope or current_in_scope,
            active_in_scope=operation == "upsert",
            source_deleted=operation == "source_delete",
        )
    return ProjectionResult(
        observations=observations,
        memberships=next_memberships,
        heads=next_heads,
        ignored_count=ignored,
        unchanged_count=unchanged,
        source_text_repairs=source_text_repairs,
    )


def fold_observations(
    *,
    scope_id: str,
    observations: Iterable[dict[str, Any]],
) -> tuple[dict[str, Membership], dict[str, Head]]:
    """Rebuild authoritative state; producer corrections require an explicit replay."""

    validated = [validate_observation(item, scope_id=scope_id) for item in observations]
    retracted = {
        item["record"]["target_observation_id"]
        for item in validated
        if item["record"]["operation"] == "producer_retract"
    }
    source_by_id = {
        item["observation_id"]: item
        for item in validated
        if item["record"]["operation"] != "producer_retract"
    }
    if not retracted.issubset(source_by_id):
        raise ProjectionError("producer_retract target is absent")
    for correction in validated:
        record = correction["record"]
        if record["operation"] != "producer_retract":
            continue
        target = source_by_id[record["target_observation_id"]]
        if target["record"]["logical_record_key"] != record["logical_record_key"]:
            raise ProjectionError("producer_retract logical record does not match target")
        replacement = record["replacement_observation_id"]
        if replacement is not None:
            if replacement not in source_by_id:
                raise ProjectionError("producer_retract replacement is absent")
            if source_by_id[replacement]["record"]["logical_record_key"] != record["logical_record_key"]:
                raise ProjectionError("producer_retract replacement has wrong logical record")
    memberships: dict[str, Membership] = {}
    heads: dict[str, Head] = {}
    seen: set[str] = set()
    pending = [
        item
        for item in validated
        if item["record"]["operation"] != "producer_retract"
        and item["observation_id"] not in retracted
    ]
    while pending:
        progressed = False
        for item in list(pending):
            record = item["record"]
            key = record["logical_record_key"]
            predecessor = item["supersedes_observation_id"]
            current = heads.get(key)
            if predecessor is None:
                if current is not None:
                    continue
            elif predecessor not in seen:
                continue
            elif current is None or current.observation_id != predecessor:
                raise ProjectionError("supersedes graph forks or skips the current head")
            operation = record["operation"]
            previous = memberships.get(
                key, Membership(key, False, False, False)
            )
            if previous.source_deleted:
                raise ProjectionError("observation follows terminal source_delete")
            memberships[key] = Membership(
                logical_record_key=key,
                ever_in_scope=previous.ever_in_scope or operation == "upsert",
                active_in_scope=operation == "upsert",
                source_deleted=operation == "source_delete",
            )
            heads[key] = Head(
                observation_id=item["observation_id"],
                record_content_sha256=item["record_content_sha256"],
                operation=operation,
            )
            seen.add(item["observation_id"])
            pending.remove(item)
            progressed = True
        if not progressed:
            raise ProjectionError("supersedes graph is incomplete or cyclic")
    return memberships, heads
