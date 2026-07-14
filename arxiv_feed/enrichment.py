"""Optional arXiv Search metadata enrichment with a persistent, bounded backlog."""

from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping

from .canonical import (
    canonical_file_bytes,
    normalize_multiline,
    normalize_single_line,
    sha256_bytes,
)
from .contract import build_observation, validate_observation
from .identity import parse_arxiv_id


ATOM = "{http://www.w3.org/2005/Atom}"
ARXIV = "{http://arxiv.org/schemas/atom}"
DEFAULT_SEARCH_ENDPOINT = "https://export.arxiv.org/api/query"
DEFER_AFTER_ATTEMPTS = 7
MAX_SEARCH_RESPONSE_BYTES = 25 * 1024 * 1024


class EnrichmentError(RuntimeError):
    """A Search transport, parse, state, or target-binding failure."""


@dataclass(frozen=True)
class SearchRecord:
    base_arxiv_id: str
    version: int
    title: str
    abstract: str
    authors: list[dict[str, str]]
    primary_category: str


@dataclass(frozen=True)
class SearchOutcome:
    record: SearchRecord | None
    error_kind: str | None = None
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.record is not None and self.error_kind is None


def target_material_sha256(record: Mapping[str, Any]) -> str:
    metadata = record.get("metadata") or {}
    payload = {
        "base_arxiv_id": record.get("base_arxiv_id"),
        "current_version": record.get("current_version"),
        "title": metadata.get("title"),
        "abstract": metadata.get("abstract"),
    }
    return sha256_bytes(canonical_file_bytes(payload))


def task_key(base_arxiv_id: str) -> str:
    return sha256_bytes(
        canonical_file_bytes(
            {"domain": "arxiv-search-enrichment-task-v1", "base_arxiv_id": base_arxiv_id}
        )
    )


def build_task(
    target_observation: Mapping[str, Any],
    *,
    scope_id: str,
    first_enqueued_at: str,
) -> dict[str, Any]:
    observation = validate_observation(dict(target_observation), scope_id=scope_id)
    record = observation["record"]
    if record["operation"] != "upsert":
        raise EnrichmentError("only an upsert observation can be enriched")
    _timestamp(first_enqueued_at)
    return {
        "task_key": task_key(record["base_arxiv_id"]),
        "base_arxiv_id": record["base_arxiv_id"],
        "target_observation_id": observation["observation_id"],
        "target_source_date": record["source_datestamp"],
        "target_current_version": record["current_version"],
        "target_material_sha256": target_material_sha256(record),
        "first_enqueued_at": first_enqueued_at,
        "last_attempt_at": None,
        "attempt_count": 0,
        "status": "pending",
        "next_attempt_date": record["source_datestamp"],
        "last_error_kind": None,
        "last_error": None,
    }


def validate_task(task: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "task_key", "base_arxiv_id", "target_observation_id", "target_source_date",
        "target_current_version", "target_material_sha256", "first_enqueued_at",
        "last_attempt_at", "attempt_count", "status", "next_attempt_date",
        "last_error_kind", "last_error",
    }
    if set(task) != required:
        raise EnrichmentError("enrichment task keys do not match v1 state")
    base_id = str(task["base_arxiv_id"])
    if task["task_key"] != task_key(base_id):
        raise EnrichmentError("enrichment task key mismatch")
    parse_arxiv_id(base_id)
    for field in ("target_observation_id", "target_material_sha256"):
        value = task[field]
        if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
            raise EnrichmentError(f"invalid {field}")
    date.fromisoformat(str(task["target_source_date"]))
    date.fromisoformat(str(task["next_attempt_date"]))
    _timestamp(str(task["first_enqueued_at"]))
    if task["last_attempt_at"] is not None:
        _timestamp(str(task["last_attempt_at"]))
    version = task["target_current_version"]
    attempts = task["attempt_count"]
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise EnrichmentError("invalid target_current_version")
    if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 0:
        raise EnrichmentError("invalid enrichment attempt_count")
    if task["status"] not in {"pending", "deferred"}:
        raise EnrichmentError("invalid enrichment task status")
    for field in ("last_error_kind", "last_error"):
        if task[field] is not None and not isinstance(task[field], str):
            raise EnrichmentError(f"invalid {field}")
    return dict(task)


def task_due(task: Mapping[str, Any], *, today_utc: date) -> bool:
    validated = validate_task(task)
    return date.fromisoformat(validated["next_attempt_date"]) <= today_utc


def transition_failure(
    task: Mapping[str, Any],
    *,
    attempted_at: str,
    today_utc: date,
    error_kind: str,
    error: str,
) -> dict[str, Any]:
    current = validate_task(task)
    _timestamp(attempted_at)
    attempts = int(current["attempt_count"]) + 1
    deferred = attempts >= DEFER_AFTER_ATTEMPTS
    current.update(
        {
            "last_attempt_at": attempted_at,
            "attempt_count": attempts,
            "status": "deferred" if deferred else "pending",
            "next_attempt_date": (
                today_utc + timedelta(days=7 if deferred else 1)
            ).isoformat(),
            "last_error_kind": normalize_single_line(error_kind)[:100],
            "last_error": normalize_multiline(error)[:1000],
        }
    )
    return validate_task(current)


def apply_search_result(
    *,
    scope_id: str,
    target_observation: Mapping[str, Any],
    result: SearchRecord,
) -> dict[str, Any]:
    target = validate_observation(dict(target_observation), scope_id=scope_id)
    record = target["record"]
    if record["operation"] != "upsert":
        raise EnrichmentError("enrichment target is not an upsert")
    if result.base_arxiv_id != record["base_arxiv_id"]:
        raise EnrichmentError("target_base_mismatch")
    target_version = int(record["current_version"])
    if result.version > target_version:
        raise EnrichmentError("target_ahead")
    if result.version < target_version:
        raise EnrichmentError("target_behind")
    metadata = record["metadata"]
    if result.title != metadata.get("title") or result.abstract != metadata.get("abstract"):
        raise EnrichmentError("target_material_mismatch")
    if result.primary_category not in metadata["categories"]:
        raise EnrichmentError("primary_category_not_in_oai_categories")
    enriched_record = json.loads(json.dumps(record, ensure_ascii=False))
    enriched_record["metadata"]["authors"] = result.authors
    enriched_record["metadata"]["primary_category"] = result.primary_category
    enriched_record["field_provenance"]["authors"] = "search_api_optional"
    enriched_record["field_provenance"]["primary_category"] = "search_api_optional"
    return build_observation(
        scope_id=scope_id,
        record=enriched_record,
        supersedes_observation_id=target["observation_id"],
    )


def parse_search_feed(payload: bytes) -> dict[str, SearchRecord]:
    if len(payload) > MAX_SEARCH_RESPONSE_BYTES:
        raise EnrichmentError("Search Atom XML exceeds the response size limit")
    upper_prefix = payload[:4096].upper()
    if b"<!DOCTYPE" in upper_prefix or b"<!ENTITY" in upper_prefix:
        raise EnrichmentError("Search Atom XML declarations are not allowed")
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise EnrichmentError("invalid Search Atom XML") from exc
    if root.tag != f"{ATOM}feed":
        raise EnrichmentError("unexpected Search Atom root")
    records: dict[str, SearchRecord] = {}
    for entry in root.findall(f"{ATOM}entry"):
        id_text = _entry_text(entry, f"{ATOM}id")
        parsed = parse_arxiv_id(urllib.parse.urlparse(id_text).path.rsplit("/", 1)[-1])
        if parsed.version is None:
            raise EnrichmentError("Search entry id is not versioned")
        authors = [
            {"name": normalize_single_line(_entry_text(author, f"{ATOM}name"))}
            for author in entry.findall(f"{ATOM}author")
        ]
        primary = entry.find(f"{ARXIV}primary_category")
        primary_term = normalize_single_line(primary.get("term") or "") if primary is not None else ""
        if not authors or not primary_term:
            raise EnrichmentError("Search entry lacks authors or primary category")
        record = SearchRecord(
            base_arxiv_id=parsed.base_id,
            version=parsed.version,
            title=normalize_single_line(_entry_text(entry, f"{ATOM}title")),
            abstract=normalize_multiline(_entry_text(entry, f"{ATOM}summary")),
            authors=authors,
            primary_category=primary_term,
        )
        if record.base_arxiv_id in records:
            raise EnrichmentError("duplicate Search entry")
        records[record.base_arxiv_id] = record
    return records


class SearchClient:
    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_SEARCH_ENDPOINT,
        user_agent: str,
        timeout_seconds: int = 45,
        max_attempts: int = 3,
        min_interval_seconds: float = 3.0,
        opener: Callable[[urllib.request.Request, int], bytes] | None = None,
    ) -> None:
        if "@" not in user_agent and "+http" not in user_agent:
            raise ValueError("user_agent must contain project/contact information")
        self.endpoint = endpoint
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.min_interval_seconds = min_interval_seconds
        self.opener = opener or self._open
        self._last_request_at: float | None = None

    @staticmethod
    def _open(request: urllib.request.Request, timeout: int) -> bytes:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read(MAX_SEARCH_RESPONSE_BYTES + 1)
            if len(payload) > MAX_SEARCH_RESPONSE_BYTES:
                raise EnrichmentError("Search Atom XML exceeds the response size limit")
            return payload

    def fetch_many(self, base_ids: Iterable[str]) -> dict[str, SearchOutcome]:
        requested = sorted(set(base_ids))
        if not requested:
            return {}
        for item in requested:
            if parse_arxiv_id(item).version is not None:
                raise EnrichmentError("Search requests must use base arXiv ids")
        parameters = {
            "id_list": ",".join(requested),
            "start": "0",
            "max_results": str(len(requested)),
        }
        try:
            records = parse_search_feed(self._request(parameters))
        except Exception as exc:
            return {
                item: SearchOutcome(None, "search_transport_or_parse", str(exc)[:1000])
                for item in requested
            }
        outcomes: dict[str, SearchOutcome] = {}
        for item in requested:
            result = records.get(item)
            outcomes[item] = (
                SearchOutcome(result)
                if result is not None
                else SearchOutcome(None, "search_missing", "Search API returned no exact id_list match")
            )
        return outcomes

    def _request(self, parameters: dict[str, str]) -> bytes:
        request = urllib.request.Request(
            f"{self.endpoint}?{urllib.parse.urlencode(parameters)}",
            headers={"User-Agent": self.user_agent, "Accept": "application/atom+xml"},
        )
        for attempt in range(1, self.max_attempts + 1):
            if self._last_request_at is not None:
                remaining = self.min_interval_seconds - (time.monotonic() - self._last_request_at)
                if remaining > 0:
                    time.sleep(remaining)
            try:
                payload = self.opener(request, self.timeout_seconds)
                self._last_request_at = time.monotonic()
                return payload
            except urllib.error.HTTPError as exc:
                self._last_request_at = time.monotonic()
                if exc.code not in {429, 500, 502, 503, 504} or attempt == self.max_attempts:
                    raise EnrichmentError(f"Search HTTP {exc.code}") from exc
                retry_after = exc.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else 2 ** (attempt - 1)
            except (urllib.error.URLError, TimeoutError) as exc:
                self._last_request_at = time.monotonic()
                if attempt == self.max_attempts:
                    raise EnrichmentError("Search request failed after retries") from exc
                delay = 2 ** (attempt - 1)
            time.sleep(delay + random.uniform(0, min(1.0, delay / 4)))
        raise AssertionError("unreachable")


def backlog_oldest_age_hours(
    tasks: Iterable[Mapping[str, Any]],
    *,
    now: datetime | None = None,
) -> float | None:
    values = [validate_task(task) for task in tasks]
    if not values:
        return None
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    oldest = min(
        datetime.fromisoformat(str(item["first_enqueued_at"]).replace("Z", "+00:00"))
        for item in values
    )
    return max(0.0, (current - oldest).total_seconds() / 3600)


def _entry_text(parent: ET.Element, tag: str) -> str:
    child = parent.find(tag)
    if child is None or child.text is None or not child.text.strip():
        raise EnrichmentError(f"Search entry missing {tag}")
    return child.text


def _timestamp(value: str) -> None:
    if not value.endswith("Z"):
        raise EnrichmentError("timestamp must end in Z")
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise EnrichmentError("invalid timestamp") from exc
