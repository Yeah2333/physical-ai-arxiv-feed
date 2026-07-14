"""Serial, fail-closed OAI-PMH client and arXivRaw parser."""

from __future__ import annotations

import email.utils
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterator

from .canonical import normalize_categories, normalize_multiline, normalize_single_line
from .identity import base_id_from_oai_identifier, parse_arxiv_id


OAI_NAMESPACE = "http://www.openarchives.org/OAI/2.0/"
RAW_NAMESPACE = "http://arxiv.org/OAI/arXivRaw/"
OAI = f"{{{OAI_NAMESPACE}}}"
RAW = f"{{{RAW_NAMESPACE}}}"
DEFAULT_ENDPOINT = "https://oaipmh.arxiv.org/oai"
SOURCE_SCHEMA_URL = "https://oaipmh.arxiv.org/OAI/arXivRaw.xsd"
SOURCE_SCHEMA_SHA256 = "sha256:b1f24a0b763c16bd68ac1871fb08cc8f3eb70e859469bdabbce92fa3e2616d85"
MAX_XML_RESPONSE_BYTES = 25 * 1024 * 1024


class OAIError(RuntimeError):
    """An upstream, pagination, or source-schema failure."""


@dataclass(frozen=True)
class RawRecord:
    oai_identifier: str
    source_datestamp: str
    source_sets: list[str]
    deleted: bool
    base_arxiv_id: str
    versioned_arxiv_id: str | None
    current_version: int | None
    version_history: list[dict[str, object]] | None
    metadata: dict[str, object] | None
    field_provenance: dict[str, str] | None


@dataclass(frozen=True)
class OAIPage:
    response_date: str
    records: list[RawRecord]
    resumption_token: str | None


@dataclass(frozen=True)
class HarvestResult:
    records: list[RawRecord]
    first_response_date: str
    last_response_date: str
    page_count: int
    token_exhausted: bool
    observed_complete_at: str


def _text(parent: ET.Element, tag: str, *, required: bool = False) -> str | None:
    child = parent.find(tag)
    value = child.text if child is not None else None
    if value is None or value.strip() == "":
        if required:
            raise OAIError(f"missing required source field {tag}")
        return None
    return value


def _source_timestamp(value: str) -> str:
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError) as exc:
        raise OAIError(f"unsupported arXivRaw version date: {value!r}") from exc
    if parsed is None:
        raise OAIError(f"unsupported arXivRaw version date: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc).replace(microsecond=0)
    return parsed.isoformat().replace("+00:00", "Z")


def _rfc3339_utc(value: str, *, field: str) -> str:
    if not value.endswith("Z"):
        raise OAIError(f"{field} must be UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise OAIError(f"invalid {field}: {value!r}") from exc
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_raw(header: ET.Element, metadata: ET.Element | None) -> RawRecord:
    identifier = _text(header, f"{OAI}identifier", required=True)
    assert identifier is not None
    base_id = base_id_from_oai_identifier(identifier)
    source_datestamp = _text(header, f"{OAI}datestamp", required=True)
    assert source_datestamp is not None
    try:
        datetime.strptime(source_datestamp, "%Y-%m-%d")
    except ValueError as exc:
        raise OAIError(f"unsupported day-granularity datestamp: {source_datestamp!r}") from exc
    source_sets = normalize_categories(
        element.text or "" for element in header.findall(f"{OAI}setSpec")
    )
    deleted = header.get("status") == "deleted"
    if deleted:
        if metadata is not None:
            raise OAIError("deleted OAI record unexpectedly contains metadata")
        return RawRecord(
            oai_identifier=identifier,
            source_datestamp=source_datestamp,
            source_sets=source_sets,
            deleted=True,
            base_arxiv_id=base_id,
            versioned_arxiv_id=None,
            current_version=None,
            version_history=None,
            metadata=None,
            field_provenance=None,
        )

    if metadata is None or len(metadata) != 1 or metadata[0].tag != f"{RAW}arXivRaw":
        raise OAIError("non-deleted record must contain exactly one arXivRaw element")
    raw = metadata[0]
    raw_id = normalize_single_line(_text(raw, f"{RAW}id", required=True) or "")
    parsed_raw_id = parse_arxiv_id(raw_id)
    if parsed_raw_id.base_id != base_id or parsed_raw_id.version is not None:
        raise OAIError("arXivRaw id does not match OAI identifier")

    history: list[dict[str, object]] = []
    for version_element in raw.findall(f"{RAW}version"):
        version_text = version_element.get("version")
        if version_text is None or not version_text.startswith("v"):
            raise OAIError("arXivRaw version attribute is missing")
        try:
            version = int(version_text[1:])
        except ValueError as exc:
            raise OAIError(f"invalid arXivRaw version: {version_text!r}") from exc
        if version < 1:
            raise OAIError("arXivRaw version must be positive")
        submitted = _text(version_element, f"{RAW}date", required=True)
        assert submitted is not None
        history.append({"version": version, "submitted_at": _source_timestamp(submitted)})
    versions = [item["version"] for item in history]
    if not versions or versions != sorted(set(versions)):
        raise OAIError("arXivRaw versions must be non-empty and strictly increasing")
    current_version = int(versions[-1])

    categories_text = _text(raw, f"{RAW}categories", required=True) or ""
    categories = normalize_categories(categories_text.split())
    metadata_value: dict[str, object] = {
        "title": _nullable_source(raw, f"{RAW}title"),
        "abstract": _nullable_source(raw, f"{RAW}abstract", multiline=True),
        "authors_raw": _nullable_source(raw, f"{RAW}authors"),
        "authors": None,
        "primary_category": None,
        "categories": categories,
        "comment": _nullable_source(raw, f"{RAW}comments", multiline=True),
        "journal_ref": _nullable_source(raw, f"{RAW}journal-ref"),
        "doi": _nullable_source(raw, f"{RAW}doi"),
        "license": _nullable_source(raw, f"{RAW}license"),
    }
    return RawRecord(
        oai_identifier=identifier,
        source_datestamp=source_datestamp,
        source_sets=source_sets,
        deleted=False,
        base_arxiv_id=base_id,
        versioned_arxiv_id=f"{base_id}v{current_version}",
        current_version=current_version,
        version_history=history,
        metadata=metadata_value,
        field_provenance={
            "version_history": "oai_arxiv_raw",
            "authors_raw": "oai_arxiv_raw",
            "authors": "oai_arxiv_raw",
            "primary_category": "oai_arxiv_raw",
        },
    )


def _nullable_source(parent: ET.Element, tag: str, *, multiline: bool = False) -> str | None:
    value = _text(parent, tag)
    if value is None:
        return None
    normalized = normalize_multiline(value) if multiline else normalize_single_line(value)
    return normalized or None


def parse_oai_page(payload: bytes) -> OAIPage:
    if len(payload) > MAX_XML_RESPONSE_BYTES:
        raise OAIError("OAI XML exceeds the response size limit")
    upper_prefix = payload[:4096].upper()
    if b"<!DOCTYPE" in upper_prefix or b"<!ENTITY" in upper_prefix:
        raise OAIError("OAI XML declarations are not allowed")
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise OAIError("invalid OAI XML") from exc
    if root.tag != f"{OAI}OAI-PMH":
        raise OAIError("unexpected OAI root namespace")
    response_date_text = _text(root, f"{OAI}responseDate", required=True)
    assert response_date_text is not None
    response_date = _rfc3339_utc(response_date_text, field="responseDate")
    errors = root.findall(f"{OAI}error")
    if errors:
        if len(errors) == 1 and errors[0].get("code") == "noRecordsMatch":
            return OAIPage(
                response_date=response_date,
                records=[],
                resumption_token=None,
            )
        rendered = ", ".join(f"{item.get('code')}: {(item.text or '').strip()}" for item in errors)
        raise OAIError(f"OAI error response: {rendered}")
    list_records = root.find(f"{OAI}ListRecords")
    if list_records is None:
        raise OAIError("OAI response has no ListRecords element")
    records: list[RawRecord] = []
    for record in list_records.findall(f"{OAI}record"):
        header = record.find(f"{OAI}header")
        if header is None:
            raise OAIError("OAI record is missing header")
        records.append(_parse_raw(header, record.find(f"{OAI}metadata")))
    token_element = list_records.find(f"{OAI}resumptionToken")
    token = None
    if token_element is not None and (token_element.text or "").strip():
        token = (token_element.text or "").strip()
    return OAIPage(response_date=response_date, records=records, resumption_token=token)


class OAIClient:
    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        user_agent: str,
        timeout_seconds: int = 45,
        max_attempts: int = 4,
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
            payload = response.read(MAX_XML_RESPONSE_BYTES + 1)
            if len(payload) > MAX_XML_RESPONSE_BYTES:
                raise OAIError("OAI XML exceeds the response size limit")
            return payload

    def _request(self, parameters: dict[str, str]) -> bytes:
        query = urllib.parse.urlencode(parameters)
        request = urllib.request.Request(
            f"{self.endpoint}?{query}",
            headers={"User-Agent": self.user_agent, "Accept": "application/xml"},
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
                retry_after = exc.headers.get("Retry-After")
                if exc.code not in {429, 500, 502, 503, 504} or attempt == self.max_attempts:
                    raise OAIError(f"OAI HTTP {exc.code}") from exc
                delay = float(retry_after) if retry_after and retry_after.isdigit() else 2 ** (attempt - 1)
            except (urllib.error.URLError, TimeoutError) as exc:
                self._last_request_at = time.monotonic()
                if attempt == self.max_attempts:
                    raise OAIError("OAI request failed after retries") from exc
                delay = 2 ** (attempt - 1)
            time.sleep(delay + random.uniform(0, min(1.0, delay / 4)))
        raise AssertionError("unreachable")

    def pages(self, *, source_date: str) -> Iterator[OAIPage]:
        parameters = {
            "verb": "ListRecords",
            "metadataPrefix": "arXivRaw",
            "from": source_date,
            "until": source_date,
        }
        seen_tokens: set[str] = set()
        while True:
            page = parse_oai_page(self._request(parameters))
            yield page
            token = page.resumption_token
            if token is None:
                return
            if token in seen_tokens:
                raise OAIError("OAI resumptionToken cycle detected")
            seen_tokens.add(token)
            parameters = {"verb": "ListRecords", "resumptionToken": token}

    def harvest(self, *, source_date: str, observed_complete_at: str | None = None) -> HarvestResult:
        pages = list(self.pages(source_date=source_date))
        if not pages:
            raise OAIError("OAI pagination produced no pages")
        records = [record for page in pages for record in page.records]
        identities = [record.oai_identifier for record in records]
        if len(identities) != len(set(identities)):
            raise OAIError("duplicate OAI identifier across pages")
        return HarvestResult(
            records=records,
            first_response_date=pages[0].response_date,
            last_response_date=pages[-1].response_date,
            page_count=len(pages),
            token_exhausted=pages[-1].resumption_token is None,
            observed_complete_at=_rfc3339_utc(
                observed_complete_at
                or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                field="observed_complete_at",
            ),
        )
