"""Stable arXiv and Feed identities shared by producer and consumer."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .canonical import canonical_json_bytes, sha256_bytes


MODERN_ID_RE = re.compile(
    r"^(?P<base>\d{4}\.\d{4,5})(?:v(?P<version>[1-9]\d*))?$"
)
LEGACY_ID_RE = re.compile(
    r"^(?P<base>[A-Za-z][A-Za-z0-9.-]*/\d{7})(?:v(?P<version>[1-9]\d*))?$"
)
OAI_IDENTIFIER_RE = re.compile(r"^oai:arXiv\.org:(?P<base>.+)$")


class ArxivIdentityError(ValueError):
    """Raised when an arXiv identifier is not a supported official form."""


@dataclass(frozen=True)
class ParsedArxivId:
    base_id: str
    version: int | None

    @property
    def versioned_id(self) -> str:
        if self.version is None:
            return self.base_id
        return f"{self.base_id}v{self.version}"


def parse_arxiv_id(value: str) -> ParsedArxivId:
    if not isinstance(value, str):
        raise ArxivIdentityError("arXiv id must be a string")
    match = MODERN_ID_RE.fullmatch(value) or LEGACY_ID_RE.fullmatch(value)
    if match is None:
        raise ArxivIdentityError(f"unsupported arXiv id: {value!r}")
    version_text = match.group("version")
    return ParsedArxivId(
        base_id=match.group("base"),
        version=int(version_text) if version_text is not None else None,
    )


def base_id_from_oai_identifier(value: str) -> str:
    if not isinstance(value, str):
        raise ArxivIdentityError("OAI identifier must be a string")
    match = OAI_IDENTIFIER_RE.fullmatch(value)
    if match is None:
        raise ArxivIdentityError(f"unsupported OAI identifier: {value!r}")
    parsed = parse_arxiv_id(match.group("base"))
    if parsed.version is not None:
        raise ArxivIdentityError("OAI identifier must not contain an arXiv version")
    return parsed.base_id


def _domain_hash(parts: list[Any]) -> str:
    return sha256_bytes(canonical_json_bytes(parts))


def logical_record_key(oai_identifier: str) -> str:
    base_id_from_oai_identifier(oai_identifier)
    return _domain_hash(["arxiv-logical-record-v1", oai_identifier])


def record_content_sha256(record: dict[str, Any]) -> str:
    return sha256_bytes(canonical_json_bytes(record))


def observation_id(
    *,
    scope_id: str,
    oai_identifier: str,
    source_datestamp: str,
    operation: str,
    record_content_hash: str,
    supersedes_observation_id: str | None,
) -> str:
    return _domain_hash(
        [
            "arxiv-oai-observation-v1",
            scope_id,
            oai_identifier,
            source_datestamp,
            operation,
            record_content_hash,
            supersedes_observation_id,
        ]
    )

def producer_retract_observation_id(
    *,
    scope_id: str,
    producer_correction_key: str,
    target_observation_id: str,
    record_content_hash: str,
) -> str:
    return _domain_hash(
        [
            "arxiv-producer-correction-v1",
            scope_id,
            producer_correction_key,
            target_observation_id,
            record_content_hash,
        ]
    )


def direct_atom_observation_id(
    *,
    logical_key: str,
    source_updated_at: str,
    record_content_hash: str,
    supersedes_observation_id: str | None,
) -> str:
    return _domain_hash(
        [
            "arxiv-direct-atom-observation-v1",
            logical_key,
            source_updated_at,
            "upsert",
            record_content_hash,
            supersedes_observation_id,
        ]
    )
