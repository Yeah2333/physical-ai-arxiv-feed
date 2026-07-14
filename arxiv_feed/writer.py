"""Content-addressed deterministic Feed object writers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from .canonical import (
    canonical_file_bytes,
    canonical_jsonl_line,
    deterministic_gzip,
    sha256_bytes,
)
from .contract import MAX_RECORD_LINE_BYTES


MAX_PART_UNCOMPRESSED_BYTES = 50 * 1024 * 1024


class FeedWriteError(RuntimeError):
    """Raised when an immutable object conflicts or exceeds a contract limit."""


def _digest_hex(digest: str) -> str:
    return digest.split(":", 1)[1]


def object_path(compressed_sha256: str, *, prefix: str = "objects") -> str:
    digest = _digest_hex(compressed_sha256)
    return f"{prefix}/sha256/{digest[:2]}/{digest}.jsonl.gz"


def write_immutable(root: Path, relative_path: str, payload: bytes) -> None:
    target = root / relative_path
    if target.exists():
        if target.read_bytes() != payload:
            raise FeedWriteError(f"immutable object conflict: {relative_path}")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)


def write_canonical_file(root: Path, relative_path: str, value: Any) -> str:
    payload = canonical_file_bytes(value)
    target = root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return sha256_bytes(payload)


def _sorted_observations(observations: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    values = list(observations)
    identities = [item["observation_id"] for item in values]
    if len(identities) != len(set(identities)):
        raise FeedWriteError("duplicate observation_id")
    return sorted(
        values,
        key=lambda item: (item["record"]["logical_record_key"], item["observation_id"]),
    )


def build_data_parts(
    *,
    root: Path,
    observations: Iterable[dict[str, Any]],
    max_uncompressed_bytes: int = MAX_PART_UNCOMPRESSED_BYTES,
    object_prefix: str = "objects",
) -> list[dict[str, Any]]:
    if max_uncompressed_bytes < 1:
        raise ValueError("max_uncompressed_bytes must be positive")
    grouped: list[list[bytes]] = []
    current: list[bytes] = []
    current_size = 0
    for observation in _sorted_observations(observations):
        line = canonical_jsonl_line(observation)
        if len(line) > MAX_RECORD_LINE_BYTES:
            raise FeedWriteError("observation exceeds the 2 MiB canonical-line limit")
        if current and current_size + len(line) > max_uncompressed_bytes:
            grouped.append(current)
            current = []
            current_size = 0
        current.append(line)
        current_size += len(line)
    if current:
        grouped.append(current)

    descriptors: list[dict[str, Any]] = []
    for ordinal, lines in enumerate(grouped):
        uncompressed = b"".join(lines)
        compressed = deterministic_gzip(uncompressed)
        compressed_hash = sha256_bytes(compressed)
        path = object_path(compressed_hash, prefix=object_prefix)
        write_immutable(root, path, compressed)
        descriptors.append(
            {
                "ordinal": ordinal,
                "path": path,
                "compressed_size": len(compressed),
                "compressed_sha256": compressed_hash,
                "uncompressed_size": len(uncompressed),
                "uncompressed_sha256": sha256_bytes(uncompressed),
                "record_count": len(lines),
            }
        )
    return descriptors


def build_jsonl_shards(
    *,
    root: Path,
    records: Iterable[dict[str, Any]],
    key_field: str,
    object_prefix: str,
) -> list[dict[str, Any]]:
    """Write one deterministic content-addressed object per hash prefix."""

    by_prefix: dict[str, list[dict[str, Any]]] = {}
    seen: set[str] = set()
    for record in records:
        key = record.get(key_field)
        if not isinstance(key, str) or not key.startswith("sha256:") or len(key) != 71:
            raise FeedWriteError(f"invalid shard key {key_field}")
        if key in seen:
            raise FeedWriteError(f"duplicate shard key: {key}")
        seen.add(key)
        by_prefix.setdefault(key[7:9], []).append(record)

    descriptors: list[dict[str, Any]] = []
    for prefix in sorted(by_prefix):
        values = sorted(by_prefix[prefix], key=lambda item: item[key_field])
        uncompressed = b"".join(canonical_jsonl_line(item) for item in values)
        compressed = deterministic_gzip(uncompressed)
        compressed_hash = sha256_bytes(compressed)
        path = object_path(compressed_hash, prefix=object_prefix)
        write_immutable(root, path, compressed)
        descriptors.append(
            {
                "prefix": prefix,
                "path": path,
                "compressed_size": len(compressed),
                "compressed_sha256": compressed_hash,
                "uncompressed_size": len(uncompressed),
                "uncompressed_sha256": sha256_bytes(uncompressed),
                "record_count": len(values),
            }
        )
    return descriptors
