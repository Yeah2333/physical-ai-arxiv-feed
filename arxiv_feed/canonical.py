"""Contract-v1 text, JSON, hash, and gzip canonicalization.

The Feed only permits JSON values whose numeric representation is identical
under RFC 8785 and Python's encoder: booleans, null, strings, arrays, objects,
and safe-range integers. Floating-point values are deliberately forbidden.
Object keys are ordered by UTF-16 code units as required by JCS.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import unicodedata
from collections import OrderedDict
from typing import Any, Iterable, Mapping


NORMALIZER_VERSION = "arxiv-feed-normalizer-v1"
SAFE_INTEGER_MAX = (1 << 53) - 1
HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class CanonicalizationError(ValueError):
    """Raised when a value cannot be represented by the v1 contract."""


def _reject_invalid_unicode(value: str) -> None:
    for char in value:
        category = unicodedata.category(char)
        if category == "Cs":
            raise CanonicalizationError("surrogate code points are forbidden")
        if category == "Cc" and char not in {"\t", "\n"}:
            raise CanonicalizationError(
                f"control character U+{ord(char):04X} is forbidden"
            )


def _normalize_line_endings(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _collapse_horizontal_whitespace(value: str) -> str:
    output: list[str] = []
    in_space = False
    for char in value:
        if char == "\n":
            output.append(char)
            in_space = False
            continue
        if char == "\t" or unicodedata.category(char) == "Zs":
            if not in_space:
                output.append(" ")
                in_space = True
            continue
        output.append(char)
        in_space = False
    return "".join(output)


def normalize_single_line(value: str) -> str:
    """Normalize a source field that is represented on one logical line."""

    if not isinstance(value, str):
        raise CanonicalizationError("single-line value must be a string")
    value = unicodedata.normalize("NFC", _normalize_line_endings(value))
    _reject_invalid_unicode(value)
    value = value.replace("\n", " ")
    return _collapse_horizontal_whitespace(value).strip()


def normalize_multiline(value: str) -> str:
    """Normalize an abstract/comment while preserving meaningful line breaks."""

    if not isinstance(value, str):
        raise CanonicalizationError("multiline value must be a string")
    value = unicodedata.normalize("NFC", _normalize_line_endings(value))
    _reject_invalid_unicode(value)
    value = _collapse_horizontal_whitespace(value)
    lines = [line.rstrip(" ") for line in value.split("\n")]
    return "\n".join(lines).strip()


def normalize_categories(values: Iterable[str]) -> list[str]:
    normalized = {normalize_single_line(value) for value in values}
    if "" in normalized:
        raise CanonicalizationError("category cannot be empty")
    return sorted(normalized)


def canonicalize_categories(
    raw_categories: Iterable[str], aliases: Mapping[str, str]
) -> list[str]:
    """Return a derived category set without changing source categories."""

    normalized_aliases = {
        normalize_single_line(key): normalize_single_line(value)
        for key, value in aliases.items()
    }
    return sorted(
        {normalized_aliases.get(value, value) for value in normalize_categories(raw_categories)}
    )


def _utf16_sort_key(value: str) -> bytes:
    _reject_invalid_unicode(value)
    return value.encode("utf-16-be")


def _prepare_json(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if abs(value) > SAFE_INTEGER_MAX:
            raise CanonicalizationError("integer exceeds the RFC 8785 safe range")
        return value
    if isinstance(value, float):
        raise CanonicalizationError("floating-point values are forbidden in Feed v1")
    if isinstance(value, str):
        _reject_invalid_unicode(value)
        return value
    if isinstance(value, (list, tuple)):
        return [_prepare_json(item) for item in value]
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise CanonicalizationError("JSON object keys must be strings")
        ordered: OrderedDict[str, Any] = OrderedDict()
        for key in sorted(value.keys(), key=_utf16_sort_key):
            ordered[key] = _prepare_json(value[key])
        return ordered
    raise CanonicalizationError(f"unsupported JSON value: {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    prepared = _prepare_json(value)
    rendered = json.dumps(
        prepared,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    return rendered.encode("utf-8")


def canonical_file_bytes(value: Any) -> bytes:
    return canonical_json_bytes(value) + b"\n"


def canonical_jsonl_line(value: Any) -> bytes:
    return canonical_file_bytes(value)


def sha256_bytes(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def exact_file_sha256(value: Any) -> str:
    return sha256_bytes(canonical_file_bytes(value))


def deterministic_gzip(value: bytes, *, compresslevel: int = 9) -> bytes:
    output = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        compresslevel=compresslevel,
        fileobj=output,
        mtime=0,
    ) as stream:
        stream.write(value)
    return output.getvalue()


def require_sha256(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or HASH_RE.fullmatch(value) is None:
        raise CanonicalizationError(f"{field} must be sha256:<64 lowercase hex>")
    return value
