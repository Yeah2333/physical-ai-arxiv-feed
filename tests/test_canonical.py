from __future__ import annotations

import json
import unittest
from pathlib import Path

from arxiv_feed.canonical import (
    CanonicalizationError,
    canonical_file_bytes,
    canonical_json_bytes,
    canonicalize_categories,
    deterministic_gzip,
    normalize_multiline,
    normalize_single_line,
    sha256_bytes,
)


FIXTURES = Path(__file__).parents[1] / "fixtures" / "contract" / "v1"


class CanonicalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.vectors = json.loads((FIXTURES / "canonical-vectors.json").read_text())

    def test_text_normalization(self) -> None:
        self.assertEqual(normalize_single_line("  Cafe\u0301\t robot\r\npolicy  "), "Café robot policy")
        self.assertEqual(normalize_multiline("  first\tline\r\nsecond  \n"), "first line\nsecond")

    def test_control_characters_are_rejected(self) -> None:
        with self.assertRaises(CanonicalizationError):
            normalize_single_line("bad\x00value")

    def test_categories_are_raw_and_alias_projection_is_derived(self) -> None:
        raw = ["eess.SY", "cs.SY", "eess.SY"]
        self.assertEqual(sorted(set(raw)), ["cs.SY", "eess.SY"])
        self.assertEqual(canonicalize_categories(raw, {"cs.SY": "eess.SY"}), ["eess.SY"])

    def test_jcs_subset_and_exact_file_hash(self) -> None:
        vector = self.vectors["canonical_object"]
        value = vector["input"]
        expected = vector["canonical_json_utf8"]
        self.assertEqual(canonical_json_bytes(value), expected.encode("utf-8"))
        self.assertEqual(canonical_file_bytes(value), expected.encode("utf-8") + b"\n")
        self.assertEqual(sha256_bytes(canonical_file_bytes(value)), vector["exact_file_sha256"])

    def test_unsafe_numbers_and_non_string_keys_are_rejected(self) -> None:
        with self.assertRaises(CanonicalizationError):
            canonical_json_bytes(1.5)
        with self.assertRaises(CanonicalizationError):
            canonical_json_bytes(1 << 53)
        with self.assertRaises(CanonicalizationError):
            canonical_json_bytes({1: "bad"})

    def test_gzip_is_deterministic(self) -> None:
        vector = self.vectors["gzip"]
        payload = vector["uncompressed_utf8"].encode("utf-8")
        compressed = deterministic_gzip(payload)
        self.assertEqual(compressed, deterministic_gzip(payload))
        self.assertEqual(sha256_bytes(compressed), vector["compressed_sha256"])

    def test_scope_fixture_is_valid_json(self) -> None:
        self.assertEqual(json.loads((FIXTURES / "scope.json").read_text())["keywords"], [])


if __name__ == "__main__":
    unittest.main()
