from __future__ import annotations

import unittest

from arxiv_feed.identity import (
    ArxivIdentityError,
    base_id_from_oai_identifier,
    logical_record_key,
    parse_arxiv_id,
)


class IdentityTests(unittest.TestCase):
    def test_modern_id(self) -> None:
        parsed = parse_arxiv_id("2607.01234v2")
        self.assertEqual(parsed.base_id, "2607.01234")
        self.assertEqual(parsed.version, 2)
        self.assertEqual(parsed.versioned_id, "2607.01234v2")

    def test_legacy_id(self) -> None:
        parsed = parse_arxiv_id("hep-th/9901001v3")
        self.assertEqual(parsed.base_id, "hep-th/9901001")
        self.assertEqual(parsed.version, 3)

    def test_oai_identifier_and_key(self) -> None:
        identifier = "oai:arXiv.org:2607.01234"
        self.assertEqual(base_id_from_oai_identifier(identifier), "2607.01234")
        self.assertEqual(logical_record_key(identifier), "sha256:cbfb3517cc1ea9391e6a109bcd4225ff15a30f4ad0b042d7d0fa5589963bc156")

    def test_invalid_identifiers_fail_closed(self) -> None:
        for value in ("2607.123v1", "arXiv:2607.01234", "2607.01234v0"):
            with self.subTest(value=value), self.assertRaises(ArxivIdentityError):
                parse_arxiv_id(value)
        with self.assertRaises(ArxivIdentityError):
            base_id_from_oai_identifier("oai:arXiv.org:2607.01234v2")


if __name__ == "__main__":
    unittest.main()
