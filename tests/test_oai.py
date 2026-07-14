from __future__ import annotations

import urllib.parse
import unittest
from pathlib import Path

from arxiv_feed.oai import OAIClient, OAIError, parse_oai_page


FIXTURES = Path(__file__).parents[1] / "fixtures" / "oai"


class OAIParserTests(unittest.TestCase):
    def test_xml_declarations_are_rejected(self) -> None:
        with self.assertRaisesRegex(OAIError, "declarations"):
            parse_oai_page(
                b'<!DOCTYPE x [<!ENTITY e "boom">]><x>&e;</x>'
            )

    def test_parse_arxiv_raw_and_resumption_token(self) -> None:
        page = parse_oai_page((FIXTURES / "page-1.xml").read_bytes())
        self.assertEqual(page.response_date, "2026-07-14T05:00:00Z")
        self.assertEqual(page.resumption_token, "opaque-token")
        self.assertEqual(len(page.records), 1)
        record = page.records[0]
        self.assertFalse(record.deleted)
        self.assertEqual(record.base_arxiv_id, "2607.01234")
        self.assertEqual(record.versioned_arxiv_id, "2607.01234v2")
        self.assertEqual(record.source_sets, ["cs:cs:AI", "cs:cs:RO"])
        self.assertEqual(record.metadata["title"], "A Physical AI Policy for Mobile Manipulation")
        self.assertEqual(record.metadata["categories"], ["cs.AI", "cs.RO"])
        self.assertEqual(record.metadata["primary_category"], None)

    def test_parse_deleted_header(self) -> None:
        page = parse_oai_page((FIXTURES / "page-2.xml").read_bytes())
        self.assertIsNone(page.resumption_token)
        self.assertTrue(page.records[0].deleted)
        self.assertIsNone(page.records[0].metadata)

    def test_oai_error_fails_closed(self) -> None:
        with self.assertRaises(OAIError):
            parse_oai_page((FIXTURES / "error.xml").read_bytes())

    def test_no_records_match_is_an_empty_exhausted_page(self) -> None:
        payload = (FIXTURES / "no-records.xml").read_bytes()
        page = parse_oai_page(payload)
        self.assertEqual(page.response_date, "2026-07-14T10:11:35Z")
        self.assertEqual(page.records, [])
        self.assertIsNone(page.resumption_token)

        client = OAIClient(
            user_agent="physical-ai-arxiv-feed/0.1 (+https://example.invalid/contact)",
            min_interval_seconds=0,
            opener=lambda request, timeout: payload,
        )
        result = client.harvest(
            source_date="2026-06-14", observed_complete_at="2026-07-14T10:12:00Z"
        )
        self.assertEqual(result.records, [])
        self.assertEqual(result.page_count, 1)
        self.assertTrue(result.token_exhausted)

    def test_client_exhausts_token_and_preserves_query_shape(self) -> None:
        calls: list[dict[str, list[str]]] = []

        def opener(request, timeout):
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
            calls.append(query)
            if "resumptionToken" in query:
                return (FIXTURES / "page-2.xml").read_bytes()
            return (FIXTURES / "page-1.xml").read_bytes()

        client = OAIClient(
            user_agent="physical-ai-arxiv-feed/0.1 (+https://example.invalid/contact)",
            min_interval_seconds=0,
            opener=opener,
        )
        result = client.harvest(
            source_date="2026-07-14", observed_complete_at="2026-07-14T05:01:00Z"
        )
        self.assertEqual(result.page_count, 2)
        self.assertTrue(result.token_exhausted)
        self.assertEqual(len(result.records), 2)
        self.assertEqual(calls[0]["metadataPrefix"], ["arXivRaw"])
        self.assertEqual(calls[0]["from"], ["2026-07-14"])
        self.assertEqual(calls[1], {"verb": ["ListRecords"], "resumptionToken": ["opaque-token"]})


if __name__ == "__main__":
    unittest.main()
