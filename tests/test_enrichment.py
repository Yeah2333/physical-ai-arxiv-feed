from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from arxiv_feed.enrichment import (
    EnrichmentError,
    SearchOutcome,
    SearchRecord,
    apply_search_result,
    build_task,
    parse_search_feed,
    transition_failure,
)
from arxiv_feed.feed import ProducerIdentity, SnapshotBuilder, validate_snapshot
from arxiv_feed.oai import HarvestResult, parse_oai_page
from arxiv_feed.projection import project_records


ROOT = Path(__file__).parents[1]


class EnrichmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = json.loads((ROOT / "config/feed-scope.json").read_text())
        cls.raw = parse_oai_page((ROOT / "fixtures/oai/page-1.xml").read_bytes()).records[0]
        cls.observation = project_records(
            scope=cls.scope,
            records=[cls.raw],
            memberships={},
            heads={},
        ).observations[0]
        cls.search_record = SearchRecord(
            base_arxiv_id=cls.raw.base_arxiv_id,
            version=cls.raw.current_version,
            title=cls.raw.metadata["title"],
            abstract=cls.raw.metadata["abstract"],
            authors=[{"name": "Alice Example"}, {"name": "Bob Example"}],
            primary_category="cs.RO",
        )

    def test_search_parser_and_exact_target_binding(self) -> None:
        payload = f"""<?xml version='1.0'?>
        <feed xmlns='http://www.w3.org/2005/Atom' xmlns:arxiv='http://arxiv.org/schemas/atom'>
          <entry>
            <id>https://arxiv.org/abs/{self.raw.base_arxiv_id}v{self.raw.current_version}</id>
            <title>{self.raw.metadata['title']}</title>
            <summary>{self.raw.metadata['abstract']}</summary>
            <author><name>Alice Example</name></author>
            <author><name>Bob Example</name></author>
            <arxiv:primary_category term='cs.RO'/>
          </entry>
        </feed>""".encode()
        parsed = parse_search_feed(payload)[self.raw.base_arxiv_id]
        enriched = apply_search_result(
            scope_id=self.scope["scope_id"],
            target_observation=self.observation,
            result=parsed,
        )
        self.assertEqual(
            enriched["supersedes_observation_id"], self.observation["observation_id"]
        )
        self.assertEqual(
            enriched["record"]["field_provenance"]["authors"],
            "search_api_optional",
        )

        ahead = SearchRecord(**{**self.search_record.__dict__, "version": 3})
        with self.assertRaisesRegex(EnrichmentError, "target_ahead"):
            apply_search_result(
                scope_id=self.scope["scope_id"],
                target_observation=self.observation,
                result=ahead,
            )
        mismatch = SearchRecord(**{**self.search_record.__dict__, "title": "Wrong title"})
        with self.assertRaisesRegex(EnrichmentError, "target_material_mismatch"):
            apply_search_result(
                scope_id=self.scope["scope_id"],
                target_observation=self.observation,
                result=mismatch,
            )

    def test_search_xml_declarations_are_rejected(self) -> None:
        with self.assertRaisesRegex(EnrichmentError, "declarations"):
            parse_search_feed(
                b'<!DOCTYPE x [<!ENTITY e "boom">]><x>&e;</x>'
            )

    def test_failure_backlog_defers_after_seven_attempts(self) -> None:
        task = build_task(
            self.observation,
            scope_id=self.scope["scope_id"],
            first_enqueued_at="2026-07-14T05:00:00Z",
        )
        for offset in range(7):
            task = transition_failure(
                task,
                attempted_at=f"2026-07-{14 + offset:02d}T05:00:00Z",
                today_utc=date(2026, 7, 14 + offset),
                error_kind="search_transport",
                error="temporary failure",
            )
        self.assertEqual(task["attempt_count"], 7)
        self.assertEqual(task["status"], "deferred")
        self.assertEqual(task["next_attempt_date"], "2026-07-27")

    def test_optional_failure_does_not_drop_oai_and_later_success_supersedes(self) -> None:
        producer = ProducerIdentity(
            code_sha="a" * 40,
            config_sha256="sha256:" + "b" * 64,
            schema_sha256="sha256:" + "c" * 64,
            workflow_run_id="enrichment-test",
        )
        harvest = HarvestResult(
            records=[self.raw],
            first_response_date="2026-07-15T04:58:00Z",
            last_response_date="2026-07-15T04:58:00Z",
            page_count=1,
            token_exhausted=True,
            observed_complete_at="2026-07-15T04:58:00Z",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            builder = SnapshotBuilder(root=root, scope=self.scope, producer=producer)
            failed = builder.build(
                today_utc=date(2026, 7, 15),
                generated_at="2026-07-15T05:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={"2026-07-14": harvest},
                enrichment_enabled=True,
                enrichment_outcomes={
                    self.raw.base_arxiv_id: SearchOutcome(
                        None, "search_transport", "temporary failure"
                    )
                },
            )
            self.assertEqual(failed.enrichment_backlog_count, 1)
            self.assertEqual(failed.closed_complete_through, "2026-07-14")
            validate_snapshot(root)

            succeeded = builder.build(
                today_utc=date(2026, 7, 15),
                generated_at="2026-07-15T06:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={},
                enrichment_enabled=True,
                enrichment_outcomes={
                    self.raw.base_arxiv_id: SearchOutcome(self.search_record)
                },
            )
            self.assertEqual(succeeded.enrichment_backlog_count, 0)
            self.assertEqual(succeeded.changed_partitions, ["2026-07-14"])
            validate_snapshot(root)


if __name__ == "__main__":
    unittest.main()
