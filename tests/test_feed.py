from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path

from arxiv_feed.feed import (
    FeedValidationError,
    ProducerIdentity,
    SnapshotBuilder,
    build_producer_correction,
    load_part_observations,
    validate_snapshot,
)
from arxiv_feed.oai import HarvestResult, parse_oai_page


ROOT = Path(__file__).parents[1]


def harvest(raw, source_date: str, observed: str) -> HarvestResult:
    return HarvestResult(
        records=[replace(raw, source_datestamp=source_date)],
        first_response_date=observed,
        last_response_date=observed,
        page_count=1,
        token_exhausted=True,
        observed_complete_at=observed,
    )


class FeedSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = json.loads((ROOT / "config/feed-scope.json").read_text())
        cls.raw = parse_oai_page((ROOT / "fixtures/oai/page-1.xml").read_bytes()).records[0]
        cls.producer = ProducerIdentity(
            code_sha="a" * 40,
            config_sha256="sha256:" + "b" * 64,
            schema_sha256="sha256:" + "c" * 64,
            workflow_run_id="fixture-run",
        )

    def test_bootstrap_closed_plus_provisional_and_noop_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            feed_root = Path(directory)
            builder = SnapshotBuilder(
                root=feed_root, scope=self.scope, producer=self.producer
            )
            result = builder.build(
                today_utc=date(2026, 7, 15),
                generated_at="2026-07-15T05:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={
                    "2026-07-14": harvest(self.raw, "2026-07-14", "2026-07-15T04:58:00Z"),
                    "2026-07-15": harvest(self.raw, "2026-07-15", "2026-07-15T05:00:00Z"),
                },
            )
            self.assertEqual(result.closed_complete_through, "2026-07-14")
            self.assertEqual(result.provisional_dates, ["2026-07-15"])
            self.assertEqual(result.changed_partitions, ["2026-07-14", "2026-07-15"])
            self.assertEqual(validate_snapshot(feed_root).feed_root_sha256, result.feed_root_sha256)

            replay = builder.build(
                today_utc=date(2026, 7, 15),
                generated_at="2026-07-15T06:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={},
            )
            self.assertEqual(replay.changed_partitions, [])
            self.assertEqual(replay.feed_root_sha256, result.feed_root_sha256)

    def test_provisional_does_not_close_without_requery(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            feed_root = Path(directory)
            builder = SnapshotBuilder(root=feed_root, scope=self.scope, producer=self.producer)
            first = builder.build(
                today_utc=date(2026, 7, 15),
                generated_at="2026-07-15T05:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={
                    "2026-07-14": harvest(self.raw, "2026-07-14", "2026-07-15T04:58:00Z"),
                    "2026-07-15": harvest(self.raw, "2026-07-15", "2026-07-15T05:00:00Z"),
                },
            )
            second = builder.build(
                today_utc=date(2026, 7, 16),
                generated_at="2026-07-16T05:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={},
            )
            self.assertEqual(second.feed_root_sha256, first.feed_root_sha256)
            self.assertEqual(validate_snapshot(feed_root).closed_complete_through, "2026-07-14")
            self.assertEqual(validate_snapshot(feed_root).provisional_dates, ["2026-07-15"])

    def test_hash_tampering_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            feed_root = Path(directory)
            SnapshotBuilder(root=feed_root, scope=self.scope, producer=self.producer).build(
                today_utc=date(2026, 7, 15),
                generated_at="2026-07-15T05:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={"2026-07-14": harvest(self.raw, "2026-07-14", "2026-07-15T04:58:00Z")},
            )
            feed = json.loads((feed_root / "feed.json").read_text())
            feed["scopes"][0]["index_sha256"] = "sha256:" + "0" * 64
            (feed_root / "feed.json").write_text(json.dumps(feed, separators=(",", ":")) + "\n")
            with self.assertRaises(FeedValidationError):
                validate_snapshot(feed_root)

    def test_historical_change_requires_and_builds_full_projection_correction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            feed_root = Path(directory)
            builder = SnapshotBuilder(root=feed_root, scope=self.scope, producer=self.producer)
            first_record = replace(
                self.raw,
                metadata={**self.raw.metadata, "title": "Initial title"},
            )
            later_record = replace(
                self.raw,
                metadata={**self.raw.metadata, "title": "Later title"},
            )
            builder.build(
                today_utc=date(2026, 7, 16),
                generated_at="2026-07-16T05:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={
                    "2026-07-14": harvest(first_record, "2026-07-14", "2026-07-15T05:00:00Z"),
                    "2026-07-15": harvest(later_record, "2026-07-15", "2026-07-16T05:00:00Z"),
                },
            )
            corrected_record = replace(
                self.raw,
                metadata={**self.raw.metadata, "title": "Corrected initial title"},
            )
            with self.assertRaisesRegex(FeedValidationError, "full_projection_correction"):
                builder.build(
                    today_utc=date(2026, 7, 17),
                    generated_at="2026-07-17T05:00:00Z",
                    coverage_start=date(2026, 7, 14),
                    harvests={
                        "2026-07-14": harvest(corrected_record, "2026-07-14", "2026-07-17T04:59:00Z")
                    },
                )

            result = builder.build(
                today_utc=date(2026, 7, 17),
                generated_at="2026-07-17T05:05:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={
                    "2026-07-14": harvest(corrected_record, "2026-07-14", "2026-07-17T05:04:00Z")
                },
                snapshot_kind="full_projection_correction",
            )
            self.assertIn("2026-07-15", result.changed_partitions)
            self.assertEqual(validate_snapshot(feed_root).feed_root_sha256, result.feed_root_sha256)

    def test_producer_retract_cascades_as_one_full_projection_correction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            feed_root = Path(directory)
            builder = SnapshotBuilder(root=feed_root, scope=self.scope, producer=self.producer)
            first_record = replace(
                self.raw,
                metadata={**self.raw.metadata, "title": "Initial title"},
            )
            later_record = replace(
                self.raw,
                metadata={**self.raw.metadata, "title": "Later title"},
            )
            builder.build(
                today_utc=date(2026, 7, 16),
                generated_at="2026-07-16T05:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={
                    "2026-07-14": harvest(first_record, "2026-07-14", "2026-07-16T04:55:00Z"),
                    "2026-07-15": harvest(later_record, "2026-07-15", "2026-07-16T04:57:00Z"),
                },
            )
            target = self._partition_observations(feed_root, "2026-07-14")[0]
            correction = build_producer_correction(
                feed_root,
                scope_id=self.scope["scope_id"],
                spec={
                    "producer_correction_key": "reviewed-fixture-correction-1",
                    "target_observation_id": target["observation_id"],
                    "producer_observed_at": "2026-07-16T06:00:00Z",
                    "reason_code": "fixture_bad_source_fact",
                    "reason": "Fixture correction for a known bad historical source fact.",
                    "replacement_observation_id": None,
                },
            )
            with self.assertRaisesRegex(
                FeedValidationError,
                "historical correction rewrites downstream supersedes",
            ):
                builder.build(
                    today_utc=date(2026, 7, 16),
                    generated_at="2026-07-16T06:00:00Z",
                    coverage_start=date(2026, 7, 14),
                    harvests={},
                    producer_corrections=correction,
                )
            corrected = builder.build(
                today_utc=date(2026, 7, 16),
                generated_at="2026-07-16T06:00:00Z",
                coverage_start=date(2026, 7, 14),
                harvests={},
                snapshot_kind="full_projection_correction",
                producer_corrections=correction,
            )
            self.assertEqual(
                corrected.changed_partitions, ["2026-07-14", "2026-07-15"]
            )
            first_partition = self._partition_observations(feed_root, "2026-07-14")
            self.assertEqual(
                [item["record"]["operation"] for item in first_partition].count(
                    "producer_retract"
                ),
                1,
            )
            validate_snapshot(feed_root)

    def _partition_observations(self, root: Path, source_date: str):
        feed = json.loads((root / "feed.json").read_text())
        index = json.loads((root / feed["scopes"][0]["index_path"]).read_text())
        reference = next(
            item for item in index["partitions"] if item["source_date"] == source_date
        )
        manifest = json.loads((root / reference["manifest_path"]).read_text())
        return load_part_observations(
            root,
            manifest["data_parts"],
            scope_id=self.scope["scope_id"],
        )


if __name__ == "__main__":
    unittest.main()
