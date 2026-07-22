from __future__ import annotations

import json
import unittest
from dataclasses import replace
from pathlib import Path

from arxiv_feed.oai import parse_oai_page
from arxiv_feed.projection import fold_observations, project_records


ROOT = Path(__file__).parents[1]


class ProjectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.scope = json.loads((ROOT / "fixtures/contract/v1/scope.json").read_text())
        cls.raw = parse_oai_page((ROOT / "fixtures/oai/page-1.xml").read_bytes()).records[0]

    def test_alias_enters_scope_and_requery_is_idempotent(self) -> None:
        metadata = dict(self.raw.metadata)
        metadata["categories"] = ["cs.SY"]
        raw = replace(self.raw, metadata=metadata)
        first = project_records(
            scope=self.scope, records=[raw], memberships={}, heads={}
        )
        self.assertEqual([item["record"]["operation"] for item in first.observations], ["upsert"])
        self.assertTrue(next(iter(first.memberships.values())).active_in_scope)

        second = project_records(
            scope=self.scope,
            records=[raw],
            memberships=first.memberships,
            heads=first.heads,
        )
        self.assertEqual(second.observations, [])
        self.assertEqual(second.unchanged_count, 1)

    def test_exit_delete_and_fold(self) -> None:
        entered = project_records(
            scope=self.scope, records=[self.raw], memberships={}, heads={}
        )
        outside_metadata = dict(self.raw.metadata)
        outside_metadata["categories"] = ["econ.GN"]
        outside = replace(self.raw, metadata=outside_metadata)
        exited = project_records(
            scope=self.scope,
            records=[outside],
            memberships=entered.memberships,
            heads=entered.heads,
        )
        self.assertEqual(exited.observations[0]["record"]["operation"], "scope_exit")
        self.assertFalse(next(iter(exited.memberships.values())).active_in_scope)

        deleted_raw = replace(
            self.raw,
            deleted=True,
            versioned_arxiv_id=None,
            current_version=None,
            version_history=None,
            metadata=None,
            field_provenance=None,
        )
        deleted = project_records(
            scope=self.scope,
            records=[deleted_raw],
            memberships=exited.memberships,
            heads=exited.heads,
        )
        self.assertEqual(deleted.observations[0]["record"]["operation"], "source_delete")

        observations = entered.observations + exited.observations + deleted.observations
        memberships, heads = fold_observations(
            scope_id="physical-ai-v1", observations=observations
        )
        self.assertTrue(next(iter(memberships.values())).source_deleted)
        self.assertEqual(next(iter(heads.values())).operation, "source_delete")

    def test_never_in_scope_is_not_published(self) -> None:
        metadata = dict(self.raw.metadata)
        metadata["categories"] = ["econ.GN"]
        result = project_records(
            scope=self.scope,
            records=[replace(self.raw, metadata=metadata)],
            memberships={},
            heads={},
        )
        self.assertEqual(result.observations, [])
        self.assertEqual(result.ignored_count, 1)

    def test_out_of_scope_control_text_cannot_block_projection(self) -> None:
        metadata = dict(self.raw.metadata)
        metadata["categories"] = ["math.AP", "math.OC"]
        metadata["abstract"] = "A 1D Schr\x7fodinger equation."
        result = project_records(
            scope=self.scope,
            records=[replace(self.raw, metadata=metadata)],
            memberships={},
            heads={},
        )
        self.assertEqual(result.observations, [])
        self.assertEqual(result.ignored_count, 1)
        self.assertEqual(result.source_text_repairs, [])

    def test_in_scope_control_text_is_repaired_and_audited(self) -> None:
        metadata = dict(self.raw.metadata)
        metadata["abstract"] = "A robot Schr\x7fodinger policy."
        result = project_records(
            scope=self.scope,
            records=[replace(self.raw, metadata=metadata)],
            memberships={},
            heads={},
        )
        self.assertEqual(
            result.observations[0]["record"]["metadata"]["abstract"],
            "A robot Schr odinger policy.",
        )
        self.assertEqual(
            [repair.as_dict() for repair in result.source_text_repairs],
            [{
                "oai_identifier": self.raw.oai_identifier,
                "source_datestamp": self.raw.source_datestamp,
                "field": "abstract",
                "code_point": "U+007F",
                "count": 1,
                "replacement": "U+0020",
            }],
        )


if __name__ == "__main__":
    unittest.main()
