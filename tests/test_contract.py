from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from arxiv_feed.contract import (
    ContractValidationError,
    build_observation,
    scope_exact_sha256,
    validate_observation,
    validate_record,
    validate_scope,
)


FIXTURES = Path(__file__).parents[1] / "fixtures" / "contract" / "v1"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.vectors = load_fixture("canonical-vectors.json")

    def test_scope_has_no_remote_filtering(self) -> None:
        scope = load_fixture("scope.json")
        self.assertIs(validate_scope(scope), scope)
        self.assertEqual(scope_exact_sha256(scope), self.vectors["scope_exact_sha256"])

        invalid = copy.deepcopy(scope)
        invalid["keywords"] = ["robot"]
        with self.assertRaises(ContractValidationError):
            validate_scope(invalid)

    def test_modern_and_legacy_records(self) -> None:
        for name in ("modern-upsert-record.json", "legacy-upsert-record.json"):
            with self.subTest(name=name):
                record = load_fixture(name)
                self.assertIs(validate_record(record), record)

    def test_observation_hashes_and_identity_are_recomputed(self) -> None:
        record = load_fixture("modern-upsert-record.json")
        observation = build_observation(
            scope_id="physical-ai-v1",
            record=record,
            supersedes_observation_id=None,
        )
        self.assertEqual(observation["record_content_sha256"], self.vectors["identities"]["modern_record_content_sha256"])
        self.assertEqual(observation["observation_id"], self.vectors["identities"]["modern_observation_id"])
        self.assertIs(validate_observation(observation, scope_id="physical-ai-v1"), observation)

        tampered = copy.deepcopy(observation)
        tampered["record"]["metadata"]["title"] = "Tampered"
        with self.assertRaises(ContractValidationError):
            validate_observation(tampered, scope_id="physical-ai-v1")

    def test_unknown_keys_and_unnormalized_text_fail_closed(self) -> None:
        record = load_fixture("modern-upsert-record.json")
        record["surprise"] = True
        with self.assertRaises(ContractValidationError):
            validate_record(record)

        record = load_fixture("modern-upsert-record.json")
        record["metadata"]["title"] = "  padded"
        with self.assertRaises(ContractValidationError):
            validate_record(record)


if __name__ == "__main__":
    unittest.main()
