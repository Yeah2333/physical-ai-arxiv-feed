from __future__ import annotations

import json
import unittest
from pathlib import Path


SCHEMAS = Path(__file__).parents[1] / "schemas" / "feed" / "v1"


class SchemaTests(unittest.TestCase):
    def test_all_contract_schemas_are_strict_json(self) -> None:
        expected = {
            "feed-root.schema.json",
            "observation.schema.json",
            "partition-manifest.schema.json",
            "scope-index.schema.json",
            "scope.schema.json",
            "state-root.schema.json",
            "status.schema.json",
            "enrichment-task.schema.json",
        }
        self.assertEqual({path.name for path in SCHEMAS.glob("*.json")}, expected)
        for name in expected:
            schema = json.loads((SCHEMAS / name).read_text(encoding="utf-8"))
            self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
            self.assertFalse(schema["additionalProperties"], name)


if __name__ == "__main__":
    unittest.main()
