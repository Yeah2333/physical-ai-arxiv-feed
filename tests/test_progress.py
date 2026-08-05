from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from arxiv_feed.cli import _append_progress


class ProgressTests(unittest.TestCase):
    def test_progress_is_append_only_metadata_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "progress.jsonl"
            _append_progress(
                str(path),
                "page_received",
                source_date="2026-08-05",
                page_number=2,
                record_count=1000,
                token_exhausted=False,
            )
            _append_progress(
                str(path),
                "collection_failed",
                error_type="TimeoutError",
            )
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual([row["event"] for row in rows], ["page_received", "collection_failed"])
        self.assertEqual(rows[0]["record_count"], 1000)
        self.assertEqual(rows[1]["error_type"], "TimeoutError")
        rendered = json.dumps(rows, ensure_ascii=False)
        self.assertNotIn("resumption", rendered)
        self.assertNotIn("abstract", rendered)
        self.assertNotIn("raw", rendered)


if __name__ == "__main__":
    unittest.main()
