from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from arxiv_feed.contract import build_observation
from arxiv_feed.writer import FeedWriteError, build_data_parts


ROOT = Path(__file__).parents[1]


class WriterTests(unittest.TestCase):
    def setUp(self) -> None:
        record = json.loads(
            (ROOT / "fixtures/contract/v1/modern-upsert-record.json").read_text()
        )
        self.observation = build_observation(
            scope_id="physical-ai-v1", record=record, supersedes_observation_id=None
        )

    def test_parts_are_content_addressed_and_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            first = build_data_parts(root=Path(first_dir), observations=[self.observation])
            second = build_data_parts(root=Path(second_dir), observations=[self.observation])
            self.assertEqual(first, second)
            descriptor = first[0]
            payload = (Path(first_dir) / descriptor["path"]).read_bytes()
            self.assertEqual(len(payload), descriptor["compressed_size"])
            self.assertEqual(len(gzip.decompress(payload)), descriptor["uncompressed_size"])
            self.assertEqual(descriptor["record_count"], 1)

    def test_empty_and_duplicate_sets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(build_data_parts(root=Path(directory), observations=[]), [])
            with self.assertRaises(FeedWriteError):
                build_data_parts(
                    root=Path(directory), observations=[self.observation, self.observation]
                )

    def test_greedy_split_has_contiguous_ordinals(self) -> None:
        other = json.loads(json.dumps(self.observation))
        other["observation_id"] = "sha256:" + "f" * 64
        with tempfile.TemporaryDirectory() as directory:
            parts = build_data_parts(
                root=Path(directory),
                observations=[other, self.observation],
                max_uncompressed_bytes=1,
            )
            self.assertEqual([item["ordinal"] for item in parts], [0, 1])


if __name__ == "__main__":
    unittest.main()
