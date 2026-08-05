from __future__ import annotations

import unittest
from pathlib import Path


class WorkflowSafetyTests(unittest.TestCase):
    def test_failed_or_timed_out_collection_cannot_schedule_publish(self) -> None:
        workflow = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "arxiv-fact-feed.yml"
        ).read_text(encoding="utf-8")
        publish = workflow.split("\n  publish:\n", maxsplit=1)[1]

        # GitHub's default `needs` behavior skips this job unless collect
        # succeeds.  Do not add `if: always()` to the publish job.
        self.assertIn("    needs: collect\n", publish)
        self.assertNotIn("    if: always()\n", publish)

        # Only diagnostics are failure-tolerant; the validated input consumed
        # by publish is uploaded exclusively after a complete collection.
        self.assertIn("      - name: Upload validated publish artifact\n        if: success()", workflow)
        self.assertIn("      - name: Upload collection diagnostics\n        if: always()", workflow)
        self.assertIn("    timeout-minutes: 90", workflow)
        self.assertIn("        timeout-minutes: 80", workflow)


if __name__ == "__main__":
    unittest.main()
