from __future__ import annotations

import unittest
from datetime import date

from arxiv_feed.planner import plan_collection


CONFIG = {
    "bootstrap_days": 31,
    "max_gap_closures": 5,
    "max_changed_partitions": 7,
}


class PlannerTests(unittest.TestCase):
    def test_bootstrap_prioritizes_five_oldest_gaps(self) -> None:
        plan = plan_collection(
            today_utc=date(2026, 7, 15), producer_config=CONFIG, previous_index=None
        )
        self.assertEqual(plan.coverage_start.isoformat(), "2026-06-15")
        self.assertEqual(plan.source_dates, [
            "2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19"
        ])

    def test_last_gap_and_current_provisional_share_a_run(self) -> None:
        index = {
            "coverage": {
                "start_date": "2026-06-15",
                "closed_complete_through": "2026-07-13",
            }
        }
        plan = plan_collection(
            today_utc=date(2026, 7, 15), producer_config=CONFIG, previous_index=index
        )
        self.assertEqual(plan.source_dates, ["2026-07-14", "2026-07-15"])

    def test_steady_state_rechecks_previous_and_current(self) -> None:
        index = {
            "coverage": {
                "start_date": "2026-06-15",
                "closed_complete_through": "2026-07-14",
            }
        }
        plan = plan_collection(
            today_utc=date(2026, 7, 15), producer_config=CONFIG, previous_index=index
        )
        self.assertEqual(plan.source_dates, ["2026-07-14", "2026-07-15"])


if __name__ == "__main__":
    unittest.main()
