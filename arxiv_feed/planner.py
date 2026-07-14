"""Bounded, gap-live source-date planning for scheduled collection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any


@dataclass(frozen=True)
class CollectionPlan:
    coverage_start: date
    source_dates: list[str]
    pending_before: list[str]


def plan_collection(
    *, today_utc: date, producer_config: dict[str, Any], previous_index: dict[str, Any] | None
) -> CollectionPlan:
    bootstrap_days = int(producer_config["bootstrap_days"])
    max_gaps = int(producer_config["max_gap_closures"])
    max_changes = int(producer_config["max_changed_partitions"])
    if bootstrap_days < 1 or max_gaps < 1 or max_changes < 1:
        raise ValueError("producer planning limits must be positive")
    if previous_index is None:
        coverage_start = today_utc - timedelta(days=bootstrap_days - 1)
        closed_through = None
    else:
        coverage_start = date.fromisoformat(previous_index["coverage"]["start_date"])
        closed_value = previous_index["coverage"]["closed_complete_through"]
        closed_through = date.fromisoformat(closed_value) if closed_value else None
    cursor = (closed_through + timedelta(days=1)) if closed_through else coverage_start
    pending: list[str] = []
    while cursor < today_utc:
        pending.append(cursor.isoformat())
        cursor += timedelta(days=1)

    selected = pending[:max_gaps]
    all_gaps_selected = len(selected) == len(pending)
    previous_date = (today_utc - timedelta(days=1)).isoformat()
    current_date = today_utc.isoformat()
    if not pending:
        selected.extend([previous_date, current_date])
    elif all_gaps_selected:
        selected.append(current_date)
    selected = list(dict.fromkeys(selected))
    if len(selected) > max_changes:
        selected = selected[:max_changes]
    return CollectionPlan(
        coverage_start=coverage_start,
        source_dates=selected,
        pending_before=pending,
    )
