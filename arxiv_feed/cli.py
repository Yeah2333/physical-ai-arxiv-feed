"""Command line entry point for collection, validation, and publish staging."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .canonical import canonical_file_bytes, canonicalize_categories, sha256_bytes
from .enrichment import SearchClient, SearchOutcome, task_due
from .feed import (
    FeedValidationError,
    ProducerIdentity,
    SnapshotBuilder,
    build_producer_correction,
    canonical_tree_sha256,
    load_part_observations,
    load_state_records,
    validate_snapshot,
)
from .oai import OAIClient
from .planner import plan_collection
from .writer import write_canonical_file


GENERATED_TOP_LEVEL = {"feed.json", "scopes", "objects"}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _previous_index(feed_dir: Path) -> dict[str, Any] | None:
    if not (feed_dir / "feed.json").exists():
        return None
    validate_snapshot(feed_dir)
    feed = _read_json(feed_dir / "feed.json")
    return _read_json(feed_dir / feed["scopes"][0]["index_path"])


def _copy_feed(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for name in GENERATED_TOP_LEVEL:
        source_path = source / name
        destination_path = destination / name
        if not source_path.exists():
            continue
        if source_path.is_dir():
            shutil.copytree(source_path, destination_path, dirs_exist_ok=True)
        else:
            shutil.copy2(source_path, destination_path)


def _incremental_tree_bytes(previous: Path, current: Path) -> int:
    previous_hashes = {
        path.relative_to(previous).as_posix(): sha256_bytes(path.read_bytes())
        for path in previous.rglob("*")
        if path.is_file()
    } if previous.exists() else {}
    total = 0
    for path in current.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(current).as_posix()
        payload = path.read_bytes()
        if previous_hashes.get(relative) != sha256_bytes(payload):
            total += len(payload)
    return total


def _operation_counts(
    previous_feed: Path,
    feed_dir: Path,
    changed_dates: list[str],
    *,
    scope_id: str,
) -> dict[str, int]:
    counts = {name: 0 for name in ("upsert", "scope_exit", "source_delete", "producer_retract")}
    if not changed_dates:
        return counts
    feed = _read_json(feed_dir / "feed.json")
    index = _read_json(feed_dir / feed["scopes"][0]["index_path"])
    previous_ids: dict[str, set[str]] = {}
    if (previous_feed / "feed.json").exists():
        old_feed = _read_json(previous_feed / "feed.json")
        old_index = _read_json(previous_feed / old_feed["scopes"][0]["index_path"])
        for reference in old_index["partitions"]:
            if reference["source_date"] not in changed_dates:
                continue
            manifest = _read_json(previous_feed / reference["manifest_path"])
            previous_ids[reference["source_date"]] = {
                item["observation_id"]
                for item in load_part_observations(
                    previous_feed,
                    manifest["data_parts"],
                    scope_id=scope_id,
                )
            }
    for reference in index["partitions"]:
        if reference["source_date"] not in changed_dates:
            continue
        manifest = _read_json(feed_dir / reference["manifest_path"])
        for observation in load_part_observations(
            feed_dir,
            manifest["data_parts"],
            scope_id=scope_id,
        ):
            if observation["observation_id"] in previous_ids.get(reference["source_date"], set()):
                continue
            counts[observation["record"]["operation"]] += 1
    return counts


def _producer_identity(args: argparse.Namespace, repository: Path) -> ProducerIdentity:
    config_paths = [repository / "config/feed-scope.json", repository / "config/feed-producer.json"]
    schema_paths = list((repository / "schemas/feed/v1").glob("*.json"))
    code_sha = args.code_sha.lower()
    if len(code_sha) != 40 or any(char not in "0123456789abcdef" for char in code_sha):
        raise ValueError("--code-sha must be a full 40-character Git SHA")
    return ProducerIdentity(
        code_sha=code_sha,
        config_sha256=canonical_tree_sha256(config_paths, base=repository),
        schema_sha256=canonical_tree_sha256(schema_paths, base=repository),
        workflow_run_id=args.workflow_run_id,
    )


def collect(args: argparse.Namespace) -> int:
    repository = Path(args.repository).resolve()
    input_feed = Path(args.input_feed).resolve()
    output_feed = Path(args.output_feed).resolve()
    result_path = Path(args.result_file).resolve()
    if output_feed.exists() and any(output_feed.iterdir()):
        raise ValueError("--output-feed must be absent or empty")
    _copy_feed(input_feed, output_feed)
    previous = _previous_index(output_feed)
    scope = _read_json(repository / "config/feed-scope.json")
    config = _read_json(repository / "config/feed-producer.json")
    producer_corrections: dict[str, list[dict[str, Any]]] = {}
    if args.correction_file:
        if not args.full_projection_correction:
            raise ValueError(
                "--correction-file requires --full-projection-correction"
            )
        correction_spec = _read_json(Path(args.correction_file).resolve())
        producer_corrections = build_producer_correction(
            output_feed,
            scope_id=scope["scope_id"],
            spec=correction_spec,
        )
    today = date.fromisoformat(args.today) if args.today else datetime.now(timezone.utc).date()
    plan = plan_collection(today_utc=today, producer_config=config, previous_index=previous)
    client = OAIClient(
        endpoint=config["oai_endpoint"],
        user_agent=args.user_agent,
        max_attempts=config["max_retries"],
        min_interval_seconds=float(config["request_interval_seconds"]),
    )
    harvests = {source_date: client.harvest(source_date=source_date) for source_date in plan.source_dates}
    generated_at = _utc_now()
    enrichment_config = config.get("search_enrichment") or {}
    enrichment_enabled = bool(enrichment_config.get("enabled", False))
    enrichment_outcomes: dict[str, SearchOutcome] = {}
    if enrichment_enabled:
        previous_tasks = (
            load_state_records(
                output_feed,
                previous["enrichment_root"],
                expected_kind="enrichment_backlog",
                expected_scope=scope["scope_id"],
                expected_coverage=None,
            )
            if previous is not None
            else []
        )
        due_backlog = [
            item for item in previous_tasks if task_due(item, today_utc=today)
        ]
        due_backlog.sort(key=lambda item: (item["first_enqueued_at"], item["task_key"]))
        new_ids: list[str] = []
        scope_categories = set(scope["categories"])
        for harvest in harvests.values():
            for raw in harvest.records:
                if raw.deleted or raw.metadata is None:
                    continue
                canonical = set(
                    canonicalize_categories(
                        raw.metadata["categories"], scope["category_aliases"]
                    )
                )
                if canonical.intersection(scope_categories):
                    new_ids.append(raw.base_arxiv_id)
        max_per_run = int(enrichment_config.get("max_per_run", 20))
        requested: list[str] = []
        for base_id in [item["base_arxiv_id"] for item in due_backlog] + sorted(set(new_ids)):
            if base_id not in requested:
                requested.append(base_id)
            if len(requested) >= max_per_run:
                break
        search_client = SearchClient(
            endpoint=str(enrichment_config.get("endpoint") or "https://export.arxiv.org/api/query"),
            user_agent=args.user_agent,
            max_attempts=int(enrichment_config.get("max_retries", 3)),
            min_interval_seconds=float(enrichment_config.get("request_interval_seconds", 3)),
        )
        batch_size = max(1, int(enrichment_config.get("batch_size", 20)))
        for start in range(0, len(requested), batch_size):
            enrichment_outcomes.update(
                search_client.fetch_many(requested[start : start + batch_size])
            )
    builder = SnapshotBuilder(
        root=output_feed,
        scope=scope,
        producer=_producer_identity(args, repository),
    )
    snapshot_kind = (
        "full_projection_correction"
        if args.full_projection_correction
        else "ordinary"
    )
    try:
        result = builder.build(
            today_utc=today,
            generated_at=generated_at,
            coverage_start=plan.coverage_start,
            harvests=harvests,
            snapshot_kind=snapshot_kind,
            enrichment_enabled=enrichment_enabled,
            enrichment_outcomes=enrichment_outcomes,
            producer_corrections=producer_corrections,
        )
    except FeedValidationError as exc:
        if (
            snapshot_kind == "ordinary"
            and enrichment_enabled
            and "historical correction rewrites downstream supersedes" in str(exc)
        ):
            # SnapshotBuilder writes immutable candidate objects before it can
            # know whether a downstream supersedes chain must be rewritten.
            # Restart from the exact prior Feed so failed-attempt objects do
            # not leak into the full snapshot's closure.
            shutil.rmtree(output_feed)
            _copy_feed(input_feed, output_feed)
            builder = SnapshotBuilder(
                root=output_feed,
                scope=scope,
                producer=_producer_identity(args, repository),
            )
            snapshot_kind = "full_projection_correction"
            result = builder.build(
                today_utc=today,
                generated_at=generated_at,
                coverage_start=plan.coverage_start,
                harvests=harvests,
                snapshot_kind=snapshot_kind,
                enrichment_enabled=enrichment_enabled,
                enrichment_outcomes=enrichment_outcomes,
                producer_corrections=producer_corrections,
            )
        else:
            raise
    if (
        snapshot_kind != "full_projection_correction"
        and len(result.changed_partitions) > int(config["max_changed_partitions"])
    ):
        raise ValueError("snapshot exceeds the changed-partition cap")
    incremental_bytes = _incremental_tree_bytes(input_feed, output_feed)
    estimated_pack_bytes = int(args.base_reachable_pack_bytes) + incremental_bytes
    if estimated_pack_bytes >= int(config["max_reachable_pack_bytes"]):
        raise ValueError(
            "estimated reachable Feed pack reaches the hard capacity gate"
        )
    capacity_status = (
        "warning"
        if estimated_pack_bytes >= int(config["warning_reachable_pack_bytes"])
        else "ok"
    )
    operation_counts = _operation_counts(
        input_feed,
        output_feed,
        result.changed_partitions,
        scope_id=scope["scope_id"],
    )
    status = {
        "status_schema_version": "1.0",
        "feed_id": "physical-ai-arxiv-facts-v1",
        "scope_id": scope["scope_id"],
        "last_workflow_success_at": generated_at,
        "latest_query_complete_at": max(
            (item.observed_complete_at for item in harvests.values()), default=generated_at
        ),
        "closed_complete_through": result.closed_complete_through,
        "membership_closed_through": result.closed_complete_through,
        "pending_gaps": [
            item for item in plan.pending_before
            if result.closed_complete_through is None or item > result.closed_complete_through
        ],
        "provisional_dates": result.provisional_dates,
        "changed_partitions": result.changed_partitions,
        "snapshot_kind": snapshot_kind,
        "enrichment_backlog_count": result.enrichment_backlog_count,
        "enrichment_oldest_age_hours": result.enrichment_oldest_age_hours,
        "reachable_pack_size_bytes": int(args.base_reachable_pack_bytes),
        "estimated_reachable_pack_size_bytes": estimated_pack_bytes,
        "capacity_status": capacity_status,
        "operation_counts": operation_counts,
        "feed_root_sha256": result.feed_root_sha256,
        "index_sha256": result.index_sha256,
        "workflow_run_url": args.workflow_run_url,
    }
    write_canonical_file(
        output_feed,
        f"scopes/{scope['scope_id']}/status/latest.json",
        status,
    )
    artifact = {
        "artifact_schema_version": "1.0",
        "base_feed_commit": args.base_feed_commit,
        "feed_root_sha256": result.feed_root_sha256,
        "index_sha256": result.index_sha256,
        "changed_partitions": result.changed_partitions,
        "snapshot_kind": status["snapshot_kind"],
        "generated_at": generated_at,
    }
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_bytes(canonical_file_bytes(artifact))
    print(json.dumps(artifact, ensure_ascii=False, sort_keys=True))
    return 0


def validate(args: argparse.Namespace) -> int:
    result = validate_snapshot(Path(args.feed_dir).resolve())
    print(json.dumps(result.__dict__, ensure_ascii=False, sort_keys=True))
    return 0


def sync_publish_tree(args: argparse.Namespace) -> int:
    staged = Path(args.staged_feed).resolve()
    destination = Path(args.feed_worktree).resolve()
    validate_snapshot(staged)
    unexpected = {path.name for path in staged.iterdir()} - GENERATED_TOP_LEVEL
    if unexpected:
        raise ValueError(f"staged Feed contains unexpected top-level paths: {sorted(unexpected)}")
    _copy_feed(staged, destination)
    validate_snapshot(destination)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m arxiv_feed.cli")
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--repository", default=".")
    collect_parser.add_argument("--input-feed", required=True)
    collect_parser.add_argument("--output-feed", required=True)
    collect_parser.add_argument("--result-file", required=True)
    collect_parser.add_argument("--base-feed-commit", required=True)
    collect_parser.add_argument("--base-reachable-pack-bytes", type=int, default=0)
    collect_parser.add_argument("--code-sha", required=True)
    collect_parser.add_argument("--workflow-run-id", required=True)
    collect_parser.add_argument("--workflow-run-url", required=True)
    collect_parser.add_argument("--today")
    collect_parser.add_argument("--full-projection-correction", action="store_true")
    collect_parser.add_argument(
        "--correction-file",
        help="Reviewed producer-retract JSON spec; requires --full-projection-correction.",
    )
    collect_parser.add_argument(
        "--user-agent",
        default="physical-ai-arxiv-feed/0.1 (+https://github.com/Yeah2333/physical-ai-arxiv-feed/issues)",
    )
    collect_parser.set_defaults(function=collect)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--feed-dir", required=True)
    validate_parser.set_defaults(function=validate)

    sync_parser = subparsers.add_parser("sync-publish-tree")
    sync_parser.add_argument("--staged-feed", required=True)
    sync_parser.add_argument("--feed-worktree", required=True)
    sync_parser.set_defaults(function=sync_publish_tree)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.function(args))
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
