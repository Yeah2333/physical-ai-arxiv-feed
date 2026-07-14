# arXiv Fact Feed v1

This repository produces a public, machine-verifiable stream of arXiv source facts. It does not decide whether a paper matters to a particular researcher.

## Responsibility boundary

The scheduled producer enumerates the global OAI-PMH change stream with `metadataPrefix=arXivRaw`, projects the pinned category scope and publishes a content-addressed snapshot. It never uses keywords, an LLM, a personal profile or a private model token.

The local `physical-ai-research-radar` consumer owns trust anchoring, metadata guards, Saved/core continuity, the deterministic `cs.RO` route, title-and-abstract-only Admission, expert Triage, Chinese briefs, PDF deep reads and all user state.

## Scope and coverage

The versioned scope in `config/feed-scope.json` contains exactly:

`cs.RO`, `cs.AI`, `cs.CV`, `cs.LG`, `cs.CL`, `cs.MA`, `cs.HC`, `cs.GR`, `eess.SY`, `eess.SP`.

The raw source category `cs.SY` is preserved in observations and aliases to `eess.SY` only for scope intersection. `keywords` is required to be an empty array.

The planner is gap-first:

- Bootstrap queries up to five oldest missing UTC datestamps within the configured 31-day window.
- Steady state rechecks the previous and current UTC datestamps so a provisional partition is never promoted merely because time passed.
- Pagination is exhausted through OAI resumption tokens. An OAI error, malformed record or incomplete page fails the run.
- `closed_complete_through` advances only over a contiguous prefix of successfully requeried partitions. The current date remains provisional.

The initial 31-day bootstrap is intentionally bounded to five oldest gaps per run. It therefore needs repeated scheduled or manual runs before reaching steady state; status exposes the remaining gaps, and consumers must not count bootstrap days as current-day shadow evidence until the frontier has caught up.

An OAI upsert is projected only while its canonical categories intersect the scope. Later category exit emits `scope_exit`; OAI deleted headers emit `source_delete` for previously known in-scope identities. A record that never entered scope is not published.

## Identity and operation model

Modern and legacy arXiv identifiers are normalized into a stable base id plus explicit version. Each observation has a deterministic logical key, content-derived observation id, source datestamp, operation and field-level provenance.

Operations are:

- `upsert`: source metadata/version is active in scope.
- `scope_exit`: a later source version no longer intersects the scope.
- `source_delete`: OAI reports deletion for a previously known in-scope item.
- `producer_retract`: a reviewed correction retracts a specific producer observation and optionally points to a replacement.

Consumers fold operations in producer order. History is never force-rewritten.

## Canonical serialization and object closure

All contract JSON uses the supported JCS subset and normalized UTF-8 text. Numbers outside the safe subset, non-string keys, control characters, unknown schema keys and non-normalized text fail closed. Data parts are deterministic gzip streams with fixed headers.

Every file is SHA-256 addressed or referenced by an exact SHA-256 and byte count. Validation recomputes observation ids, logical keys, part hashes, manifests, state roots, scope index and `feed.json`, and rejects missing or unreferenced objects. Contract fixtures under `fixtures/contract/v1/` are shared with the local consumer.

## Snapshot tree

The generated `feed` branch contains only:

```text
feed.json
scopes/physical-ai-v1/
  index.json
  status/latest.json
objects/
  ... immutable manifests, gzip parts and sharded producer state
```

The scope index records partition manifests plus coverage and state roots. A partition manifest records exact data parts, counts, producer identity and `ordinary` versus `full_projection_correction` kind. Producer identity pins code, configuration, schema and workflow run.

## Optional Search enrichment

OAI-PMH is authoritative for discovery, version, categories, deletion and coverage. `config/feed-producer.json` also defines an optional arXiv Search API enrichment worker for title, abstract, structured authors and primary category.

It is disabled by default until real rate/capacity behavior is measured. When enabled:

- only exact returned arXiv base/version identities are accepted;
- Search failure never removes or delays the OAI fact observation;
- a success emits a later superseding `upsert` with per-field Search provenance;
- tasks persist in strict, content-addressed state shards;
- retry cadence is bounded, and after seven failures a task is deferred to weekly retry;
- `enrichment_backlog_count` and `enrichment_oldest_age_hours` appear in status and are cross-checked against state.

Delayed enrichment of a historical partition can require a cascading supersedes rewrite. The producer automatically upgrades that validated retry to a full projection correction rather than publishing an internally inconsistent ordinary snapshot.

## Capacity and status

The producer calculates incremental object bytes and combines them with the reachable prior Feed pack estimate supplied by the workflow. `warning_reachable_pack_bytes` sets a visible warning; `max_reachable_pack_bytes` fails before publication. Status also records gaps, provisional dates, changed partitions, operation counts, enrichment backlog, exact Feed/index hashes and workflow URL.

Operation counts include only observation ids newly introduced relative to the prior Feed, even when a corrected manifest republishes older surviving observations.

## Corrections

A historical producer error is repaired by a reviewed correction specification, not by editing generated files or force-pushing `feed`:

```json
{
  "scope_id": "physical-ai-v1",
  "reason": "reviewed correction reason",
  "target_observation_id": "sha256:<64 hex>",
  "target_logical_key": "<exact existing logical key>",
  "replacement_observation_id": null
}
```

Use the manual Action inputs:

- `correction_spec_json`: the reviewed JSON object.
- `full_projection_correction=true`: authorizes a cascading full projection correction and bypasses only the ordinary seven-partition change cap.

The producer verifies that the target exists and matches its logical key, generates a stable `producer_retract`, rechains downstream supersedes links, validates the entire staged snapshot and publishes without rewriting Git history. The local consumer must separately review and recover across a known bad range.

## GitHub Actions trust boundary

`.github/workflows/arxiv-fact-feed.yml` runs at `04:30 UTC` and can also be dispatched manually.

1. The `collect` job has `contents: read`, runs the full tests, fetches the current `feed` head, collects facts and validates a staged tree.
2. The `publish` job alone has `contents: write`. It verifies the selected parent has not changed, copies only allowlisted paths into an isolated worktree and pushes without force.
3. It fetches the resulting remote commit and validates the published tree again.

Pinning third-party Actions by commit prevents floating-tag substitution. Workflow artifacts expire after two days and are not an authoritative Feed.

Branch protection is an external repository setting: protect `main` and `feed` from force-push/deletion and require producer tests on `main`. The first published Feed commit must be manually reviewed before its commit and exact-byte `feed.json` SHA-256 become the local consumer's bootstrap anchor.

## Commands

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q arxiv_feed tests
python3 -m arxiv_feed.cli validate --feed-dir /path/to/feed-branch-worktree
```

For a local collection invocation and operational checklist, see the root [README](../README.md). The authoritative dual-repository design is `physical-ai-research-radar/docs/system-design-remote-acquisition-local-analysis.md`.

## Repository isolation

This repository begins with independent Git history. The legacy `robotics_arXiv_daily` repository is neither a parent nor a fallback publication target; only commits reachable from this repository's reviewed bootstrap may enter the local trust chain.
