# physical-ai-arxiv-feed

A deterministic, machine-verifiable arXiv Fact Feed for the Physical AI research radar.

This repository has one production responsibility: enumerate arXiv changes and publish source facts. It does not decide whether a paper is relevant, does not use keywords or an LLM, and does not contain a personal research profile, Chinese summary, Saved state or private model credential.

## Architecture

```text
arXiv OAI-PMH (global change stream)
              |
              v
daily GitHub Action -> validate -> immutable `feed` branch
                                      |
                                      v
                  local physical-ai-research-radar
                  Admission -> Triage -> Brief/Deep Read
```

The producer projects the versioned `physical-ai-v1` scope:

```text
cs.RO  cs.AI  cs.CV  cs.LG  cs.CL
cs.MA  cs.HC  cs.GR  eess.SY  eess.SP
```

Raw `cs.SY` source metadata is retained and aliases to `eess.SY` only for scope intersection. Scope `keywords` must be an empty array.

## Repository branches

- `main`: producer code, pinned scope/config, strict JSON Schemas, fixtures, tests and GitHub workflow.
- `feed`: generated `feed.json`, scope index, immutable gzip objects, producer state and status. No hand-authored research output belongs here.

The workflow runs daily at `04:30 UTC`. Collection is read-only. A separate least-privilege publish job checks that the selected parent is unchanged, stages only `feed.json`, `scopes/` and `objects/`, pushes without force, then fetches and revalidates the remote commit.

## Local validation

Python 3.12 is used by GitHub Actions; the implementation itself has no third-party runtime dependency.

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q arxiv_feed tests
python3 -m arxiv_feed.cli validate --feed-dir /path/to/feed-worktree
```

Run a local collection against an extracted prior Feed:

```bash
python3 -m arxiv_feed.cli collect \
  --input-feed /path/to/prior-feed \
  --output-feed /tmp/staged-feed \
  --result-file /tmp/collect-result.json \
  --base-feed-commit <commit-or-none> \
  --code-sha <producer-code-sha> \
  --workflow-run-id local \
  --workflow-run-url local
```

## Optional Search enrichment

OAI-PMH is authoritative for discovery, version/category/deletion state and coverage. The arXiv Search API may optionally enrich title, abstract, structured authors and primary category. It is disabled by default in `config/feed-producer.json` until measured against real rate and capacity limits.

Enrichment is retryable, bounded and non-blocking: Search failure cannot drop or delay the OAI observation. A later successful enrichment emits a new superseding observation. Persistent task shards expose backlog count and oldest age in `status.json`; repeated failures move to a weekly deferred cadence after seven attempts.

## Corrections

Ordinary collection is append-only at the logical Feed level. A bad historical observation is repaired with a reviewed `producer_retract` specification and a full projection correction; no force-push or history rewrite is allowed.

The manual Action input `correction_spec_json` accepts the reviewed JSON object, and `full_projection_correction` explicitly authorizes the otherwise capped cascading rewrite. The producer validates target identity, logical key and replacement before publishing.

## Required GitHub settings

Repository files cannot enforce branch settings. Before commissioning:

1. Protect `main` and `feed` from force-push and deletion.
2. Require producer tests for changes to `main`.
3. Keep default workflow permissions read-only; grant `contents: write` only to the publish job as declared in the workflow.
4. Run the first workflow manually, inspect the generated Feed, then pin that exact commit and exact-byte `feed.json` SHA-256 in the local consumer.

See [Feed v1](./docs/FEED.md) for the contract and operations details and [ADR-0001](./docs/adr/0001-arxiv-fact-feed.md) for the decision record. This is a greenfield repository with independent Git history; the legacy `robotics_arXiv_daily` repository is not part of its trust or publication chain.
