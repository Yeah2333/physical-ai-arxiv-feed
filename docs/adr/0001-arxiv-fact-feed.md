# ADR-0001: Publish an arXiv Fact Feed

- Status: Accepted
- Date: 2026-07-14
- Producer: `Yeah2333/physical-ai-arxiv-feed`
- Consumer: `physical-ai-research-radar`
- Canonical design: `physical-ai-research-radar/docs/system-design-remote-acquisition-local-analysis.md`

## Context

The previous `robotics_arXiv_daily` repository contains keyword-driven output and unrelated history. Reusing that repository would mix legacy artifacts with the new producer's trust boundary and make its initial state harder to audit.

## Decision

Create this repository as a greenfield public arXiv fact producer with independent Git history:

- collect OAI-PMH `arXivRaw` facts for the versioned `physical-ai-v1` category scope;
- publish normalized, schema-validated, content-addressed JSONL through a machine-managed `feed` branch;
- keep source acquisition free of keywords, LLM calls, Chinese summaries and user-specific state;
- make canonical bytes, identity functions and contract vectors shared inputs for producer and consumer tests;
- keep `main` protected for code and contract files; only the publish job may write `feed`.

The old repository remains separate and is never an authoritative input, ancestor or publication target for this Feed.

## Consequences

The producer must fail closed on incomplete OAI pagination, schema drift, invalid projection or publish conflicts. Optional Search API enrichment may be partial but cannot suppress OAI facts. Git history and Feed hashes become part of the public data contract.
