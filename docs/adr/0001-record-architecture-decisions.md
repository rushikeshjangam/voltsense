# ADR 0001 — Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-06-25

## Context

VoltSense spans streaming, AI infra, and IaC. Several non-obvious technology choices
(Flink vs Kafka Streams, Qdrant vs pgvector, vLLM vs a hosted API, KRaft vs ZooKeeper)
need to be explained to a reviewer. Capturing the *why* as we go is a senior signal and
keeps future-me honest.

## Decision

We keep short Architecture Decision Records in `docs/adr/`, one file per decision,
numbered sequentially (`NNNN-title.md`). Each records context, the decision, and
consequences. We use the lightweight format popularised by Michael Nygard.

## Consequences

- A reviewer can trace *why* each tool is in the stack, not just *that* it is.
- Low overhead — a few paragraphs per decision.

## Decisions to record as the build proceeds (placeholders)

- `0002` — Kafka in **KRaft** mode (no ZooKeeper) for the local stack.
- `0003` — **Flink** over Kafka Streams for windowed feature extraction.
- `0004` — **Qdrant** over pgvector for the vector store.
- `0005` — **vLLM** self-hosted over a hosted LLM API.
