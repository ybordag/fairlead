# Fairlead Documentation

This directory is organized by reader intent. Start with the shortest document
that answers the question you have, then follow the links only when you need more
detail.

## First Read

- [`../README.md`](../README.md) — project overview, current capabilities, local
  run commands, and the documentation index.
- [`architecture.md`](architecture.md) — how Fairlead fits with Rhizome, vLLM,
  GPU nodes, resource reporting, priority admission, and future async jobs.
- [`code_walkthrough.md`](code_walkthrough.md) — end-to-end Rust walkthrough from
  process startup to proxied response.

## Planning And Design

- [`design.md`](design.md) — design horizon and longer-term product shape.
- [`roadmap.md`](roadmap.md) — completed phases, current Trim scope, future
  phases, acceptance criteria, and deferred work.
- [`job_scheduler_and_temporal.md`](job_scheduler_and_temporal.md) — async
  scheduler boundary and why Temporal is deferred.

## Running And Demonstrating

- [`../demo/README.md`](../demo/README.md) — local GPU-free routing demo with two
  mock OpenAI-compatible backends.
- [`dgx_spark_deployment.md`](dgx_spark_deployment.md) — manual two-node DGX
  Spark deployment notes using vLLM and Fairlead.

## Development Hygiene

- [`fixture_examples.md`](fixture_examples.md) — sanitized fixture and local
  config conventions.
- [`deferred_tests.md`](deferred_tests.md) — useful tests intentionally deferred
  until there is a CI-friendly integration harness.

## Maintenance Rule

Keep current behavior in `README.md`, `architecture.md`, and
`code_walkthrough.md`. Keep future work in `design.md`, `roadmap.md`, and
`job_scheduler_and_temporal.md`. If a feature graduates from planned to
implemented, update both the implementation docs and the roadmap in the same
change.
