# Fairlead Documentation

This directory is organized by reader intent. Start with the shortest document
that answers the question you have, then follow the links only when you need more
detail.

## First Read

- [`../README.md`](../README.md) — project overview, current capabilities, local
  run commands, and the documentation index.
- [`planning/architecture.md`](planning/architecture.md) — how Fairlead fits
  with Rhizome, vLLM, GPU nodes, resource reporting, priority admission, and
  worker-pull async jobs.
- [`implementation/code_walkthrough.md`](implementation/code_walkthrough.md) —
  end-to-end Rust walkthrough from process startup to proxied response.

## Planning And Design

- [`planning/design.md`](planning/design.md) — design horizon and longer-term
  product shape.
- [`planning/glossary.md`](planning/glossary.md) — shared terminology for
  backends, providers, workers, workloads, routes, affinity, pools, and leases.
- [`planning/workloads.md`](planning/workloads.md) — current synchronous and
  async workload shapes.
- [`planning/roadmap.md`](planning/roadmap.md) — completed phases, active Phase
  8 scheduler-hardening scope, future phases, acceptance criteria, and deferred
  work.

## Implementation And Examples

- [`../demo/README.md`](../demo/README.md) — local GPU-free routing and async job
  demos, including the shared strict pool policy used by both runners. The
  executable demos remain under the repo-level `demo/` directory.
- [`implementation/dgx_spark_deployment.md`](implementation/dgx_spark_deployment.md)
  — manual two-node DGX Spark deployment notes using vLLM and Fairlead.
- [`implementation/fixture_examples.md`](implementation/fixture_examples.md) —
  sanitized fixture and local config conventions.

## Current Work

- [`current_work/phase_8a_bowline.md`](current_work/phase_8a_bowline.md) —
  completed Phase 8A worker lifecycle and graceful drain notes.
- [`current_work/phase_7d_navigator.md`](current_work/phase_7d_navigator.md) —
  completed Phase 7D shared pool demo and policy closeout notes.
- [`current_work/phase_7c_tactician.md`](current_work/phase_7c_tactician.md) —
  completed Phase 7C async worker pool placement notes.
- [`current_work/phase_7b_trimmer.md`](current_work/phase_7b_trimmer.md) —
  completed Phase 7B synchronous backend pool routing notes.
- [`current_work/phase_7a_helm.md`](current_work/phase_7a_helm.md) — completed
  Phase 7A pool model and validation notes.
- [`current_work/phase_6f_stay.md`](current_work/phase_6f_stay.md) — completed
  Phase 6F callback delivery and finalization notes.
- [`current_work/phase_6e_shackle.md`](current_work/phase_6e_shackle.md) —
  completed Phase 6E durable job state and recovery notes.
- [`current_work/phase_6d_halyard.md`](current_work/phase_6d_halyard.md) —
  completed Phase 6D worker execution, retries, and utilization notes.
- [`current_work/phase_6c_cleat.md`](current_work/phase_6c_cleat.md) — completed
  Phase 6C worker-pull claims and leases notes.
- [`current_work/phase_6b_tackle.md`](current_work/phase_6b_tackle.md) — completed
  Phase 6B scope and progress notes.
- [`current_work/phase_6a_clew.md`](current_work/phase_6a_clew.md) — completed
  Phase 6A scope and progress notes.
- [`current_work/deferred_tests.md`](current_work/deferred_tests.md) — useful
  tests intentionally deferred until there is a CI-friendly integration harness.
- Current phase notes should live in `current_work/` while a phase is active.

## Maintenance Rule

Keep current behavior in `README.md`, `planning/architecture.md`, and
`implementation/code_walkthrough.md`. Keep future product direction in
`planning/design.md` and `planning/roadmap.md`; keep current phase notes and
deferred work in `current_work/`. If a feature graduates from planned to
implemented, update both the implementation docs and the roadmap in the same
change.
