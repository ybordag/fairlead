# Phase 7B: Trimmer

Branch: `trimmer`

Goal: apply the validated Phase 7 pool policy to synchronous backend routing.

## Completed Slice

- Added `WorkloadPoolPolicy` as the runtime helper for workload-to-pool
  eligibility.
- Stored the workload pool policy in shared `AppState`.
- Updated synchronous route eligibility so chat and embeddings only consider a
  backend when:
  - the backend supports the requested workload
  - the backend's static route policy allows its pool
  - the workload pool policy allows its pool
- Treat each workload's pool list as an ordered synchronous fallback chain.
- Preserve origin locality, affinity, resource ranking, circuit state, and
  backend order within each pool stage.
- Added per-pool synchronous decision metrics for selected/unavailable pool
  stages, candidate backend counts, and resource-ineligible backend counts.
- Preserved permissive behavior for workloads omitted from an explicit partial
  policy.
- Scoped the strict-policy decision to Phase 7D, after async worker placement
  and shared demos use the same pool vocabulary.
- Added proxy tests for:
  - skipping a backend outside the workload's allowed pools
  - returning `503` without touching upstreams when no backend is pool-eligible
  - keeping omitted workload policy permissive
  - preferring an earlier pool even when a later pool appears first in backend
    order
  - falling back to a later pool when every backend in an earlier pool is open
  - emitting per-pool selected/unavailable metrics
  - emitting per-pool resource pressure metrics
  - preserving pool-order precedence over cross-pool origin locality

## Test Audit

Immediate 7B coverage includes:

- workload pool allowlist eligibility
- ordered pool fallback independent of backend order
- fallback to a later pool when the earlier pool's backends are circuit-open
- pool-order precedence over cross-pool origin locality
- permissive behavior for workloads omitted from explicit partial policy
- `503` without upstream calls when no backend is pool-eligible
- per-pool selected/unavailable metrics
- per-pool candidate count metrics
- per-pool resource pressure metrics

Deferred e2e coverage is listed in
[`deferred_tests.md`](deferred_tests.md):

- local process-level sync pool routing e2e
- DGX Spark sync pool routing smoke test

## Documentation Audit

- Updated the README pool-policy section to describe Phase 7B synchronous pool
  routing and the remaining Phase 7C/7D pool work.
- Updated the architecture notes so current routing includes ordered
  synchronous backend-pool selection, while async worker pool placement remains
  future work.
- Updated the roadmap baseline and moderate task list so completed synchronous
  backend pool routing is not described as missing.
- Updated the code walkthrough so "not happening yet" points at async worker
  pool placement rather than synchronous pool routing.

## Deferrals

- Async worker pool metadata and worker placement belong to `tactician` /
  Phase 7C.
- Shared pool demos and final deployment examples belong to `navigator` /
  Phase 7D.
