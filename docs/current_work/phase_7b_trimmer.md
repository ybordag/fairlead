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
- Preserved permissive behavior for workloads omitted from an explicit partial
  policy.
- Added proxy tests for:
  - skipping a backend outside the workload's allowed pools
  - returning `503` without touching upstreams when no backend is pool-eligible
  - keeping omitted workload policy permissive
  - preferring an earlier pool even when a later pool appears first in backend
    order
  - falling back to a later pool when every backend in an earlier pool is open

## Remaining 7B Work

- Decide whether `WORKLOAD_POOLS_JSON` should remain a partial override or become
  strict before Phase 7 ends.
- Add per-pool synchronous metrics for candidate counts, selected pool/backend,
  fallback reason, and capacity pressure.
- Audit docs and tests before opening the `trimmer` PR.

## Deferrals

- Async worker pool metadata and worker placement belong to `tactician` /
  Phase 7C.
- Shared pool demos and final deployment examples belong to `navigator` /
  Phase 7D.
