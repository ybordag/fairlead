# Phase 7D: Navigator

Branch: `navigator`

Goal: close out the shared pool model with demos, deployment notes, and policy
decisions.

## Completed So Far

- Added optional strict worker pool validation through `STRICT_WORKER_POOLS`.
- Added optional strict workload pool validation through
  `STRICT_WORKLOAD_POOLS`.
- Kept permissive defaults for local development and incremental rollout.
- Preserved the existing partial `WORKLOAD_POOLS_JSON` behavior unless strict
  workload validation is enabled.
- Added unit coverage for strict worker registration, defaulted worker pools,
  strict workload startup validation, complete workload policy, and derived
  backend pools.
- Updated deferred e2e plans for strict workload startup, strict worker
  registration, DGX Spark smoke tests, and future cloud overflow pools.

## Remaining

- Add local demo config that shows sync and async workloads using the same pool
  vocabulary.
- Document local DGX pools, peer-node pools, and shared Fairlead deployment
  examples using the finalized strictness flags.

## Decisions

### Worker Pool Registration Validation

Fairlead now supports optional strict worker pool validation.

- Default behavior remains permissive: workers can register any non-empty pool
  string.
- `STRICT_WORKER_POOLS=true` rejects worker registration when the worker's pool
  is not present in configured or derived `POOLS_JSON`.
- Strict mode lets production-like demos catch typos and keep metrics bounded to
  known pools.
- Permissive mode remains useful for local development, dynamic workers, and
  experiments where pools appear before config is finalized.

This keeps Phase 7D compatible with existing worker registration while making
the central control-plane behavior available for shared demos.

### Workload Pool Policy Strictness

Fairlead now supports optional strict workload pool validation.

- Default behavior remains a partial override: explicit `WORKLOAD_POOLS_JSON`
  can mention only the workloads that need placement constraints, while omitted
  workloads remain eligible for all configured pools.
- `STRICT_WORKLOAD_POOLS=true` requires `WORKLOAD_POOLS_JSON` to be present and
  to include every known workload.
- Strict mode is the better fit for production-like demos and deployments
  because accidental omissions fail at startup instead of silently widening
  placement.
- Partial mode remains useful for local development and incremental workload
  rollout.

This mirrors strict worker pool validation: permissive defaults keep the simple
setup easy, while strict flags make the central control-plane policy explicit
when Fairlead is acting as shared infrastructure.

## Pooling Test Audit

Immediate test coverage now checks the pooling cases that can be exercised
without a process harness:

- Config parsing keeps strict worker pool validation off by default.
- `STRICT_WORKER_POOLS=true` parses case-insensitively.
- Config parsing keeps strict workload pool validation off by default.
- `STRICT_WORKLOAD_POOLS=true` parses case-insensitively.
- Strict workload pool validation rejects absent `WORKLOAD_POOLS_JSON`.
- Strict workload pool validation rejects partial `WORKLOAD_POOLS_JSON` and
  reports missing workloads.
- Strict workload pool validation accepts complete policy for every known
  workload.
- Strict workload pool validation accepts complete policy that references pools
  derived from `BACKENDS_JSON`, preserving the Phase 7A derived-pool path.
- Strict mode does not change the default derived pool set when no `POOLS_JSON`
  is provided.
- Worker registration remains permissive by default for ad hoc pool names.
- Strict worker registration rejects unknown pools and does not insert rejected
  workers into the registry.
- Strict worker registration accepts configured pools, including request values
  with surrounding whitespace.
- Workers that omit `pool` still default to `default`.
- In strict mode, omitted/default worker pools are accepted only when `default`
  is present in configured or derived pools.
- Existing Phase 7B/7C tests cover sync backend pool routing, workload pool
  ordering, resource/circuit fallback interactions, async worker pool matching,
  priority queue behavior across compatible and incompatible pools, and per-pool
  metrics.

Deferred e2e coverage is tracked in `docs/current_work/deferred_tests.md` for
process startup, strict worker pool registration, local mock placement, DGX
Spark placement, and future cloud overflow pools.
