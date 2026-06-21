# Phase 7D: Navigator

Branch: `navigator`

Goal: close out the shared pool model with demos, deployment notes, and policy
decisions.

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

## Pooling Test Audit

Immediate test coverage now checks the pooling cases that can be exercised
without a process harness:

- Config parsing keeps strict worker pool validation off by default.
- `STRICT_WORKER_POOLS=true` parses case-insensitively.
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
