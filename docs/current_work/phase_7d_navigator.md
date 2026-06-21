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
