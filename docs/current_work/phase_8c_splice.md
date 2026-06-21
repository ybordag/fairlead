# Phase 8C Splice: Idempotency

Goal: make async scheduler operations safer under ordinary client, worker, and
callback retries without changing workload protocols.

## Implemented

- Added optional `idempotency_key` to `POST /v1/jobs`.
- Trimmed and validated idempotency keys.
- Reused the existing job when the same key is submitted with the same request
  shape.
- Rejected reuse of the same key for a different job request.
- Stored the key on `JobRecord`.
- Added an in-memory key-to-job map in `JobRegistryInner`.
- Persisted submit idempotency keys in SQLite-backed job snapshots.
- Rebuilt the in-memory idempotency map from SQLite on startup.
- Removed idempotency-key mappings when terminal jobs are pruned, allowing keys
  to be reused once the retained job record is gone.
- Made repeated `DELETE /v1/jobs/{id}` calls idempotent when the job is already
  `cancelled`.
- Kept cancellation of `complete` or `failed` jobs as `409 Conflict`, because
  those jobs were not cancelled by the caller's earlier cancellation request.

## Tests Added

- Duplicate `POST /v1/jobs` requests with the same idempotency key return the
  same job and do not enqueue a second job.
- Reusing an idempotency key for a different payload is rejected.
- SQLite-backed registries preserve submit idempotency across restart.
- Terminal-job pruning releases submit idempotency keys.
- SQLite schema migration adds the `idempotency_key` column to older job tables.
- Duplicate cancellation of an already-cancelled job returns `200 OK` with the
  existing job.
- Cancellation of a completed job still returns `409 Conflict`.

## Remaining 8C Scope

- Review whether worker completion/failure retries should return idempotent
  success for already-terminal jobs when Fairlead can prove the same worker and
  attempt produced the terminal state.
- Review callback delivery semantics around duplicate successful callback
  reports and recovery loops. Current delivery is at least once; receivers must
  remain idempotent by job ID.
- Add deferred process-level tests for submit idempotency across real Fairlead
  restarts and SQLite reuse after pruning.
