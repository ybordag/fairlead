# Phase 8C Splice: Idempotency

Goal: make async scheduler operations safer under ordinary client, worker, and
callback retries without changing workload protocols.

## Implemented

- Added optional `idempotency_key` to `POST /v1/jobs`.
- Trimmed and validated idempotency keys.
- Rejected blank and overlong idempotency keys.
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
- Added optional worker-reported `attempt` to complete/fail requests.
- Stored terminal attempt metadata for jobs completed or terminally failed by a
  worker.
- Made exact duplicate terminal complete/fail reports idempotent when worker ID,
  attempt number, and result/error payload match.
- Kept contradictory terminal result reports as `409 Conflict`.
- Reviewed callback idempotency. Fairlead already deduplicates in-flight
  callback dispatches by job ID and skips delivered callbacks, while preserving
  the documented at-least-once restart contract.

## Tests Added

- Duplicate `POST /v1/jobs` requests with the same idempotency key return the
  same job and do not enqueue a second job.
- Reusing an idempotency key for a different payload is rejected.
- Overlong idempotency keys are rejected without mutating job state.
- Retained terminal jobs are returned for matching submit idempotency retries.
- SQLite-backed registries preserve submit idempotency across restart.
- Terminal-job pruning releases submit idempotency keys.
- SQLite schema migration adds the `idempotency_key` column to older job tables.
- Duplicate cancellation of an already-cancelled job returns `200 OK` with the
  existing job.
- Cancellation of a completed job still returns `409 Conflict`.
- Exact duplicate terminal completion returns the existing completed job.
- Duplicate completion with a different result still returns `409 Conflict`.
- Exact duplicate terminal failure returns the existing failed job without
  releasing a later in-flight worker slot.
- Duplicate failure with a different error still returns `409 Conflict`.
- Terminal attempt metadata is persisted and recovered through SQLite.
- Running completion with a mismatched attempt number is rejected.
- Running failure with a mismatched attempt number is rejected.
- Terminal attempt metadata is persisted and recovered for both completed and
  failed jobs.
- Delivered callbacks are not returned by pending callback scans and cannot
  begin another delivery attempt.
- Phase 8E added process-level cancellation idempotency coverage across restart,
  including no duplicate callback delivery and retained submit idempotency.
- Phase 8E added process-level terminal completion and failure replay coverage
  across restart, including exact duplicate acceptance, contradictory replay
  conflict responses, and no duplicate completion callback delivery.
- Phase 8E added process-level mismatched attempt rejection coverage for both
  worker completion and failure endpoints.
- Deferred process-level e2e coverage now includes concurrent duplicate submit
  races, terminal-result replay races against new claims, and
  crash-after-terminal-result retry.

## Remaining Deferred 8C Scope

- Add heavier process-level terminal result idempotency races, concurrent
  duplicate submit races, and crash-after-terminal-result retry in a later
  crash/concurrency harness.
