# Phase 8D Clove: Background Maintenance Loops

## Goal

Move scheduler cleanup work that currently depends on opportunistic request
paths into explicit background maintenance loops, without changing the worker
execution protocol.

## Implemented

- Added `JOB_MAINTENANCE_INTERVAL_SECS`, defaulting to 30 seconds.
- Spawned a background lease recovery loop from `main()`.
- The loop calls the same `sweep_expired_leases()` helper used by worker claim,
  renew, complete, and fail handlers.
- Expired leases still release the previous worker's in-flight slot, record
  `attempt timed out`, requeue retryable jobs with attempts remaining, and fail
  exhausted jobs with callback dispatch.
- Added in-process coverage proving the loop requeues an expired lease and
  releases worker capacity without waiting for another worker claim.
- Added optional background terminal-job pruning with `JOB_PRUNE_INTERVAL_SECS`.
- Background pruning calls the same helper as `POST /v1/jobs/prune`, so
  retention age, per-run limits, pending-callback protection, SQLite
  persistence, submit idempotency-key release, and metrics stay consistent.
- Added in-process coverage proving background pruning removes eligible terminal
  jobs across bounded intervals, retains queued jobs, and records prune metrics.

## Remaining In 8D

- Add or defer tests for process-level timing, restart, SQLite, callback, and
  metric/log behavior.
