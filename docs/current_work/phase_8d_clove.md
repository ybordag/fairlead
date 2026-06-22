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
- Added in-process coverage proving exhausted expired leases fail from the
  background loop and dispatch terminal callbacks.
- Added optional background terminal-job pruning with `JOB_PRUNE_INTERVAL_SECS`.
- Background pruning calls the same helper as `POST /v1/jobs/prune`, so
  retention age, per-run limits, pending-callback protection, SQLite
  persistence, submit idempotency-key release, and metrics stay consistent.
- Added in-process coverage proving background pruning removes eligible terminal
  jobs across bounded intervals, retains queued jobs, and records prune metrics.
- Added in-process coverage proving background pruning retains pending-callback
  terminal jobs until callback delivery succeeds.
- Audited Phase 8 coverage. Phase 8E later added process-level restart,
  invalid-startup, SQLite durability, callback receiver, lifecycle,
  idempotency, pruning, and metrics coverage. Remaining heavier concurrency,
  crash-injection, SQLite stress, and deployment smoke tests stay recorded in
  `deferred_tests.md`.

## Remaining In 8D

- None for in-process coverage. Phase 8E covered the deterministic
  process-level timing, restart, SQLite, callback receiver, pruning, and metrics
  cases. Concurrent manual/background pruning and richer metric/log behavior are
  deferred to later hardening harness work.
