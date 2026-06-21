# Phase 8E Reef: Process-Level E2E Harness

## Goal

Add a reusable process-level test harness that can start a real Fairlead
process, drive HTTP calls against it, and verify restart-sensitive scheduler
behavior that in-process Rust tests cannot cover.

## Implemented

- Created `tests/process_harness.rs`.
- Added `FairleadProcess`, a small harness helper that:
  - reserves an ephemeral localhost port
  - starts the compiled Fairlead binary with isolated environment variables
  - writes stdout and stderr to temporary log files
  - polls `/health` until the process is ready
  - kills and waits for the process during cleanup
- Added the first process-level smoke test:
  `fairlead_process_starts_serves_health_and_shuts_down`.
- Added JSON request helpers for process-level `GET` and `POST` calls.
- Added restart support that stops and starts Fairlead again with the same
  port, temp directory, and environment.
- Added `sqlite_job_state_survives_process_restart`, which starts Fairlead with
  SQLite job storage, submits a job over HTTP, restarts the process, fetches the
  same job over HTTP, and verifies submit idempotency still returns the original
  job.
- Added harness helpers for worker registration, worker claim, worker
  completion, and text responses such as `/metrics`.
- Added `worker_can_claim_and_complete_job_over_http`, which exercises the real
  process HTTP flow for submit -> register worker -> claim -> complete -> fetch
  final job -> scrape metrics.
- Added an in-test callback receiver and
  `complete_job_delivers_callback_over_real_http`, which exercises submit with
  `callback_url` -> worker claim -> completion -> callback delivery -> callback
  state polling -> callback metrics through real HTTP boundaries.
- Added `pending_callback_retries_after_process_restart`, which uses SQLite
  storage and a sequence callback receiver to verify a failed terminal callback
  remains pending across a Fairlead process restart and is redelivered by the
  recovery loop.
- Added `expired_lease_requeues_after_process_restart`, which uses SQLite
  storage to verify an expired running lease is recovered on Fairlead restart,
  requeued with an attempt-timeout error, and claimable by a newly registered
  replacement worker.
- Added `JOB_LEASE_DURATION_MS`, defaulting to the previous 30-second lease
  behavior, so process tests and local demos can use short leases without
  changing production defaults.
- Added `background_maintenance_requeues_expired_lease`, which verifies a live
  Fairlead process requeues an expired lease from the background maintenance
  loop without relying on another worker claim to trigger recovery.
- Added harness helpers for worker drain, reactivate, deregister, and DELETE
  requests.
- Added `worker_lifecycle_controls_work_over_real_http`, which verifies
  drain/reactivate/deregister behavior through the real process API, including
  busy deregistration leaving a draining worker able to complete its held job.
- Added harness helpers for worker lease renewal and worker-reported failure.
- Added `worker_renew_and_retryable_fail_requeues_over_real_http`, which
  verifies renewal extends a running lease and retryable worker failure requeues
  the job for another worker to reclaim and complete.
- Added a process harness helper for `POST /v1/jobs/prune`.
- Added `prune_endpoint_removes_only_eligible_terminal_jobs_over_real_http`,
  which verifies manual pruning removes eligible terminal jobs and delivered
  callback jobs while preserving pending-callback, running, and queued jobs.

## Next Slices

- Move deferred Phase 8 local-process cases into concrete tests incrementally.
