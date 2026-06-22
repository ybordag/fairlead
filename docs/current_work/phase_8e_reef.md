# Phase 8E Reef: Process-Level E2E Harness

## Goal

Add a reusable process-level test harness that can start a real Fairlead
process, drive HTTP calls against it, and verify restart-sensitive scheduler
behavior that in-process Rust tests cannot cover.

## Status

Complete for the scoped Reef branch. Remaining heavier process, crash-injection,
concurrency, and remote deployment cases are intentionally deferred to later
harness/deployment hardening phases and tracked in
`docs/current_work/deferred_tests.md`.

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
- Added `invalid_scheduler_env_exits_before_serving_health`, which verifies the
  real process exits nonzero for invalid scheduler/retention configuration
  before serving health.
- Added `invalid_callback_env_exits_before_serving_health`, which verifies the
  real process exits nonzero for invalid callback retry/timeout configuration
  before serving health.
- Added JSON request helpers for process-level `GET` and `POST` calls.
- Added restart support that stops and starts Fairlead again with the same
  port, temp directory, and environment.
- Added `sqlite_job_state_survives_process_restart`, which starts Fairlead with
  SQLite job storage, submits a job over HTTP, restarts the process, fetches the
  same job over HTTP, and verifies submit idempotency still returns the original
  job.
- Added `sqlite_idempotency_keys_survive_restart_and_release_after_prune`,
  which verifies invalid idempotency keys are rejected, matching submits reuse
  the same job before and after restart, conflicting reuse is rejected without
  queue mutation, retained terminal jobs are reused, and pruning releases the
  key for a new job.
- Added `sqlite_cancelled_job_stays_idempotent_after_process_restart`, which
  verifies a queued job cancelled with a callback stays cancelled after process
  restart, duplicate cancellation remains idempotent, submit idempotency still
  returns the cancelled job, and no duplicate callback is delivered.
- Added `sqlite_terminal_worker_results_stay_idempotent_after_process_restart`,
  which verifies exact duplicate completion and failure reports remain
  idempotent after SQLite restart and worker re-registration, contradictory
  reports remain conflicts, and duplicate completion replay does not redeliver
  a callback.
- Added `worker_result_endpoints_reject_mismatched_attempts_over_real_http`,
  which verifies real HTTP complete/fail endpoints reject mismatched attempt
  numbers without moving the job out of `running`, then accept the correct
  attempt.
- Added harness helpers for worker registration, worker claim, worker
  completion, and text responses such as `/metrics`.
- Added `worker_can_claim_and_complete_job_over_http`, which exercises the real
  process HTTP flow for submit -> register worker -> claim -> complete -> fetch
  final job -> scrape metrics.
- Added `metrics_stay_consistent_across_process_scheduler_workflow`, which
  verifies `/metrics` remains internally consistent across queued work, worker
  capacity use, background lease recovery, completion, callback delivery, and
  manual pruning.
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
- Added `background_maintenance_fails_exhausted_expired_lease_and_delivers_callback`,
  which verifies the background maintenance loop fails an expired lease once the
  job exhausts its attempts, releases worker capacity, dispatches the terminal
  failure callback, and leaves no reclaimable work.
- Added `exhausted_expired_lease_dispatches_callback_after_process_restart`,
  which uses SQLite storage to verify an exhausted expired lease is failed
  during Fairlead startup recovery and its terminal failure callback is
  delivered after restart.
- Added harness helpers for worker drain, reactivate, deregister, and DELETE
  requests.
- Added `worker_lifecycle_controls_work_over_real_http`, which verifies
  drain/reactivate/deregister behavior through the real process API, heartbeat
  and re-registration preserving draining state until explicit reactivation,
  and busy deregistration leaving a draining worker able to complete its held
  job.
- Added harness helpers for worker lease renewal and worker-reported failure.
- Added `worker_renew_and_retryable_fail_requeues_over_real_http`, which
  verifies a draining worker holding a lease can still renew it, a retryable
  worker failure requeues the job while keeping that worker draining, and
  another worker can reclaim and complete the job.
- Added a process harness helper for `POST /v1/jobs/prune`.
- Added `prune_endpoint_removes_only_eligible_terminal_jobs_over_real_http`,
  which verifies manual pruning removes eligible terminal jobs and delivered
  callback jobs while preserving pending-callback, running, and queued jobs.
- Added `background_pruning_removes_only_eligible_terminal_jobs_over_real_http`,
  which enables SQLite-backed background pruning and verifies the maintenance
  loop removes only eligible terminal jobs while preserving pending-callback,
  running, and queued jobs. The test also verifies prune metrics and that a
  manual prune after the background sweep reports no remaining eligible jobs.
- Added `background_pruning_respects_limit_and_progresses_across_intervals`,
  which verifies background pruning honors `JOB_PRUNE_LIMIT=1` for a single
  sweep, leaves later eligible terminal jobs in place after the first removal,
  and continues pruning them across later intervals.
- Added `omitted_background_prune_interval_keeps_manual_pruning_enabled`, which
  verifies omitting `JOB_PRUNE_INTERVAL_SECS` prevents background pruning while
  still allowing explicit `POST /v1/jobs/prune` to remove eligible terminal
  jobs and record prune metrics.

## Deferred Beyond Reef

- Concurrent manual/background prune races and double-count protection.
- Crash-after-commit terminal result simulation for worker result idempotency.
- Fake worker processes that poll, claim, renew, complete, fail, and drain in
  loops.
- SQLite shutdown/corruption stress and interrupted-write recovery.
- DGX Spark deployment e2e on the two-node setup.
- Callback receiver crash-injection after receiver-side success but before
  Fairlead records delivery.
