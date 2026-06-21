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

## Next Slices

- Add a callback receiver process or in-test HTTP server for callback e2e
  cases.
- Add helpers for worker renew/fail/drain/reactivate/deregister and prune
  endpoints.
- Move deferred Phase 8 local-process cases into concrete tests incrementally.
