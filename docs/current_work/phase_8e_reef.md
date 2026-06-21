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

## Next Slices

- Add SQLite-backed restart support to the harness.
- Add helpers for JSON HTTP requests against `/v1/jobs` and `/v1/workers`.
- Add a callback receiver process or in-test HTTP server for callback e2e
  cases.
- Move deferred Phase 8 local-process cases into concrete tests incrementally.
