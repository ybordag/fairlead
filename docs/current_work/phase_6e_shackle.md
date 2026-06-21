# Phase 6E Shackle

## Goal

Make async job state survive ordinary Fairlead restarts without making Fairlead
the application source of truth. Shackle starts with SQLite-backed durable state
for the job scheduler while keeping the current in-memory behavior as the
default until the persistence path is fully wired and tested.

## Scope

Shackle includes:

- A configurable job store backend.
- `JOB_STORE=memory` as the default during the implementation phase.
- `JOB_STORE=sqlite` as an opt-in durable backend.
- `JOB_DB_PATH` for the SQLite database path.
- SQLite schema bootstrap and migration/version tracking.
- Persistence for job records, queue order, attempts, leases, result/error
  state, timestamps, and callback metadata.
- Restart/recovery behavior for queued and running jobs.
- Tests for persistence round trips and restart recovery.

Shackle does not include:

- Callback delivery.
- Multi-instance Fairlead coordination.
- Postgres support.
- Complete worker deregistration or graceful drain semantics.

## First Slice

Implemented:

- Added `JobStoreConfig` with `memory` and `sqlite` modes.
- Added `JOB_STORE` config parsing. Default remains `memory`.
- Added `JOB_DB_PATH` for SQLite, defaulting to `fairlead_jobs.sqlite3`.
- Added startup bootstrap for the configured job store.
- Added a `storage` module with SQLite schema creation.
- Added a first schema version through `PRAGMA user_version = 1`.
- Added a `jobs` table shaped for the current in-memory `JobRecord`:
  identifiers, type, priority, status, payload, callback URL, result, error,
  attempts, max attempts, lease, timestamps, and queue position.
- Added queue/status indexes for future recovery and claim queries.
- Added config and storage bootstrap tests.

Remaining Shackle work:

- Wire `JobRegistry` transitions through the store.
- Persist queue ordering.
- Recover queued jobs after registry restart.
- Resolve running jobs after restart.
- Add restart/recovery tests around real job transitions.
