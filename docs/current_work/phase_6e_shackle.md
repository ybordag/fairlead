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

## Second Slice

Implemented:

- Wired `JOB_STORE=sqlite` startup to create `JobRegistry` from the SQLite
  store instead of only bootstrapping the schema.
- Added snapshot loading so Fairlead restores job records when the process
  restarts.
- Persisted accepted registry transitions to SQLite: submit, claim, lease
  renewal, lease expiry sweep, complete, fail/requeue, and cancel.
- Persisted queue ordering and list ordering separately so queue priority/FIFO
  behavior and `/v1/jobs` submission order both survive restart.
- Preserved attempts, max attempts, lease metadata, callback metadata, payload,
  result/error state, timestamps, and terminal states.
- Added idempotent schema handling for databases created by the first Shackle
  slice before the `order_position` column existed.
- Added restart-style tests for queued job recovery, next ID recovery, running
  lease recovery, cancelled state, completed state, and schema migration.

Remaining Shackle work:

- Define the restart policy for already-expired running leases. Today they
  restore as running and are resolved by the next lease sweep.
- Add endpoint-level restart tests around SQLite-backed `AppState`.
- Add e2e restart tests with an actual Fairlead process and SQLite file.
