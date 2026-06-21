# Phase 6B Tackle

## Goal

Add Fairlead's async compute-router surface without turning it into a workflow
engine. Phase 6B introduces job submission, job state, queueing, worker
registration, leases, retries, callbacks, and metrics in slices.

The first slices should stay deliberately small: establish the API and state
model before dispatching work to external workers.

## Scope

Tackle includes:

- `POST /v1/jobs` for bounded async compute submission.
- `GET /v1/jobs/{id}` for polling job status.
- `DELETE /v1/jobs/{id}` for cancellation.
- Job type, priority, payload, callback URL, attempt, and state metadata.
- In-memory job state first.
- Durable-enough persistence later, with SQLite as the first likely backend.
- Priority queues, scheduler loop, worker registration, leases, retries,
  callbacks, and metrics in later slices.

Tackle does not include:

- Domain-specific Rhizome state transitions.
- Temporal or general workflow orchestration.
- Complete pool-aware placement. That is deferred to Phase 7A.
- Worker process supervision or restart policy.

## First Slice

Implemented:

- Added `JobRegistry` as in-memory shared state.
- Added job types: `vision_analysis`, `embed_batch`, `index_build`, and
  `cluster`.
- Added job states: `queued`, `running`, `complete`, `failed`, and `cancelled`.
- Added `POST /v1/jobs`, returning `202 Accepted` with a queued job record.
- Added `GET /v1/jobs/{id}`.
- Added `DELETE /v1/jobs/{id}` for cancelling queued jobs.
- Default missing job priority to `realtime`, matching the existing priority
  enum.
- Store payload and optional callback URL without dispatching to workers yet.

Next likely slice:

- Add list/queue visibility and queue metrics, or introduce explicit priority
  queues before worker registration.
