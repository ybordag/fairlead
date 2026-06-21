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
- `GET /v1/jobs` for listing in-memory job records.
- `GET /v1/jobs/{id}` for polling job status.
- `DELETE /v1/jobs/{id}` for cancellation.
- Job type, priority, payload, callback URL, attempt, and state metadata.
- In-memory job state first.
- Per-priority queue state plus queue depth and wait-time metrics.
- Worker registration, heartbeat, stale detection, and availability metrics.
- Durable-enough persistence later, with SQLite as the first likely backend.
- Durable queues, scheduler loop, worker deregistration, leases, retries,
  callbacks, and broader metrics in later slices.

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

## Second Slice

Implemented:

- Added explicit in-memory per-priority queues for queued job IDs.
- Enqueue submitted jobs by priority: `realtime`, `batch`, or `background`.
- Remove cancelled queued jobs from queue depth accounting.
- Added `GET /v1/jobs` to list in-memory job records in submission order.
- Added `fairlead_job_queue_depth{priority,type}` Prometheus metrics.
- Kept worker dispatch, durable queues, leases, and callbacks out of scope.

## Third Slice

Implemented:

- Added `WorkerRegistry` as in-memory shared state.
- Added `POST /v1/workers/register` for non-dispatching worker registration and
  upsert.
- Added `POST /v1/workers/{id}/heartbeat` to refresh worker liveness.
- Added `GET /v1/workers` to list registered workers.
- Store worker endpoint URL, optional node ID, supported job types, optional
  concurrency, and optional available VRAM metadata.
- Mark workers stale after the registry's heartbeat timeout.
- Added `fairlead_workers{type,status}` Prometheus metrics.
- Kept scheduling, dispatch, leases, deregistration, callbacks, and durable
  persistence out of scope.

## Fourth Slice

Implemented:

- Added current queued-job wait snapshots by priority and job type.
- Added `fairlead_job_queue_wait_seconds_sum{priority,type}` and
  `fairlead_job_queue_wait_seconds_max{priority,type}` Prometheus metrics.
- Use queue depth plus wait-time sum/max so dashboards can show queue depth,
  average wait time, and oldest queued job age.
- Keep cancelled jobs out of wait-time accounting, matching queue depth.
- Kept worker dispatch, durable queues, leases, and callbacks out of scope.

Next likely slice:

- Add a non-dispatching scheduler claim primitive that selects queued jobs
  without calling workers yet, or stop Tackle here and defer scheduler behavior
  to a later Phase 6B branch.
