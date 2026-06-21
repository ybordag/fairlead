# Phase 6D Halyard

## Goal

Let workers report the outcome of leased jobs and make execution behavior
observable. Halyard builds on Cleat's claims and leases: the worker that holds a
lease can now complete the job, report retryable failure, or report terminal
failure.

## Scope

Halyard includes:

- Worker result contract for completion and failure.
- `POST /v1/workers/{worker_id}/jobs/{job_id}/complete`.
- `POST /v1/workers/{worker_id}/jobs/{job_id}/fail`.
- Lease ownership checks for completion and failure.
- Retryable failure requeue while attempts remain.
- Terminal failure when an error is non-retryable or attempts are exhausted.
- Worker in-flight and capacity accounting.
- Worker utilization metrics.
- Job duration metrics.
- Per-attempt timeout behavior.

Halyard does not include:

- Durable job persistence.
- Callback delivery.
- Worker lifecycle controls, which were deferred to Phase 8A.
- Background lease expiry loop beyond the claim/renew/result-time sweeps.

## First Slice

Implemented:

- Added result and error fields to in-memory job records.
- Added `CompleteJobRequest` and `FailJobRequest`.
- Added `JobRegistry::complete_lease()`.
- Added `JobRegistry::fail_lease()`.
- Added `POST /v1/workers/{worker_id}/jobs/{job_id}/complete`.
- Added `POST /v1/workers/{worker_id}/jobs/{job_id}/fail`.
- Completion validates worker freshness and lease ownership, marks the job
  `complete`, stores the result payload, clears error state, and removes the
  lease.
- Failure validates worker freshness and lease ownership, stores an error
  message plus retryable flag, and removes the lease.
- Retryable failures requeue the job when attempts remain.
- Non-retryable failures and retry exhaustion mark the job `failed`.
- Added registry and endpoint tests for success, wrong lease holder, retryable
  failure requeue, non-retryable failure, retry exhaustion, and invalid failure
  payloads.

## Second Slice

Implemented:

- Added worker in-flight accounting to the worker registry.
- Preserved in-flight counts across worker registration upserts.
- Added worker slot acquisition and release helpers around
  `max_concurrent_jobs`.
- Extended worker snapshots with `in_flight_jobs` and
  `available_job_slots`.
- Made `POST /v1/workers/{id}/claim` acquire worker capacity before leasing a
  job.
- Return `409` when a fresh worker is already at capacity.
- Release worker capacity when a leased job completes, fails, is cancelled, or
  is swept as expired.
- Added worker utilization metrics:
  `fairlead_worker_in_flight_jobs`,
  `fairlead_worker_max_concurrent_jobs`, and
  `fairlead_worker_available_job_slots`.
- Added tests for capacity acquisition/release, registration upserts,
  claim-time capacity rejection, result-time release, cancellation-time release,
  expiry-sweep release, and metric output.

## Third Slice

Implemented:

- Added terminal job duration snapshots to `JobRegistry`.
- Duration is measured from job submission to terminal state using
  `created_at_unix_ms` and `updated_at_unix_ms`.
- Aggregates are grouped by priority, job type, and terminal status.
- Added `/metrics` output for `fairlead_job_duration_seconds_count`,
  `fairlead_job_duration_seconds_sum`, and
  `fairlead_job_duration_seconds_max`.
- Duration metrics include only terminal jobs: `complete`, `failed`, and
  `cancelled`.
- Added registry and metrics tests for terminal duration accounting.

## Fourth Slice

Implemented:

- Made lease expiry explicitly record per-attempt timeout state.
- Expired attempts now store `attempt timed out` in the job error field.
- Timed-out attempts are marked retryable while attempts remain.
- Timed-out attempts are marked non-retryable when attempts are exhausted and
  the job becomes `failed`.
- Claim, renewal, completion, and failure endpoints continue to sweep expired
  leases before mutating job state, so late worker reports cannot resurrect an
  expired attempt.
- Added registry and endpoint assertions for timeout error state.

Remaining Halyard work:

- None. Durable persistence, callback delivery, configurable per-workload
  timeout policy, and background expiry loops remain future phases.

## Coverage Sweep

Implemented after the final feature slice:

- Added endpoint tests for unknown and stale workers reporting completion.
- Added endpoint tests for unknown and stale workers reporting failure.
- Added duplicate result-report coverage: a second completion or late failure
  after terminal completion returns `409` and does not change the terminal job
  result or worker capacity accounting.

Deferred:

- Multi-process e2e with fake workers and Fairlead running as a real server.
- Opt-in DGX Spark e2e with fake async workers on both connected nodes.
- Concurrency stress around many workers claiming jobs against limited worker
  capacity.
- Configurable per-workload timeout policy, if that policy is added later.
