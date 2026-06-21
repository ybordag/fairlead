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
- Worker deregistration or graceful drain semantics.
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

Remaining likely Halyard work:

- Job duration metrics.
- Per-attempt timeout behavior.

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

Remaining likely Halyard work:

- Job duration metrics.
- Per-attempt timeout behavior.
