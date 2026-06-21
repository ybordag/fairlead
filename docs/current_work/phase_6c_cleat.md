# Phase 6C Cleat

## Goal

Turn Phase 6B's scheduler preview into worker-pull claims with bounded leases.
Cleat is about temporarily securing a queued job to a worker, without adding
worker execution, callbacks, or durable persistence yet.

## Scope

Cleat includes:

- `POST /v1/workers/{id}/claim` for worker-pull claims.
- Worker validation before claims: missing workers return `404`, stale workers
  return `409`.
- Priority/FIFO job selection for compatible worker job types.
- Job transition from `queued` to `running` only when a lease is granted.
- Lease metadata on the job record: worker ID, attempt number, claimed-at time,
  and expiry time.
- Duplicate-claim prevention by removing claimed jobs from the queue.
- Opportunistic lease expiry handling before fresh worker claims.
- Initial cancellation behavior for queued and running jobs.

Cleat does not include:

- Worker execution or callbacks.
- Worker lease renewal or completion/failure endpoints.
- Durable job persistence.
- Background lease expiry scheduler loop.
- Complete worker utilization metrics.

## First Slice

Implemented:

- Added `JobLease` metadata to `JobRecord`.
- Added `JobRegistry::claim_next_for_worker()`.
- Added `POST /v1/workers/{id}/claim`.
- Mark claimed jobs `running`, increment attempts, attach lease metadata, and
  remove claimed jobs from queue-depth and wait-time accounting.
- Return `204 No Content` when a fresh worker has no compatible queued job.
- Return `404 Not Found` for unknown workers.
- Return `409 Conflict` for stale workers.
- Added tests for lease creation, priority/FIFO claim order, duplicate-claim
  prevention, stale worker exclusion, unsupported job types, and queued/running
  cancellation basics.

## Second Slice

Implemented:

- Added `JobRegistry::requeue_expired_leases()`.
- `POST /v1/workers/{id}/claim` now sweeps expired running leases before
  selecting work.
- Expired running jobs return to their priority queue when attempts remain.
- Expired running jobs become `failed` when attempts are exhausted.
- Requeued jobs clear old lease metadata before another worker can claim them.
- Added registry tests for requeue, retry exhaustion, and ignoring fresh or
  terminal jobs.
- Added endpoint coverage proving worker claims can reclaim an expired lease.

Remaining likely Cleat work:

- Decide whether to add explicit lease renewal in 6C or defer it to worker
  execution in 6D.
- Tighten cancellation-race tests around running jobs and future completion
  endpoints.
