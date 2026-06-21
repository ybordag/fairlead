# Phase 7C: Tactician

Branch: `tactician`

Goal: apply the shared Phase 7 pool policy to async worker placement.

## Completed Slice

- Added `pool` metadata to worker registration and worker snapshots.
- Preserved backward compatibility by defaulting omitted worker pools to
  `default`.
- Rejected empty worker pool values during registration.
- Updated scheduler preview so it only pairs queued jobs with fresh workers
  whose pool is allowed by that job type's workload pool policy.
- Updated worker-pull claims so a worker only leases queued jobs whose workload
  allows that worker's pool.
- Preserved existing priority/FIFO behavior among jobs that are eligible for the
  claiming worker's pool.
- Added per-pool async placement counters for selected claim decisions,
  compatible worker counts, and queued jobs skipped because the claiming
  worker's pool is not allowed.

## Test Coverage

Immediate coverage includes:

- omitted worker pool defaults to `default`
- explicit worker pool metadata is trimmed and returned by the worker API
- empty worker pool registration is rejected
- scheduler preview skips workers outside workload pool policy
- scheduler preview selects a worker from an allowed pool
- worker claims skip higher-priority jobs outside the worker's pool and claim
  the next eligible job
- worker claims return `204 No Content` and release capacity when no queued job
  is eligible for the worker's pool
- selected worker claims emit async pool selection and candidate-worker metrics
- no-compatible-pool worker claims emit skipped-job metrics

## Remaining 7C Work

- Review whether worker registration should validate pools against configured
  `POOLS_JSON` once the shared demo shape is clearer.
