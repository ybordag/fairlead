# Phase 6A Clew

## Goal

Clean up Fairlead's synchronous proxy surface before adding async jobs, durable
queues, workers, or job state.

The main direction is to make routing behavior explicit as workload metadata
instead of scattering route-specific choices through handler arguments and
conditionals.

## Scope

Clew includes:

- Workload route metadata for synchronous proxy routes.
- Route metadata for upstream path, retry behavior, backend pool, and metric
  labels.
- Backend pool selection by workload.
- A clear session-affinity policy: global, per workload, or per backend pool.
- Provider/header forwarding policy.
- `GET /v1/models` from configured workloads and backend metadata.

Clew does not include:

- Async job submission, status, cancellation, leases, retries, or callbacks.
- Durable priority queues.
- Worker registration or heartbeat.
- Queue depth or wait-time metrics.
- Cloud-provider fallback unless a clear demo need appears.

## First Slice

Implemented:

- Added `WorkloadRoute` metadata.
- Moved chat completions and embeddings upstream paths out of handler arguments.
- Kept current runtime behavior unchanged: both routes still retry upstream
  server errors before response bytes are streamed.

Next likely slice:

- Add backend-pool eligibility to route metadata and selection.
- Decide whether the current default pool remains implicit for legacy `BACKENDS`.
