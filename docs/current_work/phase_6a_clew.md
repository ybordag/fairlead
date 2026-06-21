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
- Backend pool metadata on routes and backends.
- A clear session-affinity policy: global, per workload, or per backend pool.
- Provider/header forwarding policy.
- `GET /v1/models` from configured workloads and backend metadata.

Clew does not include:

- Async job submission, status, cancellation, leases, retries, or callbacks.
- Durable priority queues.
- Worker registration or heartbeat.
- Queue depth or wait-time metrics.
- Complete pool-aware backend configuration, pool fallback chains, or placement
  policies. These are deferred to Phase 7 so they can cover both synchronous
  backends and async workers.
- Cloud-provider fallback unless a clear demo need appears.

## First Slice

Implemented:

- Added `WorkloadRoute` metadata.
- Moved chat completions and embeddings upstream paths out of handler arguments.
- Kept current runtime behavior unchanged: both routes still retry upstream
  server errors before response bytes are streamed.

## Second Slice

Implemented:

- Added route-level backend pool policy.
- Preserved compatibility by letting current chat and embeddings routes accept
  any configured pool.
- Enforced workload eligibility so chat requests skip embeddings-only backends
  and embeddings requests skip chat-only backends.
- Return `503` without touching an upstream backend when no backend supports the
  requested workload.

## Third Slice

Implemented:

- Chose per-workload session affinity for Clew.
- Kept the public `X-Fairlead-Thread-Id` header unchanged.
- Scoped the stored affinity key as `<workload>:<thread-id>`, for example
  `chat_completions:abc` or `embeddings:abc`.
- Added tests proving chat and embeddings with the same thread ID can keep
  independent backend affinity.

## Fourth Slice

Implemented:

- Added explicit upstream request header policy.
- Preserve incoming `content-type`; default to `application/json` when missing.
- Forward `authorization`.
- Forward allowlisted provider opt-in headers:
  - `openai-organization`
  - `openai-project`
  - `anthropic-version`
  - `anthropic-beta`
  - `x-goog-api-key`
- Do not forward Fairlead routing/control headers such as
  `x-fairlead-thread-id`, `x-fairlead-origin-node`, and
  `x-fairlead-priority`.

## Fifth Slice

Implemented:

- Added `GET /v1/models`.
- Return an OpenAI-style model list with one entry per configured backend.
- Include Fairlead backend metadata: backend ID, backend URL, health URL, node
  ID, pool, and supported workloads.
- Do not fan out to upstream backends or infer actual served model names.

Next likely slice:

- Run a final Phase 6A readiness audit, then open the Clew PR.
