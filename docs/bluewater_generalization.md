# Bluewater Generalization Plan

## Purpose

Bluewater is the effort to make Fairlead a general compute router, not a
Rhizome-specific inference proxy.

The intended shape is:

```text
request or job
  -> workload type
  -> eligible backend or worker pool
  -> resource constraints
  -> priority
  -> dispatch strategy
  -> streamed response, synchronous response, or async callback
```

OpenAI-compatible chat and embeddings are the first implemented workload types.
They should remain first-class, but they should not define the whole system.

## Current Baseline

Fairlead currently provides:

- Axum HTTP service with `/health`, `/metrics`, `/v1/chat/completions`, and
  `/v1/embeddings`.
- Ordered backend selection from `BACKENDS`.
- Per-backend circuit breakers with background health probes.
- Soft session affinity through `X-Fairlead-Thread-Id`.
- Streaming proxy support for Server-Sent Events.
- Basic Prometheus circuit-state metrics.

It does not yet provide:

- First-class workload definitions.
- Separate backend pools by workload type.
- Provider-specific auth/header policies.
- VRAM or CPU resource accounting.
- Priority queues.
- Async job submission, status, cancellation, worker registration, or callbacks.
- True same-request retry across the fallback chain after an upstream failure.

## Easy Tasks

These should be achievable without changing the core architecture.

- [ ] Update `README.md` so it reflects the current runnable Phase 4 service and
  the Bluewater generalization direction.
- [ ] Add a short glossary for `backend`, `provider`, `worker`, `workload`,
  `route`, and `affinity` so future docs use the terms consistently.
- [ ] Document the current supported workload shape: HTTP request in, selected
  backend URL out, streamed or buffered HTTP response back.
- [ ] Add example configuration for multiple OpenAI-compatible local backends.
- [ ] Document how any OpenAI-compatible client can point at Fairlead without
  Rhizome in the loop.
- [ ] Add explicit docs for current headers:
  `X-Fairlead-Thread-Id` and future `X-Fairlead-Priority`.
- [ ] Add the deferred low-risk tests listed in `docs/deferred_tests.md`.
- [ ] Run and require the current quality gate:
  `cargo fmt --check`, `cargo clippy --all -- -D warnings`, and `cargo test`.

## Moderate Tasks

These require small abstractions, but should not require a full scheduler or job
system.

- [ ] Introduce a `WorkloadKind` enum for synchronous proxy workloads, starting
  with `chat_completions` and `embeddings`.
- [ ] Move route-specific behavior out of the stringly typed
  `forward(state, path, headers, body)` call and into workload metadata.
- [ ] Add route metadata for:
  path,
  allowed HTTP method,
  streaming behavior,
  retry policy,
  backend pool name,
  metric labels.
- [ ] Split backend configuration by pool so different workloads can target
  different backend sets.
- [ ] Preserve a default backend pool for today's simple `BACKENDS` config.
- [ ] Add provider/header forwarding policy:
  content type,
  authorization,
  organization/project headers,
  and provider-specific opt-in headers.
- [ ] Add `/v1/models` for the synchronous proxy surface, backed by configured
  workloads and backend metadata.
- [ ] Add an adapter boundary for non-OpenAI-compatible synchronous endpoints,
  such as `/v1/rerank` or `/v1/images/generations`.
- [ ] Add metrics labels for workload kind and selected backend.
- [ ] Decide whether session affinity should be keyed globally, per workload, or
  per backend pool.
- [ ] Implement same-request fallback for retryable upstream failures where it is
  safe to replay the request body.

## Hard Tasks

These are the pieces that turn Fairlead from a resilient proxy into a general
compute router.

### Resource Accounting

Fairlead needs a shared resource model before it can schedule heterogeneous work.
VRAM is the first resource, but the model should not assume GPU memory is the
only constraint.

Questions to settle:

- What resources are tracked: VRAM, system RAM, CPU slots, GPU slots, model
  residency, disk bandwidth, or custom worker capacity?
- Are resources reported cooperatively by workers, polled from nodes, or both?
- What happens when a consumer fails to deregister?
- Should unregistered capacity be treated as available, unavailable, or unknown?
- Does a synchronous inference request reserve estimated resources before
  dispatch, or only consult current backend state?

Initial design direction:

- Use cooperative registration first.
- Store resources per node and per worker.
- Treat unknown resource state conservatively for scheduled jobs.
- Keep the inference path fast: resource checks must be cheap and read-heavy.

### Priority Scheduling

The core guarantee is that user-waiting work must not sit behind background
maintenance work.

Priority levels:

- `realtime`: chat completions, query-time embeddings, user-waiting requests.
- `batch`: user-triggered async work, such as vision analysis.
- `background`: ingestion, index builds, clustering, cleanup.

Hard parts:

- Avoid starving background work forever during heavy realtime use.
- Bound concurrency per workload and per resource pool.
- Decide whether synchronous HTTP requests can queue or should fail fast.
- Make queue depth and wait time observable.
- Keep strict priority without accidentally blocking the Tokio runtime.

### Async Job API

Async jobs need durable-enough state to survive normal service behavior without
turning Fairlead into the system of record for business data.

Proposed API surface:

```text
POST   /v1/jobs
GET    /v1/jobs/{id}
DELETE /v1/jobs/{id}
```

Needed concepts:

- Job ID.
- Workload type.
- Priority.
- Payload.
- Status: queued, running, complete, failed, cancelled.
- Callback URL and callback delivery state.
- Retry policy.
- Expiration and cleanup.

Open questions:

- Is job state in memory first, or backed by a lightweight database?
- Are completed results stored, or only status plus callback outcome?
- How are duplicate submissions made idempotent?
- How are cancellations propagated to workers?

### Worker Registration

General workloads require workers that can announce what they can do.

Proposed API surface:

```text
POST   /v1/workers/register
POST   /v1/workers/{id}/heartbeat
DELETE /v1/workers/{id}
GET    /v1/workers
```

Worker metadata:

- Worker ID.
- Node ID.
- Supported workload types.
- Resource requirements or available capacity.
- Endpoint URL.
- Health state.
- Heartbeat timestamp.
- Current load.

Hard parts:

- Deregister stale workers without dropping in-flight jobs incorrectly.
- Distinguish worker health from node health.
- Handle rolling deploys and worker version changes.
- Define capability matching without making the scheduler provider-specific.

### Adapter Model

Not every useful workload will be OpenAI-compatible. Fairlead needs an adapter
boundary so the router core can stay generic while individual protocols remain
pluggable.

Adapter responsibilities:

- Validate incoming request shape when Fairlead owns the public API.
- Convert request body if the upstream provider uses a different protocol.
- Decide which upstream responses are retryable.
- Preserve streaming when supported.
- Normalize errors enough for metrics and fallback decisions.

The router core should not know about gardens, agent threads, model-specific
payloads, or provider-specific JSON beyond the adapter boundary.

### Observability

Generalization will fail operationally if every workload looks the same in
metrics.

Needed metrics:

- Request count by workload, backend, status, and retry outcome.
- Request latency by workload and backend.
- Circuit state by backend.
- Queue depth by priority and workload.
- Queue wait time by priority and workload.
- Worker availability and utilization.
- Resource used/free by node and resource kind.
- Job duration and callback success/failure.

Tracing should carry:

- Request ID.
- Workload kind.
- Selected backend or worker.
- Thread/session affinity key when present.
- Retry/fallback path.
- Job ID for async work.

## Non-Goals

Fairlead should not own:

- End-user authentication.
- Application domain objects.
- Business database schema.
- Long-term result storage.
- Container scheduling or process supervision.
- Provider billing policy.

Those belong to Cambium, Rhizome or other applications, worker services, k3s,
Docker, or the provider accounts themselves.

## Milestone Proposal

### Bluewater 1: Generalized Synchronous Proxy

- Complete the easy tasks.
- Introduce `WorkloadKind`.
- Add route/workload metadata.
- Add backend pools.
- Add provider/header policy.
- Add `/v1/models`.
- Add workload-aware metrics.

### Bluewater 2: Resource-Aware Routing

- Add resource registry.
- Add resource-aware backend eligibility.
- Add conservative behavior for unknown capacity.
- Add resource metrics.

### Bluewater 3: Async Compute Router

- Add job API.
- Add priority queues.
- Add worker registration and heartbeat.
- Add callback delivery.
- Add async workload metrics.

### Bluewater 4: Advanced Workloads

- Add adapters for rerank, image, vision, batch embeddings, index builds, and
  clustering.
- Add cancellation and idempotency.
- Add richer retry policies.
- Add deployment documentation for multiple applications sharing Fairlead.
