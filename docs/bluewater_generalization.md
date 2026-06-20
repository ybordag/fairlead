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
- Node-aware backend metadata through `BACKENDS_JSON`.
- `WorkloadKind` metadata for chat completions and embeddings.

It does not yet provide:

- Workload-aware route selection.
- Separate backend pools by workload type.
- Provider-specific auth/header policies.
- VRAM or CPU resource accounting.
- Priority queues.
- Async job submission, status, cancellation, worker registration, or callbacks.
- True same-request retry across the fallback chain after an upstream failure.

## Easy Tasks

These should be achievable without changing the core architecture.

- [x] Update `README.md` so it reflects the current runnable Phase 4 service and
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

- [x] Introduce a `WorkloadKind` enum for synchronous proxy workloads, starting
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
- [x] Preserve a default backend pool for today's simple `BACKENDS` config.
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

## Implementation Epics

These are the first implementation-ready Bluewater epics. They turn the broad
plan above into features with concrete acceptance criteria.

### 1. Node-Aware Backend Model

Goal: make Fairlead understand where a backend runs and what workloads it can
serve.

Scope:

- [x] Add stable `backend_id` and `node_id` fields to backend configuration.
- [x] Add a backend `pool` or `backend_pool_id`.
- [x] Add supported workload kinds per backend.
- [x] Preserve the current comma-separated `BACKENDS` env var as a default pool
  for simple local use.
- [x] Document example config for Loki and Thor:
  `loki-vllm`, `thor-vllm`, node IDs, backend URLs, and supported workloads.

Acceptance criteria:

- Existing `BACKENDS=http://loki:8000/v1,http://thor:8000/v1` still works.
- A richer config can describe at least two backends on different nodes.
- Metrics and logs can identify the selected backend by stable ID, not only URL.

### 2. Origin-Aware Routing

Goal: prefer same-node inference when a request originates from a node that has a
healthy eligible local backend.

Scope:

- [ ] Add request origin metadata, initially via `X-Fairlead-Origin-Node`.
- [ ] Add route selection logic that prefers backends on the origin node when
  they are circuit-closed and resource-eligible.
- [ ] Define precedence between locality and existing session affinity.
- [ ] Add tests for Loki-origin requests preferring Loki and Thor-origin requests
  preferring Thor.

Proposed precedence:

```text
eligible backend on origin node
  -> existing session affinity if still eligible
  -> lowest-load eligible backend
  -> configured fallback order
```

Acceptance criteria:

- A request with `X-Fairlead-Origin-Node: loki` selects Loki's backend when it is
  healthy and eligible.
- If Loki is circuit-open or resource-ineligible, the same request selects Thor.
- The reverse behavior works for `X-Fairlead-Origin-Node: thor`.

### 3. Resource Registry v1

Goal: give Fairlead a simple control-plane view of node/backend capacity.

Scope:

- [ ] Define resource structs for node ID, backend ID, total VRAM, reserved VRAM,
  current load, and timestamp.
- [ ] Add an in-memory registry guarded by `Arc<RwLock<_>>`.
- [ ] Add registration/update endpoint for cooperative reporting.
- [ ] Add stale-report handling.
- [ ] Add tests for resource registration, update, stale expiry, and lookup.

Initial API sketch:

```text
POST /v1/resources/report
GET  /v1/resources
```

Acceptance criteria:

- vLLM or a mock worker can report capacity for `loki` and `thor`.
- Fairlead can read the latest reported capacity during backend selection.
- Stale reports stop being trusted after a configurable timeout.

### 4. Resource-Aware Selection

Goal: incorporate resource state into synchronous backend selection.

Scope:

- [ ] Extend backend eligibility to include reported headroom or load.
- [ ] Add workload-level resource estimates, starting with a coarse default for
  chat and embeddings.
- [ ] Decide conservative behavior when no resource report exists.
- [ ] Add tests for local backend full -> peer backend selected.

Proposed decision pipeline:

```text
candidates = backends in workload's backend pool
candidates = remove circuit-open backends
candidates = remove backends without enough reported capacity
rank by origin-node locality
rank by session affinity
rank by load/headroom
rank by configured order
```

Acceptance criteria:

- If Loki has capacity, Loki-origin requests select Loki.
- If Loki reports insufficient headroom, Loki-origin requests select Thor.
- If both Loki and Thor are ineligible, Fairlead returns the configured
  no-capacity behavior: queue, 503, or cloud fallback.

### 5. Same-Request Retry

Goal: retry a request on the next eligible backend when the selected backend
fails before producing a successful response.

Scope:

- [ ] Define retryable upstream failures: connection errors, timeouts, and
  selected 5xx statuses.
- [ ] Keep request bodies replayable for non-streaming inbound requests.
- [ ] Avoid retrying unsafe or already-partially-streamed responses.
- [ ] Record retry/fallback reason in logs and metrics.
- [ ] Add tests for primary connection failure -> secondary success.

Acceptance criteria:

- If Loki is selected but connection fails, Fairlead retries Thor before
  returning to the caller.
- If a backend starts streaming response bytes, Fairlead does not attempt to
  replay the partially completed response.
- Circuit state is updated for the failed backend.

### 6. Observability v1

Goal: make routing decisions explainable.

Scope:

- [ ] Add metrics for request count by workload, backend, origin node, and
  status.
- [ ] Add latency metrics by workload and backend.
- [ ] Add fallback/retry counters with reason labels.
- [ ] Add resource metrics for reported VRAM/load per node.
- [ ] Add structured tracing fields for request ID, workload, origin node,
  selected backend, fallback reason, and affinity key.

Acceptance criteria:

- A Loki-origin request that falls back to Thor can be explained from logs and
  metrics.
- Metrics identify whether fallback was caused by circuit state, resource state,
  or upstream failure.

### 7. Small-Cluster Demo

Goal: create a portfolio-ready demonstration of Fairlead's routing behavior.

Scope:

- [ ] Add a local demo with two mock OpenAI-compatible backends named Loki and
  Thor.
- [ ] Simulate healthy, circuit-open, full, and recovered backend states.
- [ ] Show same-node preference, peer-node fallback, all-full behavior, and
  metrics output.
- [ ] Document the demo commands and expected observations.

Acceptance criteria:

- A reviewer can run the demo locally without real GPUs.
- The demo clearly shows why Fairlead exists beyond a basic reverse proxy.
- The same policy can later be pointed at real vLLM servers on Loki and Thor.

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
