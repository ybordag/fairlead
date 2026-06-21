# Fairlead Roadmap

## Purpose

This document tracks the effort to make Fairlead a general compute router, not a
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

## Completed Generalized Proxy Scope Boundary

The completed generalized proxy work made today's request path resilient,
explainable, and demonstrable without starting the future scheduler or async job
system.

Completed generalized proxy scope includes:

- OpenAI-compatible synchronous proxying for chat completions and embeddings.
- Node-aware backend metadata.
- Origin-aware routing.
- Session affinity.
- Circuit breaking and clearer backend health probing.
- Same-request retry for safe synchronous upstream failures.
- Basic routing observability for the synchronous proxy.
- A repeatable small-cluster or mock demo.
- Documentation for local/edge deployment and sanitized fixtures.

The completed generalized proxy scope does not include:

- Resource registry or VRAM-aware scheduling.
- Resource-aware backend selection.
- Durable priority queues.
- Async job submission, status, cancellation, worker registration, or callbacks.
- Cloud-provider fallback and provider credential policy.
- Full adapter implementations for non-OpenAI-compatible protocols.

Those deferred items belong to later branches/phases so each branch stays a
coherent milestone.

## Trim Scope Boundary

Trim is the follow-on Phase 5 branch. It should finish resource-aware synchronous
routing and priority admission without implementing the async compute scheduler.

Trim includes:

- Cooperative resource reports through `POST /v1/resources/report`.
- Resource snapshots through `GET /v1/resources`.
- Stale-report handling.
- Resource-aware backend eligibility when `RESOURCE_AWARE_ROUTING=true`.
- Load/headroom ranking among eligible backends.
- Priority parsing for synchronous proxy requests.
- Per-priority synchronous in-flight limits.
- `429 Too Many Requests` when a synchronous priority bucket is full.
- Priority limit and in-flight metrics.

Trim does not include:

- Durable priority queues.
- Queue depth or queue wait-time metrics.
- Worker registration, worker heartbeat, or worker utilization metrics.
- Async job submission, status, cancellation, leases, retries, or callbacks.
- Job duration metrics.
- Backend pool splitting by workload.
- Provider/header forwarding policy.
- `/v1/models`.
- Adapter boundaries for non-OpenAI-compatible protocols.
- Cloud-provider fallback and credential policy.

Those items are explicitly assigned to future phases below.

## Current Baseline

Fairlead currently provides:

- Axum HTTP service with `/health`, `/metrics`, `/v1/chat/completions`, and
  `/v1/embeddings`.
- Resource reporting and resource snapshots through `/v1/resources/report` and
  `/v1/resources`.
- Ordered backend selection from `BACKENDS`.
- Per-backend circuit breakers with background health probes.
- Health probes use derived or configured health URLs instead of probing the
  backend base URL directly.
- Soft session affinity through `X-Fairlead-Thread-Id`.
- Origin-node locality through `X-Fairlead-Origin-Node`.
- Streaming proxy support for Server-Sent Events.
- Prometheus circuit-state, request, retry, fallback, and latency metrics.
- Prometheus resource metrics and priority admission metrics.
- Node-aware backend metadata through `BACKENDS_JSON`.
- `WorkloadKind` metadata for chat completions and embeddings.
- Resource-aware routing when enabled.
- Per-priority synchronous admission limits.
- Documentation for a manual two-node DGX Spark deployment.
- Sanitized fixture conventions and ignore rules for private local config.

It does not yet provide:

- Workload-aware route selection.
- Separate backend pools by workload type.
- Provider-specific auth/header policies.
- CPU resource accounting and richer resource dimensions beyond coarse VRAM/load.
- Durable priority queues.
- Async job submission, status, cancellation, worker registration, or callbacks.

## Easy Tasks

These should be achievable without changing the core architecture.

- [x] Update `README.md` so it reflects the current runnable service and
  generalization direction.
- [ ] Add a short glossary for `backend`, `provider`, `worker`, `workload`,
  `route`, and `affinity` so future docs use the terms consistently.
- [ ] Document the current supported workload shape: HTTP request in, selected
  backend URL out, streamed or buffered HTTP response back.
- [x] Add example configuration for multiple OpenAI-compatible local backends.
- [x] Document how any OpenAI-compatible client can point at Fairlead without
  Rhizome in the loop.
- [x] Add explicit docs for current headers:
  `X-Fairlead-Thread-Id` and `X-Fairlead-Origin-Node`.
- [x] Document manual two-node DGX Spark deployment commands and expected
  observations.
- [x] Add fixture/local-config hygiene docs and `.gitignore` rules for private
  local deployment files.
- [x] Add the deferred low-risk tests listed in
  `docs/current_work/deferred_tests.md`.
- [x] Run and require the current quality gate:
  `cargo fmt --check`, `cargo clippy --all -- -D warnings`, and `cargo test`.

## Moderate Tasks

These require small abstractions, but should not require a full scheduler or job
system.

- [x] Introduce a `WorkloadKind` enum for synchronous proxy workloads, starting
  with `chat_completions` and `embeddings`.
- [x] Move route-specific behavior out of the stringly typed
  `forward(state, path, headers, body)` call and into workload metadata.
- [x] Add route metadata for:
  path,
  allowed HTTP method,
  streaming behavior,
  retry policy,
  backend pool name,
  metric labels.
- [ ] Split backend configuration by pool so different workloads can target
  different backend sets.
- [x] Preserve a default backend pool for today's simple `BACKENDS` config.
- [x] Add provider/header forwarding policy:
  content type,
  authorization,
  organization/project headers,
  and provider-specific opt-in headers.
- [x] Add `/v1/models` for the synchronous proxy surface, backed by configured
  workloads and backend metadata.
- [ ] Add an adapter boundary for non-OpenAI-compatible synchronous endpoints,
  such as `/v1/rerank` or `/v1/images/generations`.
- [x] Add metrics labels for workload kind and selected backend.
- [x] Decide whether session affinity should be keyed globally, per workload, or
  per backend pool.
- [x] Make health probes target an explicit backend health endpoint such as
  `/health` or `/v1/models`, rather than relying on the backend base URL.
- [x] Implement same-request fallback for retryable upstream failures where it is
  safe to replay the request body.

## Implementation Epics

These are the first implementation-ready epics. They turn the broad plan above
into features with concrete acceptance criteria.

### 1. Node-Aware Backend Model

Goal: make Fairlead understand where a backend runs and what workloads it can
serve.

Scope:

- [x] Add stable `backend_id` and `node_id` fields to backend configuration.
- [x] Add a backend `pool` or `backend_pool_id`.
- [x] Add supported workload kinds per backend.
- [x] Preserve the current comma-separated `BACKENDS` env var as a default pool
  for simple local use.
- [x] Document example config for spark-a and spark-b:
  `spark-a-vllm`, `spark-b-vllm`, node IDs, backend URLs, and supported workloads.

Acceptance criteria:

- Existing `BACKENDS=http://spark-a:8000/v1,http://spark-b:8000/v1` still works.
- A richer config can describe at least two backends on different nodes.
- Metrics and logs can identify the selected backend by stable ID, not only URL.

### 2. Origin-Aware Routing

Goal: prefer same-node inference when a request originates from a node that has a
healthy eligible local backend.

Scope:

- [x] Add request origin metadata, initially via `X-Fairlead-Origin-Node`.
- [x] Add route selection logic that prefers backends on the origin node when
  they are circuit-closed. Resource eligibility is added in the resource-aware
  selection epic.
- [x] Define precedence between locality and existing session affinity.
- [x] Add tests for requests from spark-a preferring spark-a and requests from
  spark-b preferring spark-b.

Current precedence:

```text
eligible backend on origin node
  -> existing session affinity if still eligible
  -> configured fallback order
```

Acceptance criteria:

- A request with `X-Fairlead-Origin-Node: spark-a` selects spark-a's backend when it is
  healthy and eligible.
- If spark-a is circuit-open, the same request selects spark-b.
- The reverse behavior works for `X-Fairlead-Origin-Node: spark-b`.

### 3. Health Probe Target Cleanup

Goal: make backend health probing explicit and easy to reason about for
OpenAI-compatible backends.

Scope:

- [x] Add a configured or derived probe path for each backend.
- [x] Support `/health` through explicit `health_path` configuration.
- [x] Use `/v1/models` as the OpenAI-compatible default probe.
- [x] Preserve connection-liveness behavior for simple custom backends.
- [x] Add tests for healthy probe, unreachable backend, and probe URL
  derivation.

Acceptance criteria:

- vLLM backends are probed through a meaningful endpoint instead of `GET /v1`.
- A reachable but invalid base path no longer creates confusing access logs.
- Existing simple local backends can still be probed without extra config.

### 4. Same-Request Retry

Goal: retry a request on the next eligible backend when the selected backend
fails before producing a successful response.

Scope:

- [x] Define retryable upstream failures: connection errors, timeouts, and
  selected 5xx statuses.
- [x] Keep request bodies replayable for non-streaming inbound requests.
- [x] Avoid retrying unsafe or already-partially-streamed responses.
- [x] Record retry/fallback reason in logs and metrics.
- [x] Add tests for primary connection failure -> secondary success.

Acceptance criteria:

- If spark-a is selected but connection fails, Fairlead retries spark-b before
  returning to the caller.
- If a backend starts streaming response bytes, Fairlead does not attempt to
  replay the partially completed response.
- Circuit state is updated for the failed backend.

### 5. Synchronous Routing Observability

Goal: make current synchronous routing decisions explainable from logs and
metrics.

Scope:

- [x] Add metrics for request count by workload, backend, origin node, and
  status.
- [x] Add latency metrics by workload and backend.
- [x] Add fallback/retry counters with reason labels.
- [x] Add structured tracing fields for request ID, workload, origin node,
  selected backend, fallback reason, affinity key, and retry count.

Acceptance criteria:

- A request from spark-a that falls back to spark-b can be explained from logs
  and metrics.
- Metrics identify whether fallback was caused by circuit state or upstream
  failure.
- Observability stays scoped to synchronous requests; queue, worker, and
  resource metrics remain deferred.

### 6. Small-Cluster Demo

Goal: create a portfolio-ready demonstration of Fairlead's routing behavior.

Scope:

- [x] Add a local demo with two mock OpenAI-compatible backends named spark-a and
  spark-b.
- [x] Simulate healthy, circuit-open, failed-then-retried, and recovered backend
  states.
- [x] Show same-node preference, peer-node fallback, same-request retry, and
  metrics output.
- [x] Document manual two-node DGX Spark deployment commands and expected
  observations.
- [x] Add a repeatable local mock demo that does not require GPUs.

Acceptance criteria:

- A reviewer can run the demo locally without real GPUs.
- The demo clearly shows why Fairlead exists beyond a basic reverse proxy.
- The same policy can later be pointed at real vLLM servers on spark-a and
  spark-b.

## Detailed Epic Notes

These notes preserve the implementation details behind completed and future
phases. Completed Trim items remain checked here; unchecked items belong to the
future phase named in the milestone proposal.

### Resource Registry v1

Goal: give Fairlead a simple control-plane view of node/backend capacity.

Scope:

- [x] Define resource structs for node ID, backend ID, total VRAM, reserved VRAM,
  current load, and timestamp.
- [x] Add an in-memory registry guarded by `Arc<RwLock<_>>`.
- [x] Add registration/update endpoint for cooperative reporting.
- [x] Add stale-report handling.
- [x] Add tests for resource registration, update, stale expiry, and lookup.

Initial API sketch:

```text
POST /v1/resources/report
GET  /v1/resources
```

Acceptance criteria:

- vLLM or a mock worker can report capacity for `spark-a` and `spark-b`.
- Fairlead can read the latest fresh report from the registry during backend
  selection when resource-aware routing is enabled.
- Stale reports stop being trusted after a configurable timeout.

### Resource-Aware Selection

Goal: incorporate resource state into synchronous backend selection.

Scope:

- [x] Extend backend eligibility to include reported headroom.
- [x] Add workload-level resource estimates, starting with a coarse default for
  chat and embeddings.
- [x] Decide conservative behavior when no resource report exists.
- [x] Add tests for local backend full -> peer backend selected.
- [x] Rank eligible candidates by load/headroom after locality and affinity.

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

- If spark-a has capacity, requests from spark-a select spark-a.
- If spark-a reports insufficient headroom, requests from spark-a select
  spark-b.
- If both spark-a and spark-b are ineligible, Fairlead returns 503. Queueing and
  cloud fallback remain future workload-policy work.

### Full Observability

Goal: extend synchronous routing observability to future resource, queue, worker,
and async-job behavior.

Scope:

- [x] Add resource metrics for reported VRAM/load per node.
- [x] Add synchronous priority limit and in-flight gauges.
- [ ] Add queue depth by priority and workload.
- [ ] Add queue wait time by priority and workload.
- [ ] Add worker availability and utilization.
- [ ] Add job duration and callback success/failure.

Acceptance criteria:

- Metrics identify whether fallback was caused by circuit state, resource state,
  upstream failure, queue policy, or worker health.

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

Implemented groundwork:

- [x] Define the priority values.
- [x] Parse `X-Fairlead-Priority` on synchronous requests.
- [x] Default missing priority to `realtime`.
- [x] Return `400` for unknown priority values.
- [x] Add priority labels to synchronous request, retry, fallback, and latency
  metrics.
- [x] Enforce per-priority synchronous admission limits.
- [x] Return `429` when a synchronous priority bucket is full.
- [x] Expose per-priority limit and in-flight metrics.

Hard parts:

- Avoid starving background work forever during heavy realtime use.
- Bound concurrency per workload and per resource pool, not only by coarse
  priority.
- Decide whether any synchronous HTTP requests should queue instead of failing
  fast.
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
- Attempt count and retry limit.
- Per-job timeout and worker lease expiration.
- Callback URL and callback delivery state.
- Resource reservation and release for running attempts.
- Retry policy.
- Expiration and cleanup.

Open questions:

- Does the first implementation use in-memory state only, or SQLite-backed state?
- Are completed results stored, or only status plus callback outcome?
- How are duplicate submissions made idempotent?
- How are cancellations propagated to workers?

Early implementation scope:

- Start with bounded compute jobs, not arbitrary long-running workflows.
- Treat a multi-minute image-processing attempt as timed out unless the workload
  opts into a longer timeout.
- Use leases so Fairlead can recover from worker loss without holding an open
  process relationship indefinitely.
- Keep Rhizome as the source of truth for domain objects such as `VisionJob`.
- Defer Temporal until product workflows need durable multi-step orchestration,
  fanout/fanin, long waits, or compensation logic.

See `docs/planning/architecture.md` for the scheduler boundary, persistence
rationale, and Temporal deferral rule.

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

### Phase 4/Completed: Generalized Synchronous Proxy

- [x] Complete the easy tasks that support the synchronous proxy.
- [x] Introduce `WorkloadKind`.
- [x] Clean up backend health probe targets.
- [x] Add basic same-request retry for safe synchronous upstream failures.
- [x] Add workload-aware routing metrics and retry/fallback counters.
- [x] Add a repeatable local mock demo.
- [x] Move route-specific behavior into workload metadata.
- [ ] Add backend pools. Deferred to **Phase 6A: Synchronous Surface Cleanup**.
- [x] Add provider/header policy.
- [x] Add `/v1/models`.

### Phase 5/Trim: Resource-Aware Routing and Priority Admission

- [x] Add resource registry.
- [x] Add resource-aware backend eligibility.
- [x] Add conservative behavior for unknown capacity.
- [x] Add resource metrics.
- [x] Add per-priority synchronous admission limits.
- [x] Return 429 instead of queueing when a synchronous priority bucket is full.

### Phase 6A: Synchronous Surface Cleanup

This phase keeps the synchronous proxy surface clean before adding async jobs.
It should not introduce queues, workers, or job state.

- [x] Move route-specific behavior out of `forward(state, path, headers, body)` and
  into workload metadata.
- [x] Add route metadata for path, method, streaming behavior, retry policy, backend
  pool, and metric labels.
- [ ] Split backend configuration by pool so different synchronous workloads can
  target different backend sets.
- [x] Decide whether session affinity is global, per workload, or per backend pool.
- [x] Add provider/header forwarding policy for content type, authorization,
  organization/project headers, and provider-specific opt-in headers.
- [x] Add `GET /v1/models` backed by configured workloads and backend metadata.
- Keep cloud-provider fallback and provider credentials deferred unless a clear
  demo need appears.

### Phase 6B: Async Compute Router

- Add job API: submit, status, and cancellation.
- Add durable priority queues.
- Add queue depth and queue wait-time metrics by priority and workload.
- Add worker registration and heartbeat.
- Add worker availability and utilization metrics.
- Add bounded job attempts with timeouts, leases, retry limits, and cancellation.
- Add durable-enough job state, starting with in-memory state for tests and
  SQLite as the first persistent backend.
- Add callback delivery.
- Add job duration and callback success/failure metrics.
- Add async workload metrics.
- Document Temporal as deferred unless Rhizome needs durable multi-step workflow
  orchestration beyond compute dispatch.

### Phase 7: Advanced Workloads

- Add adapter boundaries for non-OpenAI-compatible synchronous and async
  endpoints, such as `/v1/rerank`, image generation, and vision analysis.
- Add concrete adapters for rerank, image, vision, batch embeddings, index
  builds, and clustering.
- Add cancellation and idempotency.
- Add richer retry policies.
- Add richer resource dimensions beyond coarse VRAM/load where workloads need
  CPU slots, GPU slots, model residency, disk bandwidth, or custom worker
  capacity.
- Add cloud-provider fallback and provider credential policy if local/edge
  deployment needs external overflow capacity.
- Add deployment documentation for multiple applications sharing Fairlead.
