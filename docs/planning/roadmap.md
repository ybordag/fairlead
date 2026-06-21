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

At the time, the completed generalized proxy scope did not include:

- Resource registry or VRAM-aware scheduling.
- Resource-aware backend selection.
- Durable priority queues.
- Async job submission, status, cancellation, worker registration, or callbacks.
- Cloud-provider fallback and provider credential policy.
- Full adapter implementations for non-OpenAI-compatible protocols.

Some of those items have since landed in Trim, Clew, and Tackle. This boundary
is kept as historical context for why the generalization work was split across
small branches.

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

- Axum HTTP service with `/health`, `/metrics`, `/v1/models`,
  `/v1/chat/completions`, `/v1/embeddings`, resource endpoints, async job
  endpoints, worker endpoints, and scheduler preview.
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
- Async job API for submission, listing, polling, and cancellation.
- Per-priority async queue state with SQLite-backed recovery when enabled.
- Queue depth and queue wait-time metrics.
- Worker registration, heartbeat, stale status, and availability metrics.
- Non-dispatching scheduler preview endpoint that matches queued jobs to fresh,
  capable workers without leasing or dispatching.
- Worker-pull claims, lease renewal, completion, failure, retryable requeue, and
  worker in-flight capacity accounting.
- Terminal job duration metrics and explicit timeout state for expired attempts.
- Opt-in SQLite job persistence for queue order, attempts, lease state,
  terminal state, result/error state, callback metadata, and callback delivery
  state.
- Terminal async job callbacks with bounded retry, timeout, success/failure
  metrics, and at-least-once restart recovery when SQLite persistence is
  enabled.
- Local GPU-free demos for synchronous routing and async job callbacks.
- Documentation for a manual two-node DGX Spark deployment.
- Sanitized fixture conventions and ignore rules for private local config.

It does not yet provide:

- CPU resource accounting and richer resource dimensions beyond coarse VRAM/load.
- Durable starvation/fairness policy beyond current priority queue ordering.
- Background pruning loops beyond the explicit `POST /v1/jobs/prune` endpoint.
- Adapter boundaries for non-OpenAI-compatible protocols.
- Cloud-provider overflow pools and provider credential policy.
- Multi-instance job coordination beyond single-process SQLite.

## Easy Tasks

These should be achievable without changing the core architecture.

- [x] Update `README.md` so it reflects the current runnable service and
  generalization direction.
- [x] Add a short glossary for `backend`, `provider`, `worker`, `workload`,
  `route`, and `affinity` so future docs use the terms consistently.
- [x] Document the current supported workload shape: HTTP request in, selected
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
- [x] Complete synchronous pool-aware backend configuration and routing policy.
  Phase 7B applies workload pool policy to chat and embedding routing with
  ordered pool fallback.
- [x] Complete async worker pool placement. Phase 7C applies workload pool
  policy to worker registration metadata, scheduler preview, worker-pull claims,
  and async placement metrics.
- [x] Preserve a default backend pool for today's simple `BACKENDS` config.
- [x] Add provider/header forwarding policy:
  content type,
  authorization,
  organization/project headers,
  and provider-specific opt-in headers.
- [x] Add `/v1/models` for the synchronous proxy surface, backed by configured
  workloads and backend metadata.
- [ ] Add an adapter boundary for non-OpenAI-compatible synchronous endpoints,
  such as `/v1/rerank` or `/v1/images/generations`. Deferred to
  **Phase 9: Adapter Boundaries and New Workloads**.
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
candidates = backends eligible for the workload
candidates = backends in workload's backend pool (Phase 7A)
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
- [x] Add queue depth by priority and workload.
- [x] Add queue wait time by priority and workload.
- [x] Add worker availability.
- [x] Add worker utilization.
- [x] Add job duration.
- [x] Add callback success/failure.

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
- Use queue depth and wait time to drive scheduling policy without adding
  unbounded waiting to synchronous requests.
- Keep strict priority without accidentally blocking the Tokio runtime.

### Async Job API

Async jobs need durable-enough state to survive normal service behavior without
turning Fairlead into the system of record for business data.

Implemented initial API surface:

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
- Optional submit idempotency key for safe caller retries.
- Resource reservation and release for running attempts.
- Retry policy.
- Expiration and cleanup.

Open questions:

- Does the first implementation use in-memory state only, or SQLite-backed state?
- Are completed results stored, or only status plus callback outcome?
- How are cancellations propagated to workers?

Implemented early Phase 6B scope:

- In-memory job records and per-priority queue state.
- Submit, list, poll, and cancel endpoints.
- Queue depth and wait-time metrics.
- Non-dispatching scheduler preview endpoint for matching queued jobs to fresh
  workers.

Scheduler boundary that guided Phase 6C+ work:

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

Implemented API surface:

```text
POST   /v1/workers/register
POST   /v1/workers/{id}/heartbeat
POST   /v1/workers/{id}/drain
POST   /v1/workers/{id}/reactivate
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

Implemented Phase 6B scope:

- Worker register/upsert.
- Worker heartbeat.
- Worker stale status.
- Worker listing.
- Worker availability metrics by job type and status.

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
- Job duration.
- Callback success/failure.

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
- [ ] Add complete backend pools. Deferred to
  **Phase 7: Pool-Aware Placement**.
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
- [x] Decide whether session affinity is global, per workload, or per backend pool.
- [x] Add provider/header forwarding policy for content type, authorization,
  organization/project headers, and provider-specific opt-in headers.
- [x] Add `GET /v1/models` backed by configured workloads and backend metadata.
- Full pool-aware backend configuration, pool fallback chains, and placement
  policies are deferred to **Phase 7** so they can cover both synchronous
  backends and async workers.
- Keep cloud-provider fallback and provider credentials deferred unless a clear
  demo need appears.

### Phase 6B: Async API and Scheduler Preview

Scope: establish the bounded async job surface, queue visibility, worker
registration, and non-dispatching selection logic. This phase proves that
Fairlead can decide which worker should receive which job, but it does not lease
or execute work yet.

- [x] Add first-slice in-memory job API: submit, status, and cancellation.
- [x] Add in-memory priority queue state and job listing.
- [x] Add queue depth metrics by priority and workload.
- [x] Add queue wait-time metrics by priority and workload.
- [x] Add non-dispatching worker registration and heartbeat.
- [x] Add worker availability metrics.
- [x] Add non-dispatching scheduler preview endpoint:
  queued job by priority/FIFO order -> fresh worker with matching job type.
- [x] Keep the preview non-mutating: no lease, no `running` transition, no
  worker call, and no callback.

### Phase 6C: Worker-Pull Claims and Leases

Scope: turn preview selection into atomic worker-pull claims while keeping worker
execution simple and bounded.

- [x] Add worker-pull claim endpoint.
- [x] Mark selected jobs `running` only when a lease is granted.
- [x] Store lease metadata: worker ID, lease expiry, attempt number, and claimed-at
  timestamp.
- [x] Prevent duplicate claims for the same job.
- [x] Requeue expired leases when attempts remain.
- [x] Mark expired leases `failed` when retry attempts are exhausted.
- [x] Add worker-scoped lease renewal for the worker holding the running lease.
- [x] Define initial cancellation semantics for queued and running jobs.
- [x] Add tests for priority ordering, FIFO ordering, stale worker exclusion,
  unsupported job types, and duplicate-claim prevention.
- [x] Add tests for lease expiry.
- [x] Add tests for lease renewal and lease ownership.
- [x] Add tests for cancellation ordering around running leases and requeued
  jobs.
- Defer true cancellation races with worker complete/fail endpoints until 6D.

### Phase 6D: Worker Execution, Retries, and Utilization

Scope: let workers complete or fail leased jobs and make execution behavior
observable.

- [x] Define the worker result contract.
- [x] Add completion and failure endpoints.
- [x] Enforce bounded attempts and retry limits for reported worker failures.
- [x] Enforce per-attempt timeouts.
- [x] Track worker in-flight counts and capacity usage.
- [x] Add worker utilization metrics.
- [x] Add job duration metrics.
- [x] Add tests for timeout accounting.
- [x] Add tests for success, retryable failure, and retry exhaustion.
- [x] Add tests for utilization accounting.
- [x] Add tests for duration accounting.
- [x] Add tests for stale/unknown workers on result endpoints and duplicate
  terminal result reports.

### Phase 6E: Durable Job State and Recovery

Scope: make async job state survive ordinary Fairlead restarts without making
Fairlead the application source of truth.

- [x] Add job store configuration with `memory` default and `sqlite` opt-in.
- [x] Add SQLite schema bootstrap as the first persistent backend.
- [x] Persist jobs, queue position, status, attempts, lease metadata,
  timestamps, callback metadata, and terminal state.
- [x] Recover queued jobs after restart.
- [x] Preserve list order, priority/FIFO queue order, and next job ID after
  restart.
- [x] Add restart/recovery tests for queued, running, cancelled, and complete
  job state.
- [x] Resolve stale running leases after restart.
- [x] Add endpoint-level SQLite restart/recovery tests.
- [x] Keep Rhizome or other callers as the source of truth for domain objects.
- Add process-level restart/recovery tests.

### Phase 6F: Callback Delivery and Async Finalization

Scope: close the async loop for callers that want status pushed back instead of
polling forever.

- [x] Deliver callbacks for terminal job states when `callback_url` is present.
- [x] Track callback success/failure separately from compute job status.
- [x] Add callback success/failure metrics.
- [x] Add callback retry and timeout policy.
- [x] Persist callback delivery state and recover pending callbacks after
  ordinary Fairlead restarts.
- [x] Add final async end-to-end demo and documentation.
- [x] Document Temporal as deferred unless Rhizome needs durable multi-step
  workflow orchestration beyond compute dispatch.

### Phase 7: Pool-Aware Placement

Goal: make named pools the shared placement model for synchronous backends and
async workers.

#### Phase 7A: Pool Model and Validation

- [x] Add named placement pools as the shared vocabulary for synchronous
  backends and future async workers.
- [x] Preserve the default pool behavior for simple `BACKENDS` configuration.
- [x] Let workloads target one or more pools through validated
  `WORKLOAD_POOLS_JSON` policy.
- [x] Validate missing, empty, misspelled, duplicate, or unsupported pool
  references at startup.
- [x] Keep runtime dispatch behavior unchanged until Phase 7B and 7C consume
  the validated policy.

#### Phase 7B: Synchronous Backend Pool Routing

- [x] Route OpenAI-compatible chat and embedding requests through workload-selected
  backend pools.
- [x] Add ordered pool fallback chains, such as local GPU pool -> peer GPU pool.
  Within each pool, existing locality, affinity, resource ranking, and backend
  order choose the concrete backend.
- [x] Keep cloud overflow as a future pool target, not an implemented provider path.
- [x] Add per-pool synchronous metrics for candidate counts, selected pool/backend,
  fallback reason, and capacity pressure.
- [x] Keep explicit workload pool policy permissive for omitted workloads by
  default; Phase 7D adds optional strict startup validation for complete
  workload policy.

#### Phase 7C: Async Worker Pool Placement

- [x] Add pool metadata to registered workers.
- [x] Route async jobs to eligible worker pools before choosing a specific
  worker.
- [x] Apply priority/FIFO ordering only after the workload's eligible worker pools
  are known.
- [x] Add per-pool async metrics for candidate workers, selected worker, and
  no-compatible-pool cases.

#### Phase 7D: Shared Pool Demo and Docs

- [x] Document local DGX pools, peer-node pools, and shared Fairlead deployments.
- [x] Add local demo config that shows sync and async workloads using the same pool
  vocabulary.
- [x] Add optional strict worker pool validation. Default registration remains
  permissive; `STRICT_WORKER_POOLS=true` rejects worker pools not present in
  configured or derived `POOLS_JSON`.
- [x] Add optional strict workload pool validation. Default explicit
  `WORKLOAD_POOLS_JSON` remains a partial override;
  `STRICT_WORKLOAD_POOLS=true` requires explicit policy for every known workload
  at startup.
- [x] Audit strict pool validation test coverage and add immediate edge tests.
- [x] Update deferred e2e plans for process-level strict pool startup,
  registration, DGX Spark smoke tests, and future cloud overflow pools.

### Phase 8: Scheduler Hardening

Goal: make the async job manager more operationally complete without adding new
workload protocols.

- **8A Bowline: Worker Lifecycle**
  - [x] Add worker drain/reactivate endpoints.
  - [x] Add worker deregistration API.
  - [x] Idle deregistration removes workers immediately.
  - [x] Busy deregistration marks workers draining so held leases can finish.
  - [x] Preview and claim skip draining workers for new work.
  - [x] Audit worker lifecycle edge tests and deferred e2e coverage.
  - [x] Add final 8A docs/readiness pass before PR.
- **8B Stopper: Retention And Pruning**
  - [x] Add completed-job pruning policy.
  - [x] Add configurable retention limits.
  - [x] Add SQLite pruning behavior and metrics.
  - [x] Audit test coverage and deferred process-level pruning tests before PR.
  - [x] Add final 8B docs/readiness pass before PR.
- **8C Splice: Idempotency**
  - [x] Add optional `idempotency_key` for async job submission retries.
  - [x] Persist submit idempotency keys in SQLite-backed job state.
  - [x] Release submit idempotency keys when terminal jobs are pruned.
  - [x] Make repeated cancellation idempotent for already-cancelled jobs while
    preserving conflict responses for completed or failed jobs.
  - [x] Add optional worker-reported `attempt` to complete/fail requests.
  - [x] Store terminal attempt metadata for completed and terminally failed
    worker attempts.
  - [x] Make exact duplicate terminal complete/fail reports idempotent when
    worker ID, attempt, and result/error payload match.
  - [x] Preserve conflict responses for contradictory terminal result reports.
  - [x] Review callback idempotency and keep the at-least-once receiver contract
    documented.
  - [x] Audit test coverage and deferred process-level e2e cases before PR.
  - [x] Add final 8C docs/readiness pass before PR.
- **8D Clove: Background Maintenance Loops**
  - [x] Add a configurable background lease expiry/recovery loop using the same
    sweep path as worker claims.
  - Add optional background pruning loop that invokes the 8B terminal-job
    pruning policy on a configured interval.
  - Keep explicit `POST /v1/jobs/prune` as the operator/manual path even if a
    background pruning loop is enabled later.
- **8E Reef: Process-Level E2E Harness**
  - Add process-level restart e2e harnesses for jobs, leases, callbacks, and
    metrics.
- Keep Temporal deferred unless application workflows need durable multi-step
  orchestration beyond bounded compute jobs.

### Phase 9: Adapter Boundaries and New Workloads

Goal: make Fairlead support useful non-OpenAI-compatible workloads without
polluting the router core with protocol-specific logic.

- Define adapter boundaries for synchronous and async workloads.
- Add one simple synchronous adapter first, such as rerank.
- Add one concrete async adapter next, such as vision analysis.
- Define adapter payload/result conventions and validation boundaries.
- Add adapter-specific tests and demos.
- Keep HTTP as the first transport unless the adapter contract proves it needs
  typed RPC.

### Phase 10: Rich Resource Policy

Goal: schedule with resource dimensions that real workloads need once pools and
adapters exist.

- Add CPU slots, GPU slots, model residency, disk bandwidth, or worker-specific
  custom capacity where workloads justify them.
- Add per-workload resource estimates and timeout policy.
- Add richer retry and fallback policies that account for resource pressure.
- Add metrics that explain resource rejection and fallback by dimension.

### Phase 11: External Scale and Overflow

Goal: support larger deployments and optional provider overflow after local/edge
placement is stable.

- Add a shared job store or coordinator, such as Postgres, if multiple Fairlead
  instances need to coordinate jobs.
- Add cloud-provider pools if local/edge deployments need external overflow.
- Add provider credential/header policy.
- Add cost, priority, and admission policy for cloud overflow.
- Add deployment documentation for multiple applications sharing Fairlead.

### Phase 12: Transport and SDK Hardening

Goal: stabilize client and worker ergonomics after the HTTP contracts have
settled.

- Add optional gRPC service definitions for stable job, worker, and callback
  contracts if HTTP/JSON starts limiting typed client development.
- Generate Rust and Python clients for Fairlead's stable APIs.
- Support gRPC worker/backend adapters where workers expose typed RPC services.
- Keep HTTP/OpenAI-compatible endpoints as the canonical LLM compatibility
  surface.
- Add parity tests so HTTP and gRPC APIs produce the same scheduling and job
  state transitions when both are supported.
