# Fairlead — Architecture and Implementation Guide

---

## The complete system

Five components, each owning a distinct layer:

```
User
  ↓  HTTPS
Verdant          React frontend — dashboard, chat, triage, tasks
  ↓  HTTPS /api/v1
Cambium          Go API gateway — auth, provider key management, SSE proxy
  ↓  HTTP /internal/agent (SSE)
Rhizome          Python LangGraph agent — 93 tools, planning, triage, DB
  ↓  HTTP /v1/chat/completions (OpenAI-compatible)
Fairlead         Rust compute router — inference routing, resource admission, circuit breaking
  ↓  HTTP /v1
vLLM             Python model server — GPU inference on spark-a
```

Future phases may add async job dispatch and cloud-provider fallback, but those
are not part of the current synchronous proxy surface.

**Cambium** handles everything external-facing: verifies JWT tokens, decrypts the
user's stored API key, injects it into the Rhizome request. It knows about users
and sessions. It does not know about gardens.

**Rhizome** is the domain engine. It runs a LangGraph graph: session context →
weather → triage → LLM → tools → loop. It knows about plants, projects, tasks,
incidents. It does not know which GPU served its last LLM call.

**Fairlead** is a compute infrastructure layer. Today it receives
OpenAI-compatible synchronous requests. Its planned async surface will also
receive job submissions. It knows about nodes, reported VRAM/load, circuit
states, priority admission, and future job queues. It does not know what a
garden is.

**vLLM** serves the model. It knows about GPU memory, batching, and
PagedAttention. It knows nothing above itself.

---

## Mental model: vLLM vs Fairlead

Fairlead is not an inference runtime. It does not load model weights, run CUDA
kernels, implement attention, tokenize prompts, or produce tokens directly. It is
the request gateway and control-plane layer in front of inference runtimes.

vLLM is the data-plane inference server. A vLLM process owns:

- Loading model weights into GPU memory.
- Tokenization and request execution.
- KV cache management.
- Continuous batching.
- GPU kernel execution through CUDA/PyTorch.
- Streaming OpenAI-compatible responses.
- Tensor parallelism when configured.

Fairlead owns routing and operational policy around those inference servers. It
can decide:

- Which backend URL should receive a request.
- Whether a backend should be skipped because its circuit is open.
- Whether a thread should prefer the same backend for cache locality.
- Whether a request should be rejected, retried, or routed elsewhere.
- What metrics and traces should describe the routing decision.

That means Fairlead is a control-plane gateway, while vLLM is the model-serving
data plane:

```text
client or agent
  -> Fairlead decides where work should go
  -> vLLM runs the model on GPU
  -> Fairlead streams the response back
```

### GPU resource management

Fairlead does not manage GPU memory at the CUDA allocator level. vLLM and other
GPU workers manage their own memory internally. Fairlead's role is admission
control: decide where work is allowed to go based on reported capacity, health,
priority, and current load.

There are three separate layers:

| Layer | Owner | Example responsibility |
|---|---|---|
| GPU execution | vLLM / CUDA / PyTorch | Load weights, allocate KV cache, run kernels |
| Process placement | k3s / Docker / systemd | Start, stop, restart, and place containers |
| Request admission | Fairlead | Pick backend, skip unhealthy nodes, avoid oversubscribed resources |

The current VRAM/load accounting model is cooperative. GPU consumers report
their resource use to Fairlead, and Fairlead uses that control-plane view to
avoid sending new work to nodes without enough reported headroom.

Example:

```text
node: spark-a
  total_vram_mb: 24576
  registered_consumers:
    - vllm-llama: 18432 MB
    - vision-worker: 6144 MB
  schedulable_headroom: 0 MB
```

Fairlead does not infer this from CUDA directly in the current design. It relies
on workers and model servers reporting capacity, or on future node probes that
publish capacity into Fairlead's registry. This is why the resource registry is a
scheduler input, not the source of truth for GPU execution.

The current code implements the synchronous gateway path: backend selection,
circuit breaking, health probing, soft affinity, streaming proxying, resource
reporting, resource-aware backend eligibility, priority admission, and basic
metrics. The early async path implements in-memory job records and
non-dispatching worker registration, queue metrics, and scheduler preview.
Worker-pull claims, worker execution, leases, callbacks, and durable job
recovery are planned for Phase 6C+.

### Rhizome example: spark-a and spark-b

Assume a deployment with two GPU nodes:

- `spark-a`: runs a Rhizome worker and a local vLLM server.
- `spark-b`: runs a Rhizome worker and a local vLLM server.
- Fairlead can run as one shared service, or as a local sidecar per node. The
  routing policy is easiest to reason about as one logical Fairlead control
  plane.

```mermaid
flowchart LR
    cambium["Cambium API gateway"] --> rhizome_a["Rhizome worker on spark-a"]
    cambium --> rhizome_b["Rhizome worker on spark-b"]

    rhizome_a --> fairlead["Fairlead routing/control plane"]
    rhizome_b --> fairlead

    fairlead --> vllm_a["vLLM on spark-a GPU"]
    fairlead --> vllm_b["vLLM on spark-b GPU"]

    vllm_a --> gpu_a["spark-a GPU"]
    vllm_b --> gpu_b["spark-b GPU"]

    fairlead --> registry["Resource registry health + VRAM + load + locality"]
    registry -. informs .-> fairlead
```

The desired routing behavior is locality-aware and resource-aware:

```text
Rhizome request starts on spark-a
  -> prefer vLLM on spark-a, because same-node traffic should have lower latency
  -> if spark-a vLLM is unhealthy, overloaded, or lacks reported GPU headroom,
     route to spark-b vLLM
  -> if both local GPU backends are unavailable or full, return 503 today
     (future phases may queue or use cloud fallback)
```

The same applies in reverse for a Rhizome request that starts on spark-b:

```text
Rhizome request starts on spark-b
  -> prefer vLLM on spark-b
  -> if spark-b is unavailable or full, route to spark-a
  -> if both are unavailable or full, return 503 today
     (future phases may queue or use cloud fallback)
```

To make this work, Fairlead uses metadata that was added across the generalized
proxy and Trim work:

- **Backend node identity:** each backend must know whether it lives on `spark-a`,
  `spark-b`, or another node.
- **Request origin identity:** Rhizome or the local Fairlead sidecar must provide
  where the request originated, for example `X-Fairlead-Origin-Node: spark-a`.
- **Resource state:** vLLM and other GPU consumers must report usable capacity,
  reserved VRAM, current load, or an equivalent schedulable signal.
- **Workload metadata:** chat completions, embeddings, vision jobs, and batch
  jobs may have different resource estimates and priority rules.
- **Queue policy:** current synchronous requests fail fast by priority-admission
  limit; future background jobs can queue longer and yield to realtime work.

With those inputs, the router can make a decision like:

```text
candidates = all backends serving the requested model/workload
candidates = remove backends with open circuits
candidates = remove backends without enough reported capacity
prefer backend on request origin node
then prefer existing session affinity
then prefer lower load / higher free capacity
if none eligible:
  return 503 now; future phases may queue or fall back to cloud by workload policy
```

Current Fairlead implements locality-aware routing and an opt-in
resource-aware slice. Backends can carry `node_id` metadata, requests can carry
`X-Fairlead-Origin-Node`, and workers/backends can report resource state through
`/v1/resources/report`. When `RESOURCE_AWARE_ROUTING=true`, the router skips
backends without a fresh report that satisfies the workload's coarse VRAM
estimate, applies locality and affinity precedence, then ranks remaining
eligible candidates by lower reported load, higher available VRAM, and
configured order. It does not yet implement backend-pool selection, durable
queueing, or cloud fallback.

---

## What Fairlead does

Two distinct surfaces.

### Surface 1: Synchronous inference proxy

```
POST /v1/chat/completions   — receive, select backend, proxy, stream back
POST /v1/embeddings         — same pattern for embedding requests
GET /v1/models              — list configured backend/model metadata
```

Every inference request goes through a decision pipeline:

```
request arrives
  → check priority (realtime / batch / background)
  → acquire a synchronous admission slot for that priority, or return 429
  → find eligible backends (circuit closed + VRAM headroom)
  → check session affinity (prefer same node for KV cache)
  → proxy to selected backend, stream response back
  → if backend fails: try next in fallback chain
  → if all eligible backends fail: return 502/503
```

### Surface 2: Async job dispatch

Phase 6B implements the HTTP job surface, non-dispatching worker registration,
queue visibility, and scheduler preview with in-memory state. Phase 6C adds the
first worker-pull claim path: Fairlead grants a bounded lease, marks the job
running, and returns it to the worker. Fairlead also opportunistically requeues
expired running leases before fresh workers claim more work. Worker execution,
persistence, and callback delivery are later Phase 6 work. The design belongs
in the architecture because it defines Fairlead's boundary: Fairlead should be a
compute control plane, not a general-purpose workflow engine.

```
POST /v1/jobs        — submit, get job_id immediately
GET  /v1/jobs        — list in-memory job records
GET  /v1/jobs/{id}   — poll status
DELETE /v1/jobs/{id} — cancel queued or running work when supported
GET  /v1/scheduler/preview    — preview next job/worker match without mutation
POST /v1/workers/register       — register or update worker capabilities
POST /v1/workers/{id}/heartbeat — refresh worker liveness
POST /v1/workers/{id}/claim     — lease a compatible queued job to a worker
POST /v1/workers/{worker_id}/jobs/{job_id}/renew — renew a held lease
GET  /v1/workers                — list registered workers
```

Current Phase 6B/6C behavior:

- submitted jobs enter `queued`
- submitted jobs are tracked in in-memory per-priority queues
- `GET /v1/jobs` lists current in-memory job records
- `/metrics` exposes `fairlead_job_queue_depth{priority,type}`
- `/metrics` exposes `fairlead_job_queue_wait_seconds_sum{priority,type}` and
  `fairlead_job_queue_wait_seconds_max{priority,type}`
- job state is in memory and is lost on process restart
- cancellation marks queued jobs `cancelled`
- cancellation removes queued jobs from queue depth and wait-time accounting
- registered workers are listed with stale status
- `/metrics` exposes `fairlead_workers{type,status}`
- `GET /v1/scheduler/preview` selects the next queued job and fresh compatible
  worker without changing job state
- `POST /v1/workers/{id}/claim` validates the worker, grants a lease for a
  compatible queued job, marks it `running`, and removes it from queue metrics
- worker claims first sweep expired leases: expired jobs reenter their priority
  queue if attempts remain, otherwise they become `failed`
- `POST /v1/workers/{worker_id}/jobs/{job_id}/renew` validates the worker,
  sweeps expired leases, and extends the lease only if that worker still holds
  the running job
- no job is dispatched to a worker yet
- no callback is delivered yet
- no durable queue or background scheduler loop exists yet

Future Phase 6C+ behavior after lease expiry and execution support:

```
job submitted
  → queued by priority (realtime > batch > background)
  → scheduler selects a registered worker with matching job type + VRAM headroom
  → Fairlead records a bounded lease for the running attempt
  → worker processes async
  → worker completes or renews the job lease before it expires
  → callback fires to caller on completion
```

**Job types:**
- `vision_analysis` — route to vision MCP sidecar on spark-a GPU
- `embed_batch` — batch embedding generation
- `index_build` — pgvector or FAISS index construction
- `cluster` — k-means or HDBSCAN over an embedding space

**Supporting infrastructure:**
- `POST /v1/workers/register` — workers announce capabilities, endpoint, and
  node metadata
- `POST /v1/workers/{id}/heartbeat` — workers refresh liveness
- `GET /v1/workers` — current in-memory worker registry
- `POST /v1/workers/{id}/claim` — Phase 6C worker-pull claim endpoint
- `POST /v1/workers/{worker_id}/jobs/{job_id}/renew` — Phase 6C lease renewal
- `POST /v1/resources/report` — GPU consumers and workers report capacity
- `GET /v1/resources` — current resource control-plane state
- `GET /metrics` — Prometheus: queue depth/wait, circuit states, VRAM per node
- Persistent job state for running attempts, retries, callbacks, and pruning.

### Worker-pull claim decision

Phase 6C uses a worker-scoped claim endpoint:

```text
POST /v1/workers/{id}/claim
```

This is a worker-pull API shape, but it does not make workers the scheduler.
Workers only announce readiness by asking for work. Fairlead remains the central
controller: it validates the worker, checks freshness and capabilities, scans
queued jobs by priority/FIFO order, applies lease/resource policy, and either
returns a leased job or returns no work.

The alternative would be a job-scoped claim endpoint such as:

```text
POST /v1/jobs/claim
{ "worker_id": "vision-a" }
```

That can work, but it treats claiming as a generic queue operation and pushes
worker authority into the request body. The worker-scoped route makes the actor
explicit and keeps the lifecycle clear:

```text
register -> heartbeat -> claim -> execute -> complete/fail
```

This choice has useful downstream consequences:

- Worker identity, freshness, auth, and draining all attach naturally to the
  route.
- Fairlead can enforce that only the worker holding a lease may renew,
  complete, or fail that job.
- Worker utilization metrics can count claims and in-flight leases per worker.
- Backpressure remains simple: a worker asks only when ready, while Fairlead
  still controls assignment.
- Workers do not need to expose externally reachable HTTP dispatch endpoints for
  Fairlead to push jobs.

The internal implementation may still live in scheduler/job modules and must
mutate job state atomically. The route shape describes the actor; it does not
weaken Fairlead's central scheduling authority.

### Future gRPC transport

Fairlead currently exposes HTTP/JSON APIs and forwards synchronous LLM requests
to OpenAI-compatible HTTP backends such as vLLM. That should remain the default
compatibility surface because it works with existing OpenAI clients, vLLM, demos,
and simple service-to-service calls.

gRPC can still be useful later as an optional transport layer once the job and
worker contracts stabilize. The important split is:

- inbound client transport: Rhizome or another caller talks to Fairlead
- scheduling core: Fairlead validates, queues, leases, retries, and records jobs
- outbound backend adapter: Fairlead talks to vLLM, a vision worker, or another
  compute service

Possible later shapes:

```text
Rhizome --gRPC--> Fairlead --HTTP/OpenAI--> vLLM
worker  --gRPC--> Fairlead claim/heartbeat/complete APIs
Fairlead --gRPC--> worker service, if that worker exposes typed RPCs
```

This is not Phase 6C scope. Adding gRPC well means defining protobuf contracts,
generating Rust/Python clients, testing HTTP/gRPC parity, deciding streaming
semantics, and preserving OpenAI-compatible HTTP behavior for LLM endpoints.
It fits better in Phase 7 adapter work or a later transport/SDK hardening phase.

### Scheduler boundaries

The scheduler exists because Fairlead can run for days, weeks, or indefinitely
as a service while individual compute jobs should stay bounded. If an
image-processing job runs longer than a few minutes, that is probably an
execution failure, not a normal workflow that needs durable multi-day
orchestration.

Layer ownership should stay split this way:

```text
k3s / Docker:
  run containers
  restart crashed services
  expose services
  apply node labels and GPU placement constraints

Fairlead:
  accept compute jobs
  queue by priority
  select workers/backends by health, node, workload, and resources
  track job attempts
  enforce timeouts and leases
  retry failed compute attempts
  expose job status
  deliver callbacks

Rhizome:
  own garden/user/domain state
  create VisionJob records
  interpret model results
  create incidents, interactions, and user-visible records
  reconcile pending domain work

Temporal, if added later:
  durable multi-step workflow orchestration
  long waits
  fanout/fanin
  cross-service retries and compensation
  recovery of product workflows after crashes
```

Fairlead should know where compute can run. It should not know what a plant
diagnosis means or which user-facing record should be created.

### Scheduler state model

The Fairlead scheduler should treat jobs as bounded state machines:

```text
pending
  -> leased/running
  -> succeeded
  -> failed
  -> cancelled
```

Failure paths:

```text
leased/running
  -> timed_out -> retry_pending or failed
  -> worker_lost -> retry_pending or failed
  -> cancelled
```

The key mechanism is a lease. When a worker starts a job, Fairlead records the
worker ID and a `lease_expires_at` timestamp. The worker must complete the job
or renew that job lease before it expires. If it does not, Fairlead releases any
resource reservation and either retries the job on another worker or marks it
failed. This is separate from worker heartbeat, which only proves worker
liveness. Job renewal proves that the leased attempt is still making progress.
This avoids holding an open process relationship for days; Fairlead only stores
job state and watches bounded leases.

The first useful workload is `vision_analysis`: a user-triggered image workflow
that should outrank background indexing but yield to realtime chat or retrieval.

### Scheduler persistence

An in-memory queue is acceptable for a prototype demo, but production-like
Fairlead should persist job state. The scheduler mostly needs durable state
transitions, not a massive distributed datastore.

Core records look relational:

```text
job_id
status
priority
workload_kind
worker_id
attempt_count
lease_expires_at
created_at
updated_at
input_ref
result_ref
error
callback_url
callback_status
```

Common scheduler operations must be atomic:

```text
claim the oldest pending high-priority job
mark it running
increment attempt_count
set lease_expires_at
reserve resources
```

Recommended persistence path:

- **SQLite first:** good for a local appliance, single Fairlead process, demos,
  and portfolio deployment. It is durable, inspectable, transactional, and does
  not require another service.
- **Postgres later:** better when multiple Fairlead instances need to coordinate
  safely. Row locking and transactions map well to job claiming and leases.
- **Redis optionally:** useful for fast queues, pub/sub, rate limits, or caches,
  but less ideal as the canonical job history and recovery database.
- **NATS JetStream or RabbitMQ optionally:** useful when broker semantics become
  more important than direct SQL-backed job state.
- **Kafka/Redpanda:** useful for high-throughput event logs and replay, likely
  overkill for Fairlead's early scheduler.
- **Cassandra:** not a good fit unless Fairlead becomes a massive distributed
  event store. It makes ordering, leases, transactions, and debugging harder than
  this project needs.

### When Temporal becomes worth it

Temporal is useful if Rhizome workflows become more complex than bounded compute
jobs. Signs that Temporal is justified:

- Steps wait for hours or days.
- A workflow fans out to many workers and collects partial results.
- Only failed branches should retry.
- Multiple services must be updated with compensation logic.
- Cancellations can happen halfway through a multi-step workflow.
- Recovery after service restart would otherwise require rebuilding a workflow
  engine inside Rhizome or Fairlead.

Until then, Fairlead should provide compute job orchestration, and Rhizome should
own the domain-level VisionJob state machine.

---

## Priority Admission Now, Priority Queues Later

The core scheduling guarantee:

| Priority | Who uses it | What it means |
|---|---|---|
| `realtime` | Chat completions, query-time embeddings | A user is waiting. Schedule first, always. |
| `batch` | Vision jobs, async embeddings | User submitted and moved on. Schedule when no realtime demand. |
| `background` | Index builds, clustering, KB ingestion | No user waiting. Only run when the GPU has nothing more important to do. |

The long-term goal is that a background index rebuild that started at midnight
will not slow down a user's chat response at 9am. That will be enforced
structurally by the future scheduler, not by application policy.

Current Fairlead parses `X-Fairlead-Priority` on synchronous requests, defaults
missing priority to `realtime`, rejects unknown values with `400`, enforces
per-priority in-flight admission limits, and exposes priority on routing
metrics.

This is not yet a durable priority queue. A full synchronous bucket fails fast
with `429 Too Many Requests`; Fairlead does not currently wait in a queue for
capacity. The async job API exposes in-memory queue depth and wait age, but
starvation policy and async job scheduling remain future scheduler work.

---

## The 7-phase build plan

| Phase | What you build | What it unlocks |
|---|---|---|
| 1 | Axum server, /health, config, tracing | Something that runs |
| 2 | OpenAI proxy, single backend, streaming | Rhizome can call Fairlead instead of cloud |
| 3 | Circuit breaker, health checks, basic /metrics | Automatic failover when vLLM crashes |
| 4 | Fallback chain, session affinity, same-request retry | Local resilience across configured backends |
| 5 | Resource registry, VRAM accounting, priority admission | Synchronous inference avoids oversubscribed local GPUs and fails fast by priority |
| 6A | Synchronous surface cleanup: workload metadata, pool metadata, header policy, `/v1/models` | More synchronous workloads can share the proxy cleanly |
| 6B | Async job API, queue visibility, worker registration, scheduler preview | Fairlead can describe async work and select a compatible worker without dispatching |
| 6C | Worker-pull claims and leases | Workers can safely claim bounded jobs without duplicate execution |
| 6D | Worker execution, retries, and utilization | Leased jobs can complete/fail with bounded attempts and useful metrics |
| 6E | Durable job state and recovery | Queued/running async work survives ordinary Fairlead restarts |
| 6F | Callback delivery and async finalization | Callers can receive terminal job updates without polling forever |
| 7A | Pool-aware routing and placement policies for sync backends and async workers | Workloads can target named local, peer, or overflow compute pools consistently |
| 7 | Adapter boundaries, index/cluster job types, FAISS/GPU, cloud fallback, full metrics | Advanced RAG indexing, external overflow, complete observability |

Phases 1–3 give you a working proxy in a few days. Phases 4–5 give you
production resilience. Phase 6 completes the core sync/async control-plane
surfaces. Phase 7A adds complete pool-aware placement. Phase 7 completes the
advanced compute platform.

Temporal is intentionally deferred. Fairlead's scheduler should handle bounded
compute jobs with leases, retries, cancellation, callbacks, and recoverable job
state. Temporal becomes useful later only if Rhizome needs durable multi-step
business workflows with long waits, fanout/fanin, or compensation logic.

---

## How Rust defines the architecture

Rust's design forces certain architectural decisions that you can't avoid — but
they are the right decisions. Understanding them before writing the first line
prevents a lot of friction.

### 1. Ownership drives shared state design

In Python you write:
```python
backends = {}  # anyone can read/write
```

Rust won't compile that across threads. Every piece of shared mutable state must
be explicit:
```rust
// Arc = shared ownership across threads
// RwLock = many readers OR one writer at a time
type BackendMap = Arc<RwLock<HashMap<String, BackendState>>>;
```

This isn't ceremony — it's the architecture. When you write `Arc<RwLock<...>>`
you're making a deliberate decision: "this data will be read by many concurrent
request handlers and occasionally written by background health check tasks."
The type encodes the concurrency pattern.

Fairlead's shared state objects, each `Arc`-wrapped and cloned into each handler
and background task:
- `Arc<RwLock<BackendMap>>` — backend states and circuit breakers
- `Arc<RwLock<ResourceRegistry>>` — cooperative resource reports per node/backend
- `Arc<RwLock<AffinityMap>>` — workload-scoped thread ID → preferred backend
- `WorkerRegistry` wrapping `Arc<RwLock<...>>` — registered job workers

### 2. The async model requires structure, not just `async fn`

Tokio is cooperative multitasking. A task runs until it hits an `.await` point,
then yields. Blocking work between `.await` points — a long computation, a
`std::thread::sleep`, synchronous file I/O — freezes the entire thread, which in
Tokio may be serving hundreds of concurrent requests.

This forces a clear architecture split.

**Async path** — request handlers, forwarding, queue checks:
```rust
async fn handle_chat_completions(/* ... */) -> Response {
    let backend = router
        .select_backend_excluding_resource(&attempted, &resource_state)
        .await;                                    // async: checks circuit state
    let response = client.post(&backend.url)
        .send().await?;                            // async: network I/O
    Body::from_stream(response.bytes_stream())     // async: stream back
}
```

**Background tasks** — health checks, job scheduler, metrics:
```rust
// Spawned once at startup, run forever
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        probe_backend(&url, &circuit).await;
    }
});
```

The server will have a handful of long-lived background tasks (one per backend
for health checking, one for the job scheduler) and thousands of short-lived
request handler tasks. They all share the `Arc`-wrapped state through clones.

### 3. The type system makes state machines correct

The circuit breaker has three states. In Python you'd use a string. In Rust:

```rust
enum CircuitState {
    Closed,
    Open { until: std::time::Instant },
    HalfOpen,
}
```

Every `match` on this enum must handle all three cases. The compiler will not let
you forget `HalfOpen`. This matters for a circuit breaker because the half-open
state (one probe request to test recovery) is the subtle one — it's exactly what
you'd accidentally skip in a string-based approach.

The same applies to job status, backend health, and priority levels. Everything
with a finite set of states becomes an enum. Exhaustiveness is enforced.

### 4. Future queues use channels instead of shared queue locks

The future async job scheduler should use Tokio channels — one per priority
level — rather than a shared queue data structure with a lock:

```rust
let (realtime_tx, mut realtime_rx) = mpsc::channel::<Job>(256);
let (batch_tx,    mut batch_rx)    = mpsc::channel::<Job>(256);
let (bg_tx,       mut bg_rx)       = mpsc::channel::<Job>(256);
```

The `biased` `select!` enforces priority:
```rust
loop {
    tokio::select! {
        biased;   // evaluate branches in declared order, not randomly
        Some(job) = realtime_rx.recv() => dispatch(job).await,
        Some(job) = batch_rx.recv(),
            if realtime_rx.is_empty() => dispatch(job).await,
        Some(job) = bg_rx.recv(),
            if realtime_rx.is_empty() && batch_rx.is_empty() => dispatch(job).await,
    }
}
```

Without `biased`, Tokio randomizes which ready branch is selected to prevent
starvation. With it, you get strict priority. The queue depth of each channel
is directly observable for Prometheus metrics.

### 5. Send + Sync are the concurrency contract

Rust's trait system expresses thread safety. Any value that needs to cross thread
boundaries must implement `Send`. Any value that can be safely shared by reference
across threads must implement `Sync`.

Tokio requires `Send + 'static` on everything spawned as a task. When you
`tokio::spawn` a health check task that closes over the backend state, Rust
verifies at compile time that the backend state is safe to send across threads.
If it isn't, you get a clear compile error — not a race condition at runtime
six months later.

`Arc<RwLock<T>>` is both `Send` and `Sync` when `T: Send + Sync`. The `Arc`
handles shared ownership; the `RwLock` handles mutation safety; the compiler
verifies the combination is sound.

### 6. Zero-cost abstractions mean the architecture mirrors performance

Rust's abstractions compile to the same machine code as hand-written code.
Generics (static dispatch) have zero overhead; `Box<dyn Trait>` (dynamic
dispatch) has a small indirection cost.

On the hot path (every inference request): use generics. The circuit breaker
check and VRAM query happen for every request — the compiler inlines them.

For the job dispatcher (not hot): trait objects are fine. The flexibility of
dispatching to different worker types at runtime matters more than the
nanoseconds.

---

## The mental model

Fairlead is a tree of async tasks sharing Arc-wrapped state through explicit
synchronized wrappers. A handful of long-lived background tasks — health
checkers, job scheduler, metrics collector — run forever alongside thousands
of short-lived request handler tasks. They communicate through channels, not
shared memory. The type system enforces that all state transitions are
exhaustive. The borrow checker enforces that nothing is mutated unsafely.
The async runtime ensures nothing blocks. The result is a system that is
concurrent by default, thread-safe by construction, and whose performance
characteristics are predictable because nothing is hiding behind a garbage
collector.
