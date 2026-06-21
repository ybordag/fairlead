# Fairlead — Claude Code Memory

## What this project is

Fairlead is a priority-aware compute task router written in Rust. It currently
routes synchronous OpenAI-compatible inference requests across local GPU nodes,
manages circuit breaking and session failover, and tracks cooperative VRAM/load
reports from model servers and other GPU consumers.

It exposes an OpenAI-compatible inference API and a first-slice generic async
job API. The inference path is synchronous (request → response). The job path is
currently submit → job_id → poll/cancel, with non-dispatching worker
registration, scheduler preview, and initial worker-pull claims. Worker
execution, durable state, and callbacks are Phase 6D+ work.

See `docs/planning/design.md` for the design horizon and
`docs/planning/architecture.md` for the current architecture.

## Related repos

- **Rhizome** (Python) — the agent. Points its model client at Fairlead `/v1`.
  Can submit async compute jobs to Fairlead's Phase 6B job API once Rhizome has
  matching job schemas.
- **Cambium** (Go) — the API gateway. Calls Rhizome; Rhizome calls Fairlead.
- **Fairlead** (this repo, Rust) — routes to vLLM on spark-a/spark-b; cloud
  fallback is a future phase.

## Tech stack

- **Language:** Rust (stable, 2021 edition)
- **Async runtime:** Tokio
- **Web framework:** Axum
- **HTTP client:** reqwest (async, streaming support)
- **Serialization:** serde + serde_json
- **Errors:** thiserror (library errors) + anyhow (application handlers)
- **Logging:** tracing + tracing-subscriber
- **Metrics:** Prometheus-compatible (`/metrics`)

## Build and test

```bash
cargo build                          # debug build
cargo build --release                # release build
cargo test                           # run all tests
cargo clippy --all -- -D warnings    # lints — must pass before any commit
cargo fmt --check                    # format check
```

Install `cargo-watch` for development:
```bash
cargo install cargo-watch
cargo watch -x run
```

## Current status

**Phase 6B complete** (tackle → main). **Phase 6C is in progress on cleat**:
worker-pull claims and bounded leases before worker execution, callbacks, and
persistence.

| Phase | Branch | Status |
|---|---|---|
| 1 — Foundation | garboard → main | ✅ complete |
| 2 — Transparent proxy | telltale → main | ✅ complete |
| 3 — Circuit breaker + health | batten → main | ✅ complete |
| 4 — Fallback chain + session affinity | spinnaker → main | ✅ complete |
| 5 — VRAM accounting + priority admission | trim → main | ✅ complete |
| 6A — Synchronous surface cleanup | clew → main | ✅ complete |
| 6B — Async API + scheduler preview | tackle → main | ✅ complete |
| 6C — Worker-pull claims + leases | cleat | in progress |
| 6D — Worker execution + retries | — | pending |
| 6E — Durable job state + recovery | — | pending |
| 6F — Callback delivery + finalization | — | pending |
| 7A — Pool-aware routing + placement | — | pending |
| 7 — Advanced compute + full metrics | — | pending |

## Project layout

**What exists now (Phases 1–6C current slices):**

```
src/
  main.rs           — tokio::main, AppState, build_router(), health probe startup
  config.rs         — Config from env, backend metadata, workload route metadata
  error.rs          — FairleadError enum with IntoResponse impl
  health.rs         — GET /health → {"status":"ok"}
  jobs.rs           — in-memory async job API: submit, list, get, cancel, queue snapshots
  metrics.rs        — GET /metrics → Prometheus circuit_state gauge per backend
  models.rs         — GET /v1/models → configured backend/model metadata
  priority.rs       — per-priority synchronous admission limiter
  resources.rs      — POST/GET resource reports for VRAM/load control-plane state
  scheduler.rs      — non-dispatching scheduler preview: queued job → fresh capable worker
  workers.rs        — non-dispatching worker registration, heartbeat, stale status
  router/
    mod.rs          — module entry point
    circuit.rs      — CircuitBreaker: Closed/Open/HalfOpen state machine
    backend.rs      — BackendState (url + Arc<RwLock<CircuitBreaker>>), spawn_health_probe()
    fallback.rs     — backend selection with origin locality, affinity, and retry exclusions
    affinity.rs     — SessionAffinity: thread_id → backend_index map
  proxy/
    mod.rs          — POST /v1/chat/completions, POST /v1/embeddings; circuit + affinity + fallback
    types.rs        — OpenAI-compatible request/response serde structs
```

**Planned layout (Phases 6–7):**

```
src/
  ...  (Phases 1–4 files as above)

  router/
    ...  (circuit.rs, backend.rs, fallback.rs, affinity.rs as above)
  jobs/
    mod.rs             — async job API: submit, query, dispatch
    queue.rs           — three-tier priority queue (realtime > batch > background)
    worker.rs          — worker registration: job types, VRAM cost, callback URL
    scheduler.rs       — select worker for a job; respect priority and VRAM budget
    types.rs           — job type definitions and payload schemas

  vram/
    mod.rs             — cooperative VRAM accounting across all consumers

  health.rs            — GET /health
  metrics.rs           — GET /metrics (Prometheus)
```

## API surface

### Inference (synchronous)

```
POST /v1/chat/completions    — OpenAI-compatible, streaming supported
POST /v1/embeddings          — OpenAI-compatible embedding generation
GET  /v1/models              — list available backends/models
```

### Async jobs

```
POST   /v1/jobs              — submit a job, returns queued in-memory job record
GET    /v1/jobs              — list in-memory job records
GET    /v1/jobs/{id}         — check status: queued | running | complete | failed
DELETE /v1/jobs/{id}         — cancel a queued job
GET    /v1/scheduler/preview — preview next job/worker match without mutation
POST   /v1/workers/{id}/claim — lease a compatible queued job to a worker
POST   /v1/workers/{worker_id}/jobs/{job_id}/renew — renew a held lease
```

The Phase 6B slices store job records in memory and track explicit per-priority
queued job IDs. The first Phase 6C slice lets fresh workers claim compatible
queued jobs. Claimed jobs become `running`, attempts increment, and lease
metadata is attached. Expired running leases are requeued when attempts remain
and failed when attempts are exhausted. Workers holding a lease can renew it
before expiry. Worker execution, deregistration, callback delivery, worker
utilization metrics, and SQLite-backed persistence are still planned.

Job request body:
```json
{
  "type":         "vision_analysis | embed_batch | index_build | cluster",
  "priority":     "realtime | batch | background",
  "payload":      { ... type-specific ... },
  "callback_url": "http://caller/internal/jobs/{id}/complete"
}
```

### Worker registration

```
POST   /v1/workers/register        — worker announces: job types, endpoint, node
POST   /v1/workers/{id}/heartbeat  — refresh worker liveness
POST   /v1/workers/{id}/claim      — claim a compatible queued job
POST   /v1/workers/{worker_id}/jobs/{job_id}/renew — renew a held lease
GET    /v1/workers                 — list registered workers and their status
```

### VRAM accounting

```
POST   /v1/resources/report     — consumer reports node/backend VRAM and load
GET    /v1/resources            — current resource state per node/backend
```

## Priority model

Every request — inference or job — carries one of three priority levels:

| Priority     | Who uses it                                          | Behavior |
|---|---|---|
| `realtime`   | Chat completions, retrieval queries (user waiting)   | Always scheduled first; preempts lower-priority work |
| `batch`      | Vision analysis, per-request embeddings (user submitted but not blocked) | Scheduled when no realtime demand |
| `background` | Knowledge base ingestion, index rebuilds, clustering | Scheduled only when both higher tiers are idle |

Current synchronous proxy behavior is admission limiting, not queueing. Each
priority has its own in-flight cap. If the cap is full, Fairlead returns `429`
and records `outcome="priority_limited"`.

Future async job scheduling should use three Tokio channels and a scheduler that
always drains higher-priority channels before accepting lower-priority work:

```rust
tokio::select! {
    job = realtime_rx.recv() => schedule(job),
    job = batch_rx.recv(),      if realtime_empty() => schedule(job),
    job = background_rx.recv(), if realtime_empty() && batch_empty() => schedule(job),
}
```

The current limiter guarantees a full lower-priority bucket does not block a
higher-priority bucket. The future queued scheduler should guarantee that a user
is never queued behind a background knowledge base rebuild, even when the GPU is
heavily loaded.

## Job types

### `vision_analysis`
Route to a registered vision worker. Payload: `{ media_path, analysis_type }`.
Priority: `batch` (user submitted, async delivery via SSE card).

### `embed_batch`
Generate embeddings for a list of documents. Payload: `{ texts: [...], model }`.
Priority: `background` for knowledge base ingestion; `realtime` for query-time
retrieval embedding (user is waiting for search results).

### `index_build`
Build or update a vector index (pgvector IVFFlat/HNSW, or FAISS for GPU-accelerated
large-scale indexing). Payload: `{ index_type, table, ... }`.
Priority: `background`. GPU-accelerated with FAISS when available.

### `cluster`
Run k-means or HDBSCAN over an embedding space to produce topic structure.
Payload: `{ table, n_clusters, algorithm }`.
Priority: `background`. CPU or GPU (cuML) depending on available worker.

## Environment variables

Current implementation:

```
PORT                         — listen port (default: 7000)
BACKENDS                     — comma-separated backend URLs in priority order
                               e.g. http://node-a:8000/v1,http://node-b:8000/v1
BACKENDS_JSON                — structured backend config with id, url, node_id,
                               pool, workloads, and health_path
LOG_LEVEL                    — tracing level: error, warn, info, debug, trace (default: info)
LOG_FORMAT                   — "json" for structured JSON logs
CIRCUIT_FAILURE_THRESHOLD    — consecutive failures to open circuit (default: 3)
CIRCUIT_COOLDOWN_SECS        — seconds before half-open probe (default: 30)
HEALTH_PROBE_INTERVAL_SECS   — seconds between background health probes (default: 10)
RESOURCE_REPORT_TTL_SECS     — seconds before a resource report is stale (default: 30)
RESOURCE_AWARE_ROUTING       — enable resource-aware eligibility (default: false)
CHAT_COMPLETIONS_REQUIRED_VRAM_MB — coarse chat request estimate (default: 1024)
EMBEDDINGS_REQUIRED_VRAM_MB  — coarse embedding request estimate (default: 512)
PRIORITY_REALTIME_LIMIT      — max concurrent realtime requests (default: 8)
PRIORITY_BATCH_LIMIT         — max concurrent batch-priority sync requests (default: 4)
PRIORITY_BACKGROUND_LIMIT    — max concurrent background-priority sync requests (default: 2)
```

Planned later phases may add:

```text
CLOUD_PROVIDERS              — JSON array of cloud provider configs
SESSION_AFFINITY             — configurable affinity key policy
JOB_CALLBACK_TIMEOUT_SECS    — time to attempt callback delivery
WORKER_HEARTBEAT_SECS        — interval before a worker is considered stale
```

## Build phases

### Phase 1 — Foundation

- `cargo init`, add core dependencies to `Cargo.toml`
- Axum server with `GET /health` returning `{"status": "ok"}`
- Config loading from environment variables
- Tracing setup (structured JSON logs)
- Test: server starts, `/health` returns 200

### Phase 2 — Transparent proxy

- OpenAI-compatible request/response types (serde structs)
- Single hardcoded backend: receive, forward via reqwest, stream response back
- Streaming responses proxied without buffering (SSE / chunked transfer)
- Test: forward a real `/v1/chat/completions` end-to-end; streaming works

### Phase 3 — Circuit breaker and health checking

- `CircuitBreaker` struct: `Closed` / `Open` / `Half-open` states
- Background health check tasks per backend (`tokio::spawn`)
- Circuit opens after N consecutive failures or timeout threshold
- Half-open probe: one test request; success closes, failure re-opens
- Basic `/metrics` endpoint: `circuit_state` per backend
- Test: circuit opens when backend unreachable; recovers when it comes back

### Phase 4 — Fallback chain and session affinity

- Multiple backends in priority order; try next on circuit-open or retryable error
- `SessionAffinity`: `thread_id → preferred_backend` (`Arc<RwLock<HashMap>>`)
- Soft affinity: prefer known backend, fall back if unavailable or overloaded
- Same-request retry for retryable upstream failures before bytes are streamed
- Test: primary down → secondary; same thread_id routes to same node while available

### Phase 5 — VRAM accounting and priority admission

- `ResourceRegistry`: per-node/backend resource reports with VRAM, load, and TTL
- `POST /v1/resources/report` and `GET /v1/resources`
- Router checks available VRAM before selecting a backend when
  `RESOURCE_AWARE_ROUTING=true`; after locality and affinity, it prefers lower
  reported load and then higher available VRAM
- Inference requests carry a `X-Fairlead-Priority` header (default:
  `realtime`)
- Synchronous proxy requests are admitted through per-priority in-flight limits:
  `realtime`, `batch`, and `background`
- A full synchronous priority bucket returns `429 Too Many Requests`; it does not
  wait in a durable queue
- Priority limit and in-flight gauges are exposed on `/metrics`
- Durable three-tier queues with Tokio channels are deferred to the async job
  scheduler
- Test: backend with insufficient VRAM is skipped; a full batch bucket does not
  block realtime admission

### Phase 6A — Synchronous surface cleanup

- [x] Move route-specific behavior into workload metadata.
- [x] Add route metadata: path, method, streaming behavior, retry policy, backend
  pool, and metric labels.
- [x] Decide whether session affinity is global, per workload, or per backend pool.
- [x] Add provider/header forwarding policy for content type, authorization,
  organization/project headers, and provider-specific opt-in headers.
- [x] Add `GET /v1/models` backed by configured workloads and backend metadata.
- Full pool-aware backend configuration, pool fallback chains, and placement
  policies are deferred to Phase 7A so the design can cover both synchronous
  backends and async workers.
- Keep cloud-provider fallback and provider credentials deferred unless a demo or
  deployment path needs external overflow capacity.

### Phase 6B — Async API, queue visibility, and scheduler preview

- [x] First-slice in-memory job API: `POST /v1/jobs`,
  `GET /v1/jobs/{id}`, `DELETE /v1/jobs/{id}`
- [x] Queue visibility: `GET /v1/jobs`, per-priority queue state, and queue
  depth/wait-time metrics
- [x] Non-dispatching worker registration, heartbeat, stale status, and
  availability metrics
- [x] Non-dispatching scheduler preview: next queued job by priority/FIFO order
  matched to a fresh worker with the required job type
- [x] Keep preview non-mutating: no lease, no `running` transition, no worker
  call, and no callback

### Phase 6C — Worker-pull claims and leases

- [x] Worker-pull claim endpoint.
- [x] Mark selected jobs `running` only when a lease is granted.
- [x] Store lease metadata: worker ID, lease expiry, attempt number, and
  claimed-at timestamp.
- [x] Prevent duplicate claims for the same job.
- [x] Initial cancellation semantics for queued and running jobs.
- [x] Requeue expired leases when attempts remain.
- [x] Mark expired leases failed when attempts are exhausted.
- [x] Let the worker holding a running lease renew it before expiry.

### Phase 6D+ — Remaining async compute router work

- Worker deregistration API and graceful shutdown semantics.
- Worker utilization metrics.
- Scheduler policy based on lease availability, VRAM headroom, and load.
- Job manager: bounded attempts, leases, timeouts, retry limits, cancellation,
  and completed-job pruning.
- Persistence path: SQLite first for local durable state, Postgres later for
  multiple Fairlead instances.
- Callback delivery: on completion, POST to `callback_url` with result payload;
  retry on failure.
- Job duration and callback success/failure metrics.
- Test: submit vision job → claimed by registered worker → completed → callback
  fires with result.

Temporal is deferred. Fairlead owns compute job orchestration; Rhizome owns
domain workflow state. Add Temporal only if Rhizome starts needing durable
multi-step workflows with long waits, fanout/fanin, or compensation logic.

### Phase 7 — Advanced compute jobs and full metrics

- `index_build` job type: pgvector (CPU) and FAISS (GPU) backends
- `cluster` job type: k-means and HDBSCAN via registered compute worker
- Adapter boundaries for non-OpenAI-compatible synchronous and async endpoints,
  such as rerank, image generation, and vision analysis
- GPU-aware job scheduling: prefer GPU worker for index/cluster; fall back to CPU worker
- Richer resource dimensions beyond coarse VRAM/load when needed: CPU slots, GPU
  slots, model residency, disk bandwidth, or custom worker capacity
- Cloud-provider fallback and credential policy if local/edge deployment needs
  external overflow capacity
- Full Prometheus `/metrics`: requests_total, queue_depth per priority, job_duration,
  worker_utilization, vram_used per node, circuit_state per backend
- Test: index_build job completes and callback fires; metrics reflect job throughput

## Invariants — never violate

- **Never block the async runtime.** No `std::thread::sleep`, no synchronous I/O,
  no `std::sync::Mutex` held across `.await` points. Use `tokio::time::sleep`,
  async I/O, and `tokio::sync::RwLock`/`Mutex`.

- **All shared state behind Arc.** Anything accessed across Tokio tasks must be
  wrapped in `Arc<RwLock<T>>` or `Arc<Mutex<T>>`. Document the choice at the call
  site: RwLock for read-heavy state (backends, affinity map); Mutex for write-heavy
  (queue counters, circuit state transitions).

- **Circuit breaker state is the routing source of truth.** Always check circuit
  state before selecting a backend. Never route to an open circuit.

- **Priority is always respected.** Current synchronous requests use separate
  in-flight caps per priority. The future queued scheduler must never dispatch a
  `background` job while a `realtime` request is waiting.

- **VRAM accounting is cooperative.** Fairlead cannot read GPU memory directly.
  When resource-aware routing is enabled, backends without fresh reports are
  ineligible. Unreported external consumers are still invisible to accounting,
  so document that limitation and encourage registration.

- **Fairlead is application-agnostic.** Job payloads are opaque to Fairlead's
  routing logic. It does not know what `vision_analysis` means, what a plant is,
  or what a `thread_id` represents. It routes jobs to workers. Keep it that way.

- **Future jobs do not store domain results.** Fairlead should hold job status
  (`queued`, `running`, `complete`, `failed`) and fire the callback. It should
  not persist application results; the caller's callback handler is responsible
  for storing them.

- **Streaming responses must be proxied without buffering.** Use reqwest's
  streaming body and Axum's `StreamBody`. Collecting the full response body
  before forwarding defeats streaming and blocks the async runtime.

- **`cargo clippy --all -- -D warnings` must pass before every commit.**

## Rust patterns used in this project

**Shared mutable state across async tasks:**
```rust
use std::sync::Arc;
use tokio::sync::RwLock;

type BackendMap = Arc<RwLock<HashMap<String, BackendState>>>;

// Writer
backends.write().await.insert(url, state);

// Reader (many concurrent readers allowed)
let state = backends.read().await.get(&url).cloned();
```

**Priority queue with Tokio channels:**
```rust
let (realtime_tx, mut realtime_rx) = mpsc::channel::<Job>(256);
let (batch_tx,    mut batch_rx)    = mpsc::channel::<Job>(256);
let (bg_tx,       mut bg_rx)       = mpsc::channel::<Job>(256);

// Scheduler loop — always drain higher priority first
loop {
    tokio::select! {
        biased;  // evaluate branches in order, not randomly
        Some(job) = realtime_rx.recv() => dispatch(job).await,
        Some(job) = batch_rx.recv(),
            if realtime_rx.is_empty() => dispatch(job).await,
        Some(job) = bg_rx.recv(),
            if realtime_rx.is_empty() && batch_rx.is_empty() => dispatch(job).await,
    }
}
```

**Background health check task:**
```rust
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        let ok = probe(&backend_url).await.is_ok();
        circuit.write().await.record(ok);
    }
});
```

**Streaming proxy:**
```rust
let upstream = reqwest_client
    .post(&backend_url)
    .json(&body)
    .send()
    .await?;

let stream = upstream.bytes_stream();
Ok(Response::new(Body::from_stream(stream)))
```

**Error propagation:**
```rust
// Library errors: thiserror for precise types
#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("no backend available (all circuits open or insufficient VRAM)")]
    NoBackend,
    #[error("job type '{0}' has no registered worker")]
    NoWorker(String),
}

// Handler errors: convert to HTTP status
impl IntoResponse for RouterError {
    fn into_response(self) -> Response {
        let status = match self {
            RouterError::NoBackend => StatusCode::SERVICE_UNAVAILABLE,
            RouterError::NoWorker(_) => StatusCode::BAD_REQUEST,
        };
        (status, self.to_string()).into_response()
    }
}
```
