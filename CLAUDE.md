# Fairlead — Claude Code Memory

## What this project is

Fairlead is a priority-aware compute task router written in Rust. It routes LLM
inference requests and async compute jobs across local GPU nodes and cloud providers,
manages circuit breaking and session failover, and tracks VRAM consumption across
all GPU consumers on the cluster.

It exposes an OpenAI-compatible inference API and a generic async job API. The
inference path is synchronous (request → response). The job path is async
(submit → job_id → callback on completion).

See `design.md` for the full architecture.

## Related repos

- **Rhizome** (Python) — the agent. Points its model client at Fairlead `/v1`.
  Submits async compute jobs (vision, embeddings, indexing) to Fairlead `/v1/jobs`.
- **Cambium** (Go) — the API gateway. Calls Rhizome; Rhizome calls Fairlead.
- **Fairlead** (this repo, Rust) — routes to vLLM on spark-a/spark-b or cloud fallback.

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

**Phase 4 complete** (spinnaker → main). **Phase 5 in progress** (trim branch).

| Phase | Branch | Status |
|---|---|---|
| 1 — Foundation | garboard → main | ✅ complete |
| 2 — Transparent proxy | telltale → main | ✅ complete |
| 3 — Circuit breaker + health | batten → main | ✅ complete |
| 4 — Fallback chain + session affinity | spinnaker → main | ✅ complete |
| 5 — VRAM accounting + priority queues | trim | 🔨 in progress |
| 6 — Async job dispatch | — | pending |
| 7 — Advanced compute + full metrics | — | pending |

## Project layout

**What exists now (Phases 1–4):**

```
src/
  main.rs           — tokio::main, AppState, build_router(), health probe startup
  config.rs         — Config from env (PORT, BACKENDS, CIRCUIT_*, HEALTH_PROBE_INTERVAL_SECS)
  error.rs          — FairleadError enum with IntoResponse impl
  health.rs         — GET /health → {"status":"ok"}
  metrics.rs        — GET /metrics → Prometheus circuit_state gauge per backend
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

**Planned layout (Phases 5–7):**

```
src/
  ...  (Phases 1–4 files as above)

  router/
    ...  (circuit.rs, backend.rs, fallback.rs, affinity.rs as above)
    priority.rs        — priority-aware request scheduling (realtime/batch/background)

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
POST   /v1/jobs              — submit a job, returns job_id immediately
GET    /v1/jobs/{id}         — check status: queued | running | complete | failed
DELETE /v1/jobs/{id}         — cancel a queued job
```

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
POST   /v1/workers/register     — worker announces: job types, VRAM cost, endpoint
DELETE /v1/workers/{id}         — deregister (graceful shutdown)
GET    /v1/workers              — list registered workers and their status
```

### VRAM accounting

```
POST   /v1/resources/report     — consumer reports node/backend VRAM and load
GET    /v1/resources            — current resource state per node/backend
```

## Priority queue model

Every request — inference or job — carries one of three priority levels:

| Priority     | Who uses it                                          | Behavior |
|---|---|---|
| `realtime`   | Chat completions, retrieval queries (user waiting)   | Always scheduled first; preempts lower-priority work |
| `batch`      | Vision analysis, per-request embeddings (user submitted but not blocked) | Scheduled when no realtime demand |
| `background` | Knowledge base ingestion, index rebuilds, clustering | Scheduled only when both higher tiers are idle |

Fairlead implements this with three Tokio channels and a scheduler that always
drains higher-priority channels before accepting lower-priority work:

```rust
tokio::select! {
    job = realtime_rx.recv() => schedule(job),
    job = batch_rx.recv(),      if realtime_empty() => schedule(job),
    job = background_rx.recv(), if realtime_empty() && batch_empty() => schedule(job),
}
```

This guarantees a user is never queued behind a background knowledge base rebuild,
even when the GPU is heavily loaded.

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

```
PORT                         — listen port (default: 7000)
BACKENDS                     — comma-separated backend URLs in priority order
                               e.g. http://node-a:8000/v1,http://node-b:8000/v1
CLOUD_PROVIDERS              — JSON array of cloud provider configs (url, api_key_env)
CIRCUIT_FAILURE_THRESHOLD    — consecutive failures to open circuit (default: 3)
CIRCUIT_COOLDOWN_SECS        — seconds before half-open probe (default: 30)
SESSION_AFFINITY             — "thread" | "user" (default: "thread")
PRIORITY_REALTIME_LIMIT      — max concurrent realtime requests (default: 8)
PRIORITY_BATCH_LIMIT         — max concurrent batch jobs (default: 4)
PRIORITY_BACKGROUND_LIMIT    — max concurrent background jobs (default: 2)
JOB_CALLBACK_TIMEOUT_SECS   — time to attempt callback delivery (default: 30)
WORKER_HEARTBEAT_SECS        — interval workers must heartbeat or be deregistered (default: 30)
LOG_LEVEL                    — tracing level: error, warn, info, debug, trace (default: info)
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
- Cloud provider support (OpenAI, Gemini, Anthropic — same OpenAI-compatible interface)
- Test: primary down → secondary; same thread_id routes to same node while available

### Phase 5 — VRAM accounting and priority queues

- `ResourceRegistry`: per-node/backend resource reports with VRAM, load, and TTL
- `POST /v1/resources/report` and `GET /v1/resources`
- Router checks available VRAM before selecting a backend
- Three-tier priority queue with Tokio channels (realtime / batch / background)
- Scheduler drains higher-priority channels before accepting lower-priority work
- Inference requests carry a `X-Fairlead-Priority` header (default: `realtime`)
- Test: backend with insufficient VRAM is skipped; background jobs yield to realtime

### Phase 6 — Async job dispatch

- Job API: `POST /v1/jobs`, `GET /v1/jobs/{id}`, `DELETE /v1/jobs/{id}`
- Worker registration API: `POST /v1/workers/register`, heartbeat, deregister
- Workers declare: job types they handle, VRAM cost per job, endpoint URL
- Scheduler: match job type → registered workers; pick based on VRAM headroom and load
- Job manager: bounded attempts, leases, timeouts, retry limits, cancellation,
  and completed-job pruning
- Persistence path: in-memory for focused tests, SQLite first for local durable
  state, Postgres later for multiple Fairlead instances
- Callback delivery: on completion, POST to `callback_url` with result payload; retry on failure
- Built-in job types: `vision_analysis`, `embed_batch`
- Test: submit vision job → dispatched to registered worker → callback fires with result

Temporal is deferred. Fairlead owns compute job orchestration; Rhizome owns
domain workflow state. Add Temporal only if Rhizome starts needing durable
multi-step workflows with long waits, fanout/fanin, or compensation logic.

### Phase 7 — Advanced compute jobs and full metrics

- `index_build` job type: pgvector (CPU) and FAISS (GPU) backends
- `cluster` job type: k-means and HDBSCAN via registered compute worker
- GPU-aware job scheduling: prefer GPU worker for index/cluster; fall back to CPU worker
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

- **Priority is always respected.** The scheduler must never dispatch a `background`
  job while a `realtime` request is waiting. This is the central guarantee Fairlead
  makes to its callers.

- **VRAM accounting is cooperative.** Fairlead cannot read GPU memory directly.
  Fail safe: treat unregistered consumers as using zero VRAM (invisible to
  accounting). This can cause OOM — document it clearly and encourage registration.
  Never assume VRAM is available if it has not been reported.

- **Fairlead is application-agnostic.** Job payloads are opaque to Fairlead's
  routing logic. It does not know what `vision_analysis` means, what a plant is,
  or what a `thread_id` represents. It routes jobs to workers. Keep it that way.

- **Jobs do not store results.** Fairlead holds job status (`queued`, `running`,
  `complete`, `failed`) and fires the callback. It does not persist results —
  the caller's callback handler is responsible for storing them.

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
