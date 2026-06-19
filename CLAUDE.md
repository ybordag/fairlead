# Fairlead — Claude Code Memory

## What this project is

Fairlead is an OpenAI-compatible inference router written in Rust. It routes LLM
requests across local vLLM nodes and cloud providers, manages circuit breaking and
session failover, and tracks VRAM consumption across GPU consumers.

See `design.md` for the full architecture and open design questions.

## Related repos

- **Rhizome** (Python) — the agent. Points its model client at Fairlead's `/v1` endpoint.
- **Cambium** (Go) — the API gateway. Calls Rhizome; Rhizome calls Fairlead.
- **Fairlead** (this repo, Rust) — routes requests to vLLM on Loki/Thor or cloud fallback.

## Tech stack

- **Language:** Rust (stable, 2021 edition)
- **Async runtime:** Tokio
- **Web framework:** Axum
- **HTTP client:** reqwest (async, streaming support)
- **Serialization:** serde + serde_json
- **Errors:** thiserror (library errors) + anyhow (application errors)
- **Logging:** tracing + tracing-subscriber
- **Metrics:** Prometheus-compatible via axum-prometheus or manual implementation

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
cargo watch -x run                   # restart on file change
```

## Project layout (planned)

```
src/
  main.rs              — entry point: parse config, init tracing, start server
  config.rs            — configuration from environment variables
  error.rs             — error types (thiserror)
  router/
    mod.rs             — select backend for incoming request
    backend.rs         — backend node: URL, health state, VRAM state, active requests
    circuit.rs         — per-node circuit breaker (Closed / Open / Half-open)
    affinity.rs        — session affinity: prefer same node for KV cache warmth
    fallback.rs        — ordered fallback chain: try next on failure
  proxy/
    mod.rs             — forward request to selected backend, stream response back
    types.rs           — OpenAI-compatible request/response structs (serde)
  vram/
    mod.rs             — cooperative VRAM accounting: register, release, query
  worker/
    mod.rs             — agent worker pool: spawn, restart, route least-busy
  health.rs            — GET /health handler
  metrics.rs           — GET /metrics handler (Prometheus)
```

## Environment variables

```
PORT                    — listen port (default: 7000)
BACKENDS                — comma-separated list of backend URLs in priority order
                          e.g. http://loki:8000/v1,http://thor:8000/v1
CLOUD_PROVIDERS         — JSON array of cloud provider configs (url, api_key env var)
CIRCUIT_FAILURE_THRESHOLD  — consecutive failures to open circuit (default: 3)
CIRCUIT_COOLDOWN_SECS   — seconds before half-open probe (default: 30)
SESSION_AFFINITY        — "thread" | "user" (default: "thread")
LOG_LEVEL               — tracing level: error, warn, info, debug, trace (default: info)
```

## Recommended build order

### Phase 1 — Foundation

- `cargo init`, add dependencies to `Cargo.toml`
- Axum server with `GET /health` returning `{"status": "ok"}`
- Config loading from environment variables
- Tracing setup (structured JSON logs)
- Test: server starts and `/health` returns 200

### Phase 2 — Transparent proxy

- OpenAI-compatible request/response types (`serde` structs for `/v1/chat/completions`)
- Single hardcoded backend: receive request, forward via `reqwest`, stream response back
- Handle streaming responses (SSE / chunked transfer)
- Test: forward a real chat completions request end-to-end; streaming works

### Phase 3 — Circuit breaker and health checking

- `CircuitBreaker` struct with `Closed` / `Open` / `Half-open` states
- Background health check tasks per backend (Tokio `spawn`)
- Circuit opens after N consecutive failures or timeout
- Half-open probe: one request to test recovery
- Test: circuit opens when backend is unreachable; recovers when it comes back

### Phase 4 — Fallback chain and session affinity

- Multiple backends in priority order
- Try next backend on circuit-open or retryable error
- `SessionAffinity` map: `thread_id → preferred_backend_url` (Arc<RwLock<HashMap>>)
- Soft affinity: prefer the known backend, fall back if unavailable
- Cloud provider support (OpenAI, Gemini, Anthropic — same interface as local vLLM)
- Test: primary backend down → falls back to secondary; requests with same thread_id prefer same node

### Phase 5 — VRAM accounting

- `VramRegistry`: `backend_url → Vec<Consumer>` (name, allocated_mb)
- `POST /v1/vram/register` and `DELETE /v1/vram/register/{consumer_id}` endpoints
- Router checks available VRAM before selecting a backend
- Test: backend with insufficient VRAM is skipped; requests route to the node with headroom

### Phase 6 — Worker pool and metrics

- Agent worker pool: spawn N processes, route requests round-robin / least-busy
- Auto-restart crashed workers
- `GET /metrics` Prometheus endpoint: requests_total, active_requests, circuit_state, vram_used
- Test: crashed worker is restarted; metrics update correctly

## Invariants — never violate

- **Never block the async runtime.** No `std::thread::sleep`, no synchronous I/O, no
  `std::sync::Mutex` held across `.await` points. Use `tokio::time::sleep`,
  async I/O, and `tokio::sync::RwLock`/`Mutex`.

- **All shared state behind Arc.** Anything accessed across Tokio tasks must be
  wrapped in `Arc<RwLock<T>>` or `Arc<Mutex<T>>`. Document why you chose RwLock
  vs Mutex at the call site.

- **Circuit breaker state is the routing source of truth.** The router must check
  circuit state before selecting a backend. Never route to an open circuit.

- **VRAM accounting is cooperative.** Fairlead cannot observe GPU memory directly —
  consumers must register. Design the registration protocol to fail safely: an
  unregistered consumer is invisible, so it is better to under-schedule than
  to assume VRAM is available.

- **Fairlead is application-agnostic.** It must not contain knowledge of gardens,
  users, tasks, or any Rhizome domain concept. It routes bytes. Keep it that way.

- **Streaming responses must be proxied without buffering.** Do not collect the
  full response body before forwarding. Use `reqwest`'s streaming body and
  Axum's `StreamBody` or similar to forward chunks as they arrive.

- **`cargo clippy --all -- -D warnings` must pass before every commit.** Clippy
  catches common Rust mistakes. Treat warnings as errors.

## Rust patterns used in this project

**Shared mutable state across async tasks:**
```rust
use std::sync::Arc;
use tokio::sync::RwLock;

type BackendMap = Arc<RwLock<HashMap<String, BackendState>>>;
```

**Background tasks:**
```rust
tokio::spawn(async move {
    loop {
        health_check(&backend).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
});
```

**Error propagation:**
```rust
// In library code: use thiserror
#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("no backend available: {0}")]
    NoBackend(String),
}

// In main/handlers: use anyhow
async fn handler() -> Result<impl IntoResponse, StatusCode> { ... }
```

**Streaming proxy:**
```rust
// Forward reqwest streaming body as Axum response
let stream = response.bytes_stream();
Body::from_stream(stream)
```
