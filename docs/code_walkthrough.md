# Code Walkthrough

This document explains the current Rust code from process startup to proxied
response. It assumes comfort with C-style systems programming, but not much Rust.

Fairlead is currently one Rust binary. Its current behavior is:

```text
start process
  -> read environment config
  -> build shared backend state
  -> start background health probes
  -> register HTTP routes
  -> accept requests
  -> pick an available backend
  -> proxy the request to that backend
  -> stream the response back
```

## Rust Concepts Used Here

### Modules

At the top of `src/main.rs`:

```rust
mod config;
mod error;
mod health;
mod metrics;
mod proxy;
mod router;
```

These lines tell the Rust compiler to compile files or directories with those
names:

- `mod config;` loads `src/config.rs`.
- `mod proxy;` loads `src/proxy/mod.rs`.
- `mod router;` loads `src/router/mod.rs`.

This is closer to declaring compilation units than to C's textual `#include`.

### `use`

`use` brings names into scope:

```rust
use axum::{
    routing::{get, post},
    Router,
};
```

This is not textual inclusion. It is closer to importing names from a namespace.

### `Result`

Many Rust functions return `Result<T, E>`:

```rust
async fn main() -> anyhow::Result<()>
```

That means the function either returns success value `()` or an error. `()` is
Rust's unit value, similar to `void` as a value.

The `?` operator means: if this expression is an error, return that error from
the current function immediately; otherwise unwrap the success value.

```rust
let cfg = config::Config::from_env()?;
```

### Ownership and Cloning

Rust values usually have one owner. If a value needs to be shared across many
async request handlers, Fairlead uses `Arc` and `RwLock`.

- `Arc<T>` is an atomically reference-counted pointer.
- `RwLock<T>` allows many readers or one writer.
- `Arc<RwLock<T>>` means shared mutable state with runtime locking.

This is the central pattern for circuit breakers and affinity maps.

### `async` and `.await`

An `async fn` can pause at `.await` points while waiting for I/O, timers, or
locks. Tokio is the async runtime that runs these tasks.

Examples:

```rust
tokio::net::TcpListener::bind(addr).await?;
backend.circuit.write().await.record_failure();
client.post(&url).send().await
```

These are places where the current task may yield so other tasks can run.

## Startup Path

The program starts in `src/main.rs`.

### 1. Declare Modules

```rust
mod config;
mod error;
mod health;
mod metrics;
mod proxy;
mod router;
```

These make the rest of the source tree available to `main.rs`.

### 2. Define Shared Application State

```rust
#[derive(Clone)]
pub struct AppState {
    pub client: reqwest::Client,
    pub backends: Vec<BackendState>,
    pub affinity: SessionAffinity,
}
```

`AppState` is the state every HTTP handler needs.

- `client` is the reusable outbound HTTP client.
- `backends` is the ordered list of backend URLs and their circuit breakers.
- `affinity` maps thread IDs to preferred backend indexes.

`#[derive(Clone)]` asks Rust to generate a `clone()` implementation. Axum clones
state handles into request handlers. This is cheap here because the expensive
shared parts are internally reference-counted.

### 3. Start Tokio Runtime and Enter `main`

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
```

The `#[tokio::main]` attribute generates normal synchronous startup code that
creates a Tokio runtime, then runs this async `main` inside it.

### 4. Read Configuration

```rust
let cfg = config::Config::from_env()?;
```

This calls `Config::from_env()` in `src/config.rs`. That function reads
environment variables:

- `PORT`
- `LOG_LEVEL`
- `LOG_FORMAT`
- `BACKENDS`
- `CIRCUIT_FAILURE_THRESHOLD`
- `CIRCUIT_COOLDOWN_SECS`
- `HEALTH_PROBE_INTERVAL_SECS`

`BACKENDS` is parsed as a comma-separated list:

```text
http://loki:8000/v1,http://thor:8000/v1
```

If parsing fails, `?` returns the error and the process exits.

### 5. Initialize Tracing

```rust
init_tracing(&cfg);
```

This configures logging. The `&cfg` syntax is a borrowed reference. It lets
`init_tracing` read the config without taking ownership of it.

### 6. Create the Outbound HTTP Client

```rust
let client = reqwest::Client::new();
```

`reqwest::Client` is Fairlead's reusable client for sending requests to backend
model servers. It is analogous to a reusable HTTP connection manager.

### 7. Build Backend State Objects

```rust
let backends: Vec<BackendState> = cfg
    .backends
    .iter()
    .map(|url| {
        BackendState::new(
            url.clone(),
            cfg.circuit_failure_threshold,
            Duration::from_secs(cfg.circuit_cooldown_secs),
        )
    })
    .collect();
```

This transforms each configured URL string into a `BackendState`.

In C-like pseudocode:

```c
BackendState backends[MAX];
for each url in cfg.backends {
    backends.push(BackendState_new(
        copy(url),
        cfg.circuit_failure_threshold,
        seconds(cfg.circuit_cooldown_secs)
    ));
}
```

`BackendState::new` creates:

```rust
pub struct BackendState {
    pub url: String,
    pub circuit: Arc<RwLock<CircuitBreaker>>,
}
```

Each backend has its own circuit breaker.

### 8. Spawn Background Health Probes

```rust
for b in &backends {
    spawn_health_probe(
        b.circuit.clone(),
        b.url.clone(),
        client.clone(),
        Duration::from_secs(cfg.health_probe_interval_secs),
    );
}
```

`for b in &backends` borrows the backend list. It does not move the backends.

For each backend, Fairlead starts a background Tokio task. The task periodically
sends `GET` to the backend URL. If the request succeeds, it records circuit
success. If it fails to connect, it records circuit failure.

The important part is that `b.circuit.clone()` clones the `Arc`, not the circuit
breaker itself. The health probe and request handlers share the same circuit
state.

### 9. Build `AppState`

```rust
let state = AppState {
    client,
    backends,
    affinity: SessionAffinity::default(),
};
```

This packages the HTTP client, backend list, and empty affinity map into one
value that Axum can pass to handlers.

### 10. Build the HTTP Router

```rust
let app = build_router(state);
```

`build_router` registers HTTP routes:

```rust
Router::new()
    .route("/health", get(health::health))
    .route("/metrics", get(metrics::metrics))
    .route("/v1/chat/completions", post(proxy::chat_completions))
    .route("/v1/embeddings", post(proxy::embeddings))
    .with_state(state)
```

This means:

- `GET /health` calls `health::health`.
- `GET /metrics` calls `metrics::metrics`.
- `POST /v1/chat/completions` calls `proxy::chat_completions`.
- `POST /v1/embeddings` calls `proxy::embeddings`.

### 11. Bind a TCP Listener

```rust
let addr: SocketAddr = format!("0.0.0.0:{}", cfg.port).parse()?;
let listener = tokio::net::TcpListener::bind(addr).await?;
```

This builds a listen address from the port and binds a TCP socket.

### 12. Serve Forever

```rust
axum::serve(listener, app).await?;
```

This starts accepting HTTP connections. In normal operation this line does not
return until the server shuts down or hits a fatal error.

## Request Path

Assume a client sends:

```text
POST /v1/chat/completions
X-Fairlead-Thread-Id: thread-123

{... OpenAI-compatible JSON ...}
```

### 1. Axum Calls the Route Handler

Because `build_router` registered:

```rust
.route("/v1/chat/completions", post(proxy::chat_completions))
```

Axum calls:

```rust
pub async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(&state, "chat/completions", &headers, body).await
}
```

The parameters are Axum extractors:

- `State(state)` extracts a clone of `AppState`.
- `headers` contains request headers.
- `body` contains raw request bytes.

The function immediately calls `forward`.

`POST /v1/embeddings` works the same way, except it passes `"embeddings"` as the
backend path.

### 2. Reject if No Backends Are Configured

```rust
if state.backends.is_empty() {
    return (StatusCode::SERVICE_UNAVAILABLE, "no backends configured").into_response();
}
```

If `BACKENDS` was empty, there is nowhere to send the request. Fairlead returns
HTTP 503.

### 3. Extract the Optional Affinity Header

```rust
let thread_id = headers
    .get("x-fairlead-thread-id")
    .and_then(|v| v.to_str().ok())
    .map(str::to_owned);
```

This tries to read `X-Fairlead-Thread-Id`.

The return type is `Option<String>`:

- `Some("thread-123")` if the header exists and is valid text.
- `None` if it is missing or invalid.

`Option<T>` is Rust's explicit nullable value. It is like a pointer that must be
checked before use, but without null pointer behavior.

### 4. Look Up Preferred Backend

```rust
let preferred = match thread_id {
    Some(ref tid) => state.affinity.preferred(tid).await,
    None => None,
};
```

If the request has a thread ID, Fairlead asks the affinity map whether that
thread already has a preferred backend index.

The `ref` means "borrow the string inside `Some` rather than moving it." We still
need `thread_id` later when recording affinity.

### 5. Select a Backend

```rust
let Some(idx) = select_backend(&state.backends, preferred).await else {
    return (
        StatusCode::SERVICE_UNAVAILABLE,
        "all backends unavailable (circuits open)",
    )
    .into_response();
};
```

`select_backend` returns `Option<usize>`:

- `Some(idx)` means backend at index `idx` is available.
- `None` means no backend is available.

`let Some(idx) = ... else { ... };` is pattern matching. It means:

```text
if result is Some(idx), continue with idx
otherwise return 503
```

### 6. How `select_backend` Works

`src/router/fallback.rs`:

```rust
pub async fn select_backend(backends: &[BackendState], preferred: Option<usize>) -> Option<usize> {
    if let Some(idx) = preferred {
        if let Some(backend) = backends.get(idx) {
            if backend.circuit.write().await.is_available() {
                return Some(idx);
            }
        }
    }

    for (i, backend) in backends.iter().enumerate() {
        if Some(i) == preferred {
            continue;
        }
        if backend.circuit.write().await.is_available() {
            return Some(i);
        }
    }

    None
}
```

It does two passes:

1. Try the preferred backend if there is one.
2. Otherwise walk the configured backend list in order.

Each check locks that backend's circuit breaker for writing because
`is_available()` may mutate circuit state from `Open` to `HalfOpen` if cooldown
has elapsed.

### 7. How the Circuit Breaker Works

`src/router/circuit.rs` defines:

```rust
pub enum CircuitState {
    Closed,
    Open { since: Instant },
    HalfOpen,
}
```

States:

- `Closed`: backend is healthy; send requests.
- `Open`: backend is unhealthy; skip it.
- `HalfOpen`: cooldown elapsed; allow one probe request to test recovery.

`record_success()` closes the circuit and resets failures.

`record_failure()` increments failures and opens the circuit when the configured
threshold is reached.

### 8. Build the Upstream URL

Back in `forward`:

```rust
let backend = &state.backends[idx];
let url = format!("{}/{}", backend.url.trim_end_matches('/'), path);
```

If the backend URL is:

```text
http://loki:8000/v1
```

and the path is:

```text
chat/completions
```

then the final upstream URL is:

```text
http://loki:8000/v1/chat/completions
```

### 9. Forward the Request to the Backend

```rust
let upstream = match state
    .client
    .post(&url)
    .header("content-type", "application/json")
    .body(body)
    .send()
    .await
{
    Ok(r) => r,
    Err(_) => {
        backend.circuit.write().await.record_failure();
        return StatusCode::BAD_GATEWAY.into_response();
    }
};
```

This uses `reqwest` to send the raw request body to the selected backend.

The `match` handles success or failure:

- `Ok(r)` means the backend returned an HTTP response.
- `Err(_)` means the HTTP request itself failed, such as connection refused.

On connection failure, Fairlead records a circuit failure and returns HTTP 502.

### 10. Record Backend Success or Failure

```rust
let status = upstream.status();

if status.is_server_error() {
    backend.circuit.write().await.record_failure();
} else {
    backend.circuit.write().await.record_success();
    if let Some(ref tid) = thread_id {
        state.affinity.record(tid, idx).await;
    }
}
```

Fairlead treats:

- `5xx` as backend failure.
- `2xx`, `3xx`, and `4xx` as backend success.

The reason `4xx` counts as success is that a `400 Bad Request` often means the
backend is alive and correctly rejected a bad client request. It should not trip
the backend's circuit breaker.

On success, if there was a thread ID, Fairlead records:

```text
thread_id -> backend_index
```

That is session affinity.

### 11. Preserve Streaming Headers

```rust
let content_type = upstream.headers().get(CONTENT_TYPE).cloned();
let is_sse = content_type
    .as_ref()
    .and_then(|v| v.to_str().ok())
    .map(|v| v.contains("text/event-stream"))
    .unwrap_or(false);
```

If the backend response is Server-Sent Events, Fairlead adds headers that help
keep streaming behavior intact:

```rust
if is_sse {
    builder = builder
        .header("cache-control", "no-cache")
        .header("x-accel-buffering", "no");
}
```

### 12. Stream the Response Back

```rust
let stream = upstream.bytes_stream();
let mut builder = Response::builder().status(status);
builder.body(Body::from_stream(stream)).unwrap()
```

This is the actual proxy behavior. Fairlead does not wait for the full backend
response body. It turns the upstream byte stream into an Axum response body and
returns it to the caller.

For LLM streaming, this is important because tokens can flow back as the backend
generates them.

## Background Health Probe Path

Each backend gets a background task:

```rust
tokio::spawn(async move {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let ok = client
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .is_ok();
        let mut cb = circuit.write().await;
        if ok {
            cb.record_success();
        } else {
            cb.record_failure();
        }
    }
});
```

`tokio::spawn` is similar to starting a lightweight async task. It runs
concurrently with request handlers.

The task loops forever:

1. Wait for the next interval tick.
2. Send `GET` to the backend base URL.
3. Lock the circuit breaker.
4. Record success or failure.

The request path and the health probe path share the same circuit breaker via
`Arc<RwLock<CircuitBreaker>>`.

## Metrics Path

`GET /metrics` calls `metrics::metrics`.

For each backend, it reads the circuit state and emits:

```text
fairlead_circuit_state{backend="http://loki:8000/v1"} 0
```

Values:

- `0`: closed
- `1`: half-open
- `2`: open

## Health Path

`GET /health` calls `health::health`, which returns:

```json
{"status":"ok"}
```

It does not currently check backend health. It only says the Fairlead process is
alive and can serve HTTP.

## Current End-to-End Example

With:

```text
BACKENDS=http://loki:8000/v1,http://thor:8000/v1
```

and a request:

```text
POST /v1/chat/completions
X-Fairlead-Thread-Id: abc
```

Fairlead does:

1. Axum routes to `proxy::chat_completions`.
2. `chat_completions` calls `forward`.
3. `forward` checks that backends exist.
4. `forward` extracts thread ID `abc`.
5. `SessionAffinity::preferred("abc")` returns a preferred index or `None`.
6. `select_backend` checks circuit breakers and returns an available backend.
7. `forward` builds the upstream URL.
8. `reqwest` sends the request body to vLLM or another compatible backend.
9. Fairlead records success or failure on that backend's circuit breaker.
10. On success, Fairlead records `abc -> selected_backend_index`.
11. Fairlead streams the backend response back to the caller.

## What Is Not Happening Yet

The current code does not:

- Run inference.
- Inspect model-specific request JSON.
- Estimate token count or memory use.
- Manage CUDA memory.
- Retry the same request on another backend after a single upstream failure.
- Keep separate backend pools per workload.
- Implement job queues or worker registration.
- Implement VRAM accounting.

Those are future Bluewater phases, not current behavior.
