# Fairlead

Fairlead is a Rust inference gateway and compute-router prototype for local and
edge AI systems. It sits between applications and model-serving backends,
routing OpenAI-compatible requests to available inference servers while tracking
health, circuit state, and session affinity.

The name comes from sailing: a fairlead is a fitting that guides lines in exactly the right direction without friction or fouling.

**Status:** Phase 4 complete. Fairlead currently runs as an Axum HTTP service
with `/health`, `/metrics`, `/v1/chat/completions`, and `/v1/embeddings`.
The `bluewater` branch is the generalization effort to make Fairlead useful
beyond a single application.

---

## Current Capabilities

The current service provides:

- **OpenAI-compatible proxying** for chat completions and embeddings.
- **Streaming response passthrough** for Server-Sent Events.
- **Ordered backend selection** from the `BACKENDS` environment variable.
- **Node-aware backend metadata** through `BACKENDS_JSON` for richer local/edge
  deployments.
- **Per-backend circuit breakers** for connection failures and 5xx responses.
- **Background health probes** that update circuit state.
- **Soft session affinity** through `X-Fairlead-Thread-Id`.
- **Prometheus-style metrics** for backend circuit state.

Fairlead does **not** run inference itself. It routes requests to model servers
such as vLLM. vLLM owns model loading, GPU execution, KV cache management, and
token streaming. Fairlead owns request routing and control-plane policy around
those model servers.

---

## Bluewater Direction

Bluewater is the effort to make Fairlead a general-purpose local/edge compute
router rather than a Rhizome-specific proxy.

Planned work includes:

- **Workload abstraction** for chat, embeddings, rerank, vision, batch jobs, and
  future non-OpenAI-compatible adapters.
- **Node-aware backend configuration** so backends know whether they run on
  Loki, Thor, or another node.
- **Locality-aware routing** so a request that starts on Loki can prefer Loki's
  local vLLM backend before crossing the network to Thor.
- **Resource-aware admission** using cooperative VRAM/load reports from vLLM and
  other GPU consumers.
- **Same-request retry** for retryable upstream failures when the request can be
  safely replayed.
- **Priority queues and async jobs** for background work that should yield to
  realtime user requests.
- **Workload-aware observability** for selected backend, fallback reason,
  latency, queue depth, and resource state.

See [`docs/bluewater_generalization.md`](docs/bluewater_generalization.md) for
the implementation plan and acceptance criteria.

---

## System topology

Current simple topology:

```
Application or OpenAI-compatible client
    │  /v1/chat/completions or /v1/embeddings
    ▼
Fairlead
    │  circuit-aware routing + streaming proxy
    ├── vLLM on Loki
    └── vLLM on Thor
```

Intended Bluewater topology:

```
Applications / agents
    │  OpenAI-compatible inference + async jobs
    ▼
Fairlead
    │  workload-aware, node-aware, resource-aware routing
    ├── vLLM on Loki
    ├── vLLM on Thor
    ├── embedding / vision / indexing workers
    └── cloud providers as fallback
```

---

## Tech stack

- **Language:** Rust
- **Async runtime:** Tokio
- **Web framework:** Axum
- **HTTP client:** reqwest (async, supports streaming)
- **Serialization:** Serde / serde_json
- **Metrics:** Prometheus-compatible `/metrics`
- **Logging:** tracing / tracing-subscriber

---

## Running Locally

Example with two local OpenAI-compatible backends:

```bash
BACKENDS=http://loki:8000/v1,http://thor:8000/v1 \
PORT=7000 \
cargo run
```

Node-aware backend configuration:

```bash
BACKENDS_JSON='[
  {
    "id": "loki-vllm",
    "url": "http://loki:8000/v1",
    "node_id": "loki",
    "pool": "local-llm",
    "workloads": ["chat_completions", "embeddings"]
  },
  {
    "id": "thor-vllm",
    "url": "http://thor:8000/v1",
    "node_id": "thor",
    "pool": "local-llm",
    "workloads": ["chat_completions", "embeddings"]
  }
]' PORT=7000 cargo run
```

`BACKENDS` remains the simplest local setup path. `BACKENDS_JSON` is the
Bluewater configuration path for stable backend IDs, node identity, backend
pools, and workload support.

Health:

```bash
curl http://localhost:7000/health
```

Metrics:

```bash
curl http://localhost:7000/metrics
```

Chat completions are proxied to one of the configured backends:

```bash
curl http://localhost:7000/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'X-Fairlead-Thread-Id: demo-thread' \
  -d '{"model":"local-model","messages":[{"role":"user","content":"hello"}]}'
```

## Local Inference: vLLM

Fairlead routes to **vLLM** instances on the DGX Spark nodes. vLLM's OpenAI-compatible API means Fairlead treats a local GPU server and a cloud provider identically — routing to local vs. cloud is a URL swap, not a protocol change.

Each inference node runs a vLLM container:

```
Loki (DGX Spark B)
  └── vLLM container (vllm/vllm-openai)
        port: 8000
        API: http://loki:8000/v1
```

---

## Relationship to the stack

| Layer | Tool | Responsibility |
|---|---|---|
| Infrastructure | k3s / Docker | Where containers run, process restarts, scaling |
| GPU execution | vLLM | Efficient model serving, PagedAttention, continuous batching |
| Inference routing | **Fairlead** | Which backend handles a request, fallback, admission policy |
| Application | Rhizome or another app | What the agent or application does with the result |

These layers do not overlap. k3s can place and restart containers. vLLM can run
the model. Fairlead can decide where a request should go. Applications decide
what the model result means.

---

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — system architecture,
  vLLM/Fairlead responsibilities, and the Loki/Thor routing example.
- [`docs/code_walkthrough.md`](docs/code_walkthrough.md) — Rust code walkthrough
  from process startup to proxied response.
- [`docs/bluewater_generalization.md`](docs/bluewater_generalization.md) —
  generalization plan, feature epics, and acceptance criteria.
- [`docs/deferred_tests.md`](docs/deferred_tests.md) — known test gaps.

---

## Related repos

| Repo | Role |
|---|---|
| [rhizome](https://github.com/ybordag/rhizome) | AI agent + domain engine (Python, LangGraph) |
| [cambium](https://github.com/ybordag/cambium) | API gateway (Go) |
| verdant | React frontend |

---

## License

Apache 2.0
