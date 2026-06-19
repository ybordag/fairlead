# fairlead

A resource router for AI agent systems, written in Rust. Fairlead sits between an agent application and its compute backend, routing inference requests to the right hardware, handling failover, and managing GPU resources across nodes.

The name comes from sailing: a fairlead is a fitting that guides lines in exactly the right direction without friction or fouling.

**Status:** Design phase — no runnable code yet.

---

## What it does

Fairlead solves the infrastructure problems that should not live inside an application:

- **Inference routing** — routes LLM calls to the right backend based on health, VRAM availability, and current load
- **Provider fallback chain** — when local hardware is unavailable, falls back through a configurable chain (local vLLM → cloud provider A → cloud provider B)
- **Circuit breaking** — per-node circuit breakers prevent cascading failures; failed nodes are bypassed immediately and probed for recovery
- **Session failover** — when a node dies mid-session, in-flight requests are retried on the next available backend; session state survives via the application's external checkpoint store
- **VRAM accounting** — maintains a real-time view of GPU memory across all consumers (LLM server, vision sidecars, embedding servers) to prevent OOM scheduling
- **Agent worker pool** — manages application process instances, restarts crashed workers, routes requests least-busy

---

## System topology

```
Cambium (Go API gateway)
    │  OpenAI-compatible  /v1/chat/completions
    ▼
Fairlead  ←  this repo  (Rust)
    │  VRAM-aware routing, circuit breaking, fallback
    ├── vLLM on Loki  (DGX Spark B — primary local inference)
    ├── vLLM on Thor  (DGX Spark A — secondary / failover)
    └── Cloud providers  (Gemini Flash, Claude Haiku — last resort)
```

Fairlead exposes a standard **OpenAI-compatible API**. Any client that speaks the OpenAI API speaks Fairlead without modification — LangChain, LlamaIndex, the `openai` SDK, or a raw HTTP client.

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

## Local inference: vLLM

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
| Inference routing | **Fairlead** | Which backend handles this request, fallback, VRAM awareness |
| Application | Rhizome | What the agent does with the result |

These layers do not overlap. k3s cannot make VRAM-aware routing decisions. Fairlead cannot schedule containers or restart crashed processes. Each solves the problem the others cannot.

---

## Extensibility

Fairlead is not coupled to any specific agent application:

- **Async embedding service** — `/v1/embeddings` with its own routing and batching
- **Context chunking service** — async document chunking for RAG pipelines
- **Additional agent applications** — each registers its own worker pool

See [`design.md`](design.md) for the full architecture, open design questions, and component details.

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
