# Fairlead

Fairlead is a Rust inference gateway and compute-router prototype for local and
edge AI systems. It sits between applications and model-serving backends,
routing OpenAI-compatible requests to available inference servers while tracking
health, circuit state, and session affinity.

The name comes from sailing: a fairlead is a fitting that guides lines in exactly the right direction without friction or fouling.

**Status:** Phase 6B is in progress on the `tackle` branch. Fairlead currently
runs as an Axum HTTP service with `/health`, `/metrics`, `/v1/models`,
`/v1/resources`, `/v1/resources/report`, `/v1/jobs`, `/v1/jobs/{id}`,
`/v1/chat/completions`, and `/v1/embeddings`.

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
- **Origin-node locality** through `X-Fairlead-Origin-Node`.
- **Same-request retry** across eligible backends for connection failures,
  timeouts, and 5xx responses before response bytes are streamed.
- **Cooperative resource reporting** through `/v1/resources/report` and
  `/v1/resources`, with stale-report detection.
- **Priority metadata** through `X-Fairlead-Priority` with `realtime`, `batch`,
  and `background` values. Missing priority defaults to `realtime`.
- **Per-priority admission limits** for synchronous proxy requests. A full
  priority bucket fails fast with `429 Too Many Requests` instead of silently
  overloading the service.
- **Prometheus-style metrics** for backend circuit state, request outcomes,
  latency, fallback reasons, retry reasons, priority limits/in-flight counts,
  and reported resource state.

Fairlead does **not** run inference itself. It routes requests to model servers
such as vLLM. vLLM owns model loading, GPU execution, KV cache management, and
token streaming. Fairlead owns request routing and control-plane policy around
those model servers.

---

## Roadmap Direction

Fairlead is intended to become a general-purpose local/edge compute router rather
than a Rhizome-specific proxy.

Implemented generalization work includes:

- **Workload abstraction** for chat, embeddings, rerank, vision, batch jobs, and
  future non-OpenAI-compatible adapters.
- **Node-aware backend configuration** so backends know whether they run on
  spark-a, spark-b, or another node.
- **Locality-aware routing** so a request that starts on spark-a can prefer spark-a's
  local vLLM backend before crossing the network to spark-b.
- **Resource-aware admission** using cooperative VRAM/load reports from vLLM and
  other GPU consumers.
- **Richer retry policy and observability** for retry reasons, retry counts, and
  backend-level outcomes.
- **Priority admission limits** for synchronous requests that should fail fast
  instead of overloading a saturated priority bucket.
- **Workload-aware observability** for selected backend, fallback reason,
  latency, priority admission, and resource state.
- **Initial async job API** with in-memory submission, polling, cancellation, job
  type, priority, payload, and callback metadata.

Future phases add durable priority queues, worker registration, complete
pool-aware routing, adapter boundaries, queue wait-time metrics, callback
delivery, and cloud fallback.

See [`docs/planning/roadmap.md`](docs/planning/roadmap.md) for the
implementation plan and acceptance criteria.

---

## System topology

Current simple topology:

```
Application or OpenAI-compatible client
    │  /v1/chat/completions or /v1/embeddings
    ▼
Fairlead
    │  circuit-aware routing + streaming proxy
    ├── vLLM on spark-a
    └── vLLM on spark-b
```

Intended future topology:

```
Applications / agents
    │  OpenAI-compatible inference + async jobs
    ▼
Fairlead
    │  workload-aware, node-aware, resource-aware routing
    ├── vLLM on spark-a
    ├── vLLM on spark-b
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
BACKENDS=http://spark-a:8000/v1,http://spark-b:8000/v1 \
PORT=7000 \
cargo run
```

Node-aware backend configuration:

```bash
BACKENDS_JSON='[
  {
    "id": "spark-a-vllm",
    "url": "http://spark-a:8000/v1",
    "node_id": "spark-a",
    "pool": "local-llm",
    "workloads": ["chat_completions", "embeddings"]
  },
  {
    "id": "spark-b-vllm",
    "url": "http://spark-b:8000/v1",
    "node_id": "spark-b",
    "pool": "local-llm",
    "workloads": ["chat_completions", "embeddings"]
  }
]' PORT=7000 cargo run
```

`BACKENDS` remains the simplest local setup path. `BACKENDS_JSON` is the richer
configuration path for stable backend IDs, node identity, backend pools, and
workload support. By default, health probes append `models` to the
backend API base URL, so `http://spark-a:8000/v1` is probed at
`http://spark-a:8000/v1/models`. Backends that expose health elsewhere can set
`health_path`, for example `"/health"`.

Health:

```bash
curl http://localhost:7000/health
```

Metrics:

```bash
curl http://localhost:7000/metrics
```

Resource report:

```bash
curl http://localhost:7000/v1/resources/report \
  -H 'content-type: application/json' \
  -d '{"node_id":"spark-a","backend_id":"spark-a-vllm","total_vram_mb":64000,"reserved_vram_mb":16000,"current_load":0.25}'
```

Current resource state:

```bash
curl http://localhost:7000/v1/resources
```

Resource-aware routing is opt-in. When enabled, Fairlead skips healthy backends
that do not have a fresh report with enough available VRAM for the workload,
then ranks eligible fallback candidates by lower load and higher available VRAM:

```bash
RESOURCE_AWARE_ROUTING=true \
CHAT_COMPLETIONS_REQUIRED_VRAM_MB=1024 \
EMBEDDINGS_REQUIRED_VRAM_MB=512 \
cargo run
```

Priority admission limits are synchronous request caps, not durable queues. Tune
them with:

```bash
PRIORITY_REALTIME_LIMIT=8 \
PRIORITY_BATCH_LIMIT=4 \
PRIORITY_BACKGROUND_LIMIT=2 \
cargo run
```

When a bucket is full, Fairlead returns `429 Too Many Requests` and records
`outcome="priority_limited"` in request metrics. Full priority queues, wait-time
metrics, and async job scheduling remain future scheduler work.

Chat completions are proxied to one of the configured backends:

```bash
curl http://localhost:7000/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'X-Fairlead-Origin-Node: spark-a' \
  -H 'X-Fairlead-Thread-Id: demo-thread' \
  -H 'X-Fairlead-Priority: realtime' \
  -d '{"model":"local-model","messages":[{"role":"user","content":"hello"}]}'
```

## Routing Demo

Run the GPU-free local demo to see locality, fallback, same-request retry,
recovery, metrics, and structured traces:

```bash
./demo/run_routing_demo.sh
```

The demo starts two mock OpenAI-compatible backends named `spark-a` and
`spark-b`, starts Fairlead, then asserts the expected routing behavior. See
[`demo/README.md`](demo/README.md) for details.

## Local Inference: vLLM

Fairlead routes to **vLLM** instances on local GPU nodes. vLLM's
OpenAI-compatible API means Fairlead treats a local GPU server and a cloud
provider identically: routing to local vs. cloud is a URL swap, not a protocol
change.

In a small DGX Spark deployment, each inference node can run one vLLM server:

```
DGX Spark node
  └── vLLM server
        port: 8000
        API: http://<node-hostname>:8000/v1
```

See [`docs/implementation/dgx_spark_deployment.md`](docs/implementation/dgx_spark_deployment.md)
for the manual two-node deployment notes using vLLM, `uv`, and Fairlead.

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

Start with [`docs/README.md`](docs/README.md) for the full documentation map.

- [`docs/planning/architecture.md`](docs/planning/architecture.md) — system
  architecture, vLLM/Fairlead responsibilities, and the spark-a/spark-b routing
  example.
- [`docs/implementation/code_walkthrough.md`](docs/implementation/code_walkthrough.md)
  — Rust code walkthrough from process startup to proxied response.
- [`docs/planning/design.md`](docs/planning/design.md) — design horizon and
  longer-term product shape.
- [`docs/planning/roadmap.md`](docs/planning/roadmap.md) — generalization plan,
  feature epics, and acceptance criteria.
- [`docs/implementation/dgx_spark_deployment.md`](docs/implementation/dgx_spark_deployment.md)
  — manual deployment notes for two DGX Spark nodes connected over InfiniBand.
- [`docs/implementation/fixture_examples.md`](docs/implementation/fixture_examples.md)
  — conventions for sanitized test fixtures and ignored local deployment config.
- [`demo/README.md`](demo/README.md) — GPU-free routing demo.
- [`docs/current_work/deferred_tests.md`](docs/current_work/deferred_tests.md) —
  known test gaps.

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
