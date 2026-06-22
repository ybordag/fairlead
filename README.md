# Fairlead

Fairlead is a Rust inference gateway and compute-router prototype for local and
edge AI systems. It sits between applications and model-serving backends,
routing OpenAI-compatible requests to available inference servers while tracking
health, circuit state, and session affinity.

The name comes from sailing: a fairlead is a fitting that guides lines in exactly the right direction without friction or fouling.

**Status:** Phase 8E is complete on `reef`. Phase 8 hardens the async scheduler
after the completed Phase 7 pool-aware placement work; 8A through 8D are merged,
and 8E adds process-level e2e coverage for restart-sensitive scheduler
behavior.
Fairlead currently runs as an Axum HTTP service with `/health`, `/metrics`,
`/v1/models`, `/v1/resources`, `/v1/resources/report`, `/v1/jobs`,
`/v1/jobs/prune`, `/v1/jobs/{id}`, `/v1/workers`, `/v1/workers/{id}`,
`/v1/workers/{id}/drain`, `/v1/workers/{id}/reactivate`,
`/v1/workers/{id}/claim`,
`/v1/workers/{worker_id}/jobs/{job_id}/renew`,
`/v1/workers/{worker_id}/jobs/{job_id}/complete`,
`/v1/workers/{worker_id}/jobs/{job_id}/fail`, `/v1/scheduler/preview`,
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
  reported resource state, async queue depth/wait, worker utilization, async
  worker pool placement, terminal job duration, and callback delivery outcomes.

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
- **Initial async job API** with in-memory submission, listing, polling,
  cancellation, per-priority queue tracking, queue depth and wait-time metrics,
  job type, priority, payload, and callback metadata.
- **Non-dispatching worker registration** with heartbeat, stale detection,
  capability metadata, and worker availability metrics.
- **Non-dispatching scheduler preview** that shows the next queued job and fresh
  compatible worker without leasing, running, or dispatching the job.
- **Worker-pull claim groundwork** that leases a compatible queued job to a
  fresh worker, marks it running, renews held leases, and requeues expired
  leases when attempts remain.
- **Initial worker result reporting** so the lease holder can complete a job
  with a result or report failure, with retryable failures requeued while
  attempts remain.
- **Worker capacity accounting** so claims respect `max_concurrent_jobs` and
  release in-flight slots on completion, failure, cancellation, or expired
  leases.
- **Per-attempt timeout state** so expired leases record `attempt timed out`,
  requeue while attempts remain, and fail when attempts are exhausted.
- **Terminal job duration metrics** grouped by priority, job type, and terminal
  status.
- **SQLite-backed durable job state** as an opt-in mode for local restart
  recovery.
- **Terminal job callbacks** with bounded retry/timeout policy,
  success/failure metrics, and SQLite-backed at-least-once restart recovery.
- **Pool-aware synchronous backend routing** through shared `POOLS_JSON` and
  `WORKLOAD_POOLS_JSON` policy, with ordered pool fallback and per-pool metrics.
- **Pool-aware async worker placement** so worker registration, scheduler
  preview, and worker-pull claims respect workload pool policy, with per-pool
  async placement metrics.
- **Optional strict pool validation** for production-like setups:
  `STRICT_WORKER_POOLS=true` bounds worker registration to configured or derived
  pools, and `STRICT_WORKLOAD_POOLS=true` requires explicit policy for every
  known workload.
- **Worker lifecycle controls** so operators can drain, reactivate, and
  deregister async workers without dropping held leases.
- **Terminal job pruning** through an explicit endpoint with configurable
  retention age, per-run limit, SQLite persistence, and Prometheus counters.
- **Background lease recovery** so expired async worker leases are requeued or
  failed on a configured maintenance interval, without waiting for the next
  worker claim. Worker lease duration defaults to 30 seconds and is configurable
  with `JOB_LEASE_DURATION_MS`.
- **Optional background terminal-job pruning** using the same retention,
  per-run limit, callback-safety, SQLite, idempotency-key, and metrics behavior
  as `POST /v1/jobs/prune`.

Phase 8E is complete on `reef` and adds a process-level e2e harness for
restart-sensitive scheduler behavior. Later phases add adapter boundaries,
richer resource policy, external scale/overflow, and transport/SDK hardening.

See [`docs/planning/roadmap.md`](docs/planning/roadmap.md) for the
implementation plan and acceptance criteria.

Local GPU-free demos are available in [`demo/`](demo/):

```bash
./demo/run_routing_demo.sh
./demo/run_async_jobs_demo.sh
```

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

Phase 7 adds explicit pool and workload placement policy:

```bash
POOLS_JSON='["local-llm", "peer-llm", {"id": "vision"}]' \
STRICT_WORKLOAD_POOLS=true \
STRICT_WORKER_POOLS=true \
WORKLOAD_POOLS_JSON='{
  "chat_completions": ["local-llm", "peer-llm"],
  "embeddings": ["local-llm", "peer-llm"],
  "vision_analysis": ["vision"]
}' \
cargo run
```

When `POOLS_JSON` is absent, Fairlead derives pools from configured backend
metadata and always includes the backward-compatible `default` pool. When
`WORKLOAD_POOLS_JSON` is absent, all known workloads are considered eligible for
all configured pools. Phase 7B applies this policy to synchronous chat and
embedding backend eligibility and treats each workload's pool list as an ordered
fallback chain. Phase 7C applies the same vocabulary to async worker
registration, scheduler preview, and worker-pull claims.

Explicit `WORKLOAD_POOLS_JSON` remains a partial override by default: if a
workload is omitted, that workload is still eligible for all configured pools.
Set `STRICT_WORKLOAD_POOLS=true` to require explicit pool policy for every known
workload at startup. Strict workload policy is useful for production-like demos
and deployments where an omitted workload should fail fast instead of silently
routing through every pool.

Worker registration is permissive by default: workers may register any non-empty
pool string. Set `STRICT_WORKER_POOLS=true` to reject worker registration unless
the worker's pool is present in configured or derived `POOLS_JSON`.

Async job state is in-memory by default. During Phase 6E, SQLite can be enabled
explicitly for durable job state across ordinary Fairlead restarts:

```bash
JOB_STORE=sqlite \
JOB_DB_PATH=fairlead_jobs.sqlite3 \
cargo run
```

`POST /v1/jobs` accepts an optional `idempotency_key` for safe caller retries.
When the same key is reused with the same request, Fairlead returns the existing
job instead of enqueueing duplicate work; reusing the key for a different
request is rejected.

With `JOB_STORE=sqlite`, Fairlead persists submitted jobs, queue order, submit
idempotency keys, claim and lease state, attempts, cancellation, completion,
failure, terminal worker-attempt metadata, payloads, callback metadata,
callback delivery state, and result/error state. On startup,
already-expired running leases are requeued when attempts remain and failed when
attempts are exhausted.

Fairlead also runs a background lease recovery loop. The loop uses the same
expiry path as worker claims: expired running leases release worker capacity,
record `attempt timed out`, and either requeue the job or fail it when attempts
are exhausted. The interval defaults to 30 seconds and can be changed with
`JOB_MAINTENANCE_INTERVAL_SECS`. Worker leases default to 30 seconds and can be
changed with `JOB_LEASE_DURATION_MS`.

Terminal async jobs can be pruned explicitly with `POST /v1/jobs/prune`.
Pruning removes only terminal jobs older than `JOB_RETENTION_SECS`, up to
`JOB_PRUNE_LIMIT` jobs per call. Jobs with pending callbacks are retained so
callback delivery can continue. Removed jobs are also deleted from SQLite, and
their submit idempotency keys are released, when `JOB_STORE=sqlite` is enabled.
Set `JOB_PRUNE_INTERVAL_SECS` to enable optional background pruning on the same
policy; leave it unset to prune only through the explicit endpoint.

`DELETE /v1/jobs/{id}` is idempotent for jobs that are already `cancelled`.
Cancelling a job that already completed or failed still returns a conflict,
because that work was not cancelled by the caller's earlier request.

Worker complete/fail requests can include the lease `attempt` returned by the
claim response. If a worker retries the same terminal complete/fail report with
the same worker ID, attempt, and result/error payload, Fairlead returns the
existing terminal job without releasing capacity or dispatching callbacks again.
Contradictory terminal reports still return a conflict.

```bash
JOB_RETENTION_SECS=86400 \
JOB_PRUNE_LIMIT=1000 \
JOB_LEASE_DURATION_MS=30000 \
JOB_MAINTENANCE_INTERVAL_SECS=30 \
JOB_PRUNE_INTERVAL_SECS=3600 \
cargo run
```

Terminal async jobs with `callback_url` are delivered asynchronously. Callback
delivery is at-least-once when SQLite persistence is enabled: pending callback
state survives ordinary Fairlead restarts and the recovery loop retries delivery
until a 2xx response is recorded. In-process callback dispatch is deduplicated
by job ID and delivered callbacks are skipped. Callback handlers should still be
idempotent by job ID because a crash after the receiver handles a callback but
before Fairlead records success can produce a duplicate callback after restart.

Callback delivery is bounded per delivery sweep by:

```bash
CALLBACK_MAX_ATTEMPTS=3 \
CALLBACK_TIMEOUT_SECS=5 \
CALLBACK_RETRY_DELAY_MS=250 \
cargo run
```

Each callback attempt is counted in `/metrics` by job type, terminal status,
delivery outcome, and callback HTTP status. Pruning operations are counted in
`/metrics` as `fairlead_job_prunes_total{status}`.

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
`outcome="priority_limited"` in request metrics. Synchronous requests still do
not wait in a queue; durable async scheduling policy remains future scheduler
work.

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
`spark-b`, starts Fairlead, then asserts the expected routing behavior. The
routing and async demos both source
[`demo/shared_pool_policy.sh`](demo/shared_pool_policy.sh) so local demos use
the same strict pool vocabulary. See [`demo/README.md`](demo/README.md) for
details.

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
- [`docs/planning/glossary.md`](docs/planning/glossary.md) — shared terminology
  for backends, providers, workers, workloads, routes, affinity, pools, and
  leases.
- [`docs/planning/workloads.md`](docs/planning/workloads.md) — current
  synchronous and async workload shapes.
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
- [`demo/README.md`](demo/README.md) — GPU-free routing and async jobs demos.
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
