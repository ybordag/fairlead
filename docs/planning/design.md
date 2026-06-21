# Fairlead — Design Document

**Status:** Design horizon; current implementation is documented in
[`../../README.md`](../../README.md), [`architecture.md`](architecture.md), and
[`../implementation/code_walkthrough.md`](../implementation/code_walkthrough.md).
**Version:** 0.1

---

## What it is

Fairlead is a resource router for AI agent systems. It sits between an agent
application and its compute backends, routing inference requests to the right
hardware, tracking cooperative capacity reports, and handling failover when
nodes become unavailable. Future phases add async job scheduling for registered
compute workers, but Fairlead should not supervise or restart those worker
processes itself.

The name comes from sailing: a fairlead is a fitting that guides lines in exactly the right direction without friction or fouling. It does not generate power or hold cargo — it ensures that what needs to flow, flows correctly.

Fairlead is not specific to any agent application. It is a general-purpose infrastructure component.

---

## Problem

An AI agent application that makes LLM calls against local hardware faces several failure modes the application should not have to manage itself:

- **No local failover.** If a model server crashes, every active session on that node fails hard.
- **No cloud fallback.** When local hardware is saturated, requests queue indefinitely or are dropped.
- **No VRAM awareness.** Multiple GPU consumers (LLM serving, vision sidecars, embedding servers) compete for memory with no coordination, causing OOM failures.
- **No session continuity.** A mid-session node failure has no recovery path.
- **No compute-worker dispatch.** The application has no infrastructure-level
  way to submit bounded compute jobs, select eligible workers, enforce leases,
  or retry failed attempts.

Solving these problems inside the application couples infrastructure concerns to business logic. Fairlead solves them as a separate infrastructure layer.

---

## Interface

Fairlead exposes an **OpenAI-compatible HTTP endpoint**. Any client that speaks
the OpenAI API can speak to Fairlead — LangChain, LlamaIndex, the `openai` SDK,
or a raw HTTP client — without modification.

Current implemented endpoints:

```
POST /v1/chat/completions
POST /v1/embeddings
GET  /v1/models
GET  /health
GET  /metrics
POST /v1/resources/report
GET  /v1/resources
```

Planned endpoints:

```text
POST /v1/jobs
GET  /v1/jobs/{id}
POST /v1/workers/register
```

The application points its model client at `http://fairlead/v1` instead of a
single model-server endpoint. Routing, failover, resource-aware backend
eligibility, and priority admission happen transparently behind that address.
Worker registration and async job dispatch are future phases.

---

## Core components

### 1. Inference router

Routes LLM calls to backend nodes based on:

- **Health state** — is the node currently responding?
- **VRAM availability** — does the node have enough free memory for this request's context length?
- **Current load** — how many requests is the node handling right now?
- **Session affinity** — prefer to keep a session on the same node for KV cache warmth, but do not hard-pin

Session affinity is soft. If the preferred node is unavailable or overloaded, the session moves rather than queues indefinitely.

### 2. Provider fallback chain

When no local node can handle a request, Fairlead falls back through a configurable priority chain:

```
local node A  (primary)
  → local node B  (secondary)
    → cloud provider A  (Gemini Flash, fast + cheap)
      → cloud provider B  (Claude Haiku, last resort)
```

Each step is tried only when the previous step is unavailable or returns a retryable error. The chain is defined in configuration, not in code.

### 3. Circuit breaker

Per-node circuit breakers prevent cascading failures:

- **Closed** — healthy; requests flow normally
- **Open** — failed; requests skip this node immediately with no retry delay
- **Half-open** — recovering; a probe request tests the node; success closes the circuit, failure re-opens it

The circuit opens after N consecutive failures or a response timeout threshold. It half-opens after a configurable cooldown.

### 4. Future worker registry

Future Fairlead phases may manage a registry of compute workers:

- Tracks registered worker processes and their capabilities
- Marks workers stale when heartbeats expire
- Dispatches jobs to available workers based on workload support, resource
  reports, and priority policy
- Exposes worker health through the `/health` and `/metrics` endpoints

This is separate from process supervision. k3s, Docker, or systemd should restart
crashed worker processes. Fairlead should track worker capabilities and dispatch
jobs, not own application domain logic.

### 5. VRAM accounting

Maintains a cooperative view of GPU memory consumption across reported consumers
on each node:

- Primary LLM serving process
- MCP sidecar processes (vision servers, embedding servers)
- Any other registered GPU consumers

VRAM accounting is cooperative: each consumer reports its allocation/load, and
the router uses this view to avoid scheduling requests onto nodes that cannot
serve them without going OOM. Consumers that do not report are invisible to the
accounting layer, so this is a control-plane hint rather than CUDA-level memory
management.

### 6. Session failover

When a node dies mid-session:

1. The circuit breaker opens for the failed node
2. In-flight requests on that node receive a retryable error response
3. The router retries on the next node in the fallback chain
4. The new node loads session state from the application's external checkpoint store
5. The session continues; the user experiences a brief pause

**Requirement:** the application must externalize its session checkpoint store (e.g., Postgres) before session failover works. Fairlead does not manage the checkpoint store — it relies on the application having already done this.

---

## Local inference backend: vLLM

Fairlead routes to local model servers running **vLLM** on the DGX Spark nodes.

### Why vLLM

vLLM is purpose-built for LLM inference serving on GPUs. Its core innovations:

- **PagedAttention** — manages GPU KV cache memory the way an OS manages virtual
  memory, dramatically improving throughput under concurrent requests
- **Continuous batching** — keeps the GPU busy rather than waiting for a full batch
  to arrive; new requests join in-flight batches as soon as capacity is available
- **OpenAI-compatible API** — exposes `POST /v1/chat/completions` and
  `POST /v1/embeddings`, the same interface as the cloud providers Fairlead routes to

The OpenAI-compatible API is the critical integration point. Fairlead treats a local
vLLM instance identically to OpenAI's cloud endpoint — same request format, same
response format. Routing to local vs cloud is a URL and authentication swap, not a
protocol change.

### vLLM on DGX Spark

```
spark-a (DGX Spark)
  └── vLLM container
        image: vllm/vllm-openai
        port: 8000
        model: e.g. meta-llama/Llama-3-8B-Instruct
        GPU: full node VRAM allocated to vLLM
        API: http://node-a:8000/v1  (OpenAI-compatible)
```

For models that exceed a single node's VRAM, vLLM supports tensor parallelism across
multiple GPUs. Cross-node parallelism (both Sparks together) is possible for 70B+
models but requires vLLM's distributed serving mode and adds latency.

### Fallback chain with vLLM

```
  1. vLLM on spark-a           (local, fast, no API cost)
  2. vLLM on spark-b           (local secondary, if spark-a is down)
  3. future cloud provider  (overflow, if configured)
```

---

## Relationship to infrastructure layers

```
k3s / Docker     — infrastructure: where containers run, restarts, scaling
vLLM             — GPU execution: efficient model serving with PagedAttention/batching
Fairlead         — inference routing: which model/provider handles this request
Rhizome          — application: what the agent does with the result
```

**k3s schedules the vLLM container onto the GPU node.** It does not know the container
is serving an LLM, cannot make VRAM-aware routing decisions, and cannot fall back to
a cloud provider when GPU memory is exhausted. That is Fairlead's job.

**Fairlead routes to the vLLM endpoint.** It does not manage the vLLM process, restart
it on crash, or allocate GPU resources. That is k3s's job.

---

## What Fairlead does not own

- Application domain logic of any kind
- Database schema or migrations
- Session checkpoint storage
- MCP sidecar process lifecycle (tracks VRAM; does not start or stop sidecars)
- End-user authentication

---

## Extensibility

- **Async embedding service** — future job queue for batch embeddings
- **Context chunking service** — async chunking for RAG pipelines, job-queue backed
- **Additional agent applications** — each registers its own worker pool

Extension model: new service type → new worker pool + endpoint path + VRAM registration if GPU-bound.

---

## Open design questions

**Session affinity granularity:** per-thread (one conversation) or per-user (all sessions prefer the same node)?

**VRAM accounting protocol:** push (consumers register with Fairlead) vs. pull (Fairlead polls consumers)?

**Worker pool sizing:** static (N workers at startup) vs. dynamic (scale on queue depth)?

**Metrics format:** Prometheus-compatible `/metrics` for Grafana integration.
