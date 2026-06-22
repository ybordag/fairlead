# Supported Workload Shapes

Fairlead currently supports two workload shapes: synchronous OpenAI-compatible
proxy requests and bounded async worker-pull jobs.

## Synchronous Proxy Workloads

Synchronous workloads follow this shape:

```text
HTTP request to Fairlead
  -> workload metadata identifies eligible backend capabilities
  -> router filters by circuit state, workload support, resource reports,
     origin node, affinity, and retry exclusions
  -> selected backend URL receives the upstream request
  -> Fairlead streams or buffers the upstream response back to the caller
```

Implemented synchronous workloads:

| Workload | Fairlead route | Upstream shape | Response mode |
|---|---|---|---|
| `chat_completions` | `POST /v1/chat/completions` | OpenAI-compatible chat completions | Buffered or streamed SSE |
| `embeddings` | `POST /v1/embeddings` | OpenAI-compatible embeddings | Buffered JSON |

Synchronous requests are not durably queued. If no eligible backend exists, or a
priority admission bucket is full, Fairlead returns an error rather than waiting
indefinitely.

## Async Worker-Pull Job Workloads

Async jobs follow this shape:

```text
HTTP job submission to Fairlead
  -> job enters a priority queue
  -> worker registers capabilities and pulls a compatible job
  -> Fairlead grants a bounded lease
  -> worker renews, completes, or fails the leased job
  -> Fairlead records terminal state and optionally delivers a callback
```

Implemented async job types:

| Job type | Intended use | Current execution model |
|---|---|---|
| `vision_analysis` | Image or vision sidecar work | Generic worker-pull job |
| `embed_batch` | Batch embedding generation | Generic worker-pull job |
| `index_build` | Vector index construction | Generic worker-pull job |
| `cluster` | Embedding clustering | Generic worker-pull job |

Fairlead currently stores generic JSON payloads and result values for async
jobs. It does not interpret domain-specific payloads or persist application
records. The caller remains responsible for domain objects such as Rhizome
`VisionJob` rows.

## Priority Semantics

Fairlead uses the same priority vocabulary across synchronous and async work:

| Priority | Intended use | Current behavior |
|---|---|---|
| `realtime` | A user is waiting | Highest async queue order; synchronous admission bucket |
| `batch` | User-triggered async work | Middle async queue order; synchronous admission bucket |
| `background` | Maintenance work | Lowest async queue order; synchronous admission bucket |

Synchronous priority is currently admission control, not queueing. Async priority
is queue ordering for worker-pull jobs.

## Resource Semantics

Resource reports are cooperative control-plane hints. Fairlead uses reported
VRAM and load to avoid sending new work to saturated backends, but it does not
allocate CUDA memory or supervise containers. Future phases add richer resource
dimensions such as CPU slots, GPU slots, model residency, and custom worker
capacity.

## Unsupported Workload Shapes

The following are not implemented yet:

- non-OpenAI-compatible synchronous adapters such as rerank or image generation
- typed async adapters for specific worker protocols
- push dispatch from Fairlead to workers
- cloud-provider overflow pools
- gRPC transport, protobuf contracts, or generated SDKs
- multi-instance job coordination through Postgres or another shared store
