# Phase 7A: Helm

Branch: `helm`

Goal: add the shared pool policy model before routing or scheduling starts using
it.

## Scope

- Add named placement pools as first-class configuration.
- Preserve `BACKENDS` compatibility by deriving the `default` pool when no
  explicit pool list is configured.
- Add workload-to-pool policy configuration for synchronous workloads and async
  job types.
- Validate pool references early so typoed placement policy fails at startup.
- Keep runtime dispatch behavior unchanged until the dedicated routing and
  worker-placement slices.

## Configuration

`POOLS_JSON` declares named pools. It accepts either string IDs or objects with
an `id` field:

```bash
POOLS_JSON='["local-llm", "peer-llm", {"id": "vision"}]'
```

`WORKLOAD_POOLS_JSON` maps known workload names to one or more configured pools:

```bash
WORKLOAD_POOLS_JSON='{
  "chat_completions": ["local-llm", "peer-llm"],
  "embeddings": ["local-llm", "peer-llm"],
  "vision_analysis": ["vision"]
}'
```

Known policy names currently include:

- `chat_completions`
- `embeddings`
- `vision_analysis`
- `embed_batch`
- `index_build`
- `cluster`

## Validation

Fairlead rejects:

- empty `POOLS_JSON`
- empty or duplicate pool IDs
- backend configs that reference undeclared pools when `POOLS_JSON` is explicit
- empty `WORKLOAD_POOLS_JSON`
- unknown workload names
- empty workload pool lists
- empty, duplicate, or undeclared pool references in workload policy

## Deferrals

- `trimmer` / Phase 7B applies pool policy to synchronous backend routing.
- `tactician` / Phase 7C adds worker pool metadata and applies pool policy to
  async worker placement.
- `navigator` / Phase 7D adds shared local demos and final pool docs.
