# Fairlead Glossary

These terms are used consistently across the roadmap, architecture notes, and
implementation docs.

## Backend

A network service that can execute a synchronous request selected by Fairlead.
Today, a backend is usually an OpenAI-compatible model server such as vLLM.
Backends are configured with a URL, health probe target, stable ID, optional
node ID, pool, and supported synchronous workloads.

Example: `spark-a-vllm` at `http://spark-a:8000/v1`.

## Provider

An external or local service family that owns a backend implementation, API
shape, credentials, and billing or operational policy. In current Fairlead, the
provider model is mostly implicit: OpenAI-compatible local servers and future
cloud providers are both represented as backend URLs plus header policy.

Provider-specific credentials and cloud overflow policy are future work.

## Worker

A cooperative async compute process that registers with Fairlead, advertises the
job types it can run, claims queued jobs, renews leases, and reports completion
or failure. Workers pull work from Fairlead; Fairlead does not currently push
jobs directly into worker processes or supervise worker lifecycles.

Example: a vision worker that supports `vision_analysis`.

## Workload

The kind of work Fairlead is asked to route or schedule. Workloads determine
eligible backends or workers, priority, resource estimates, retry behavior, and
response mode.

Current synchronous workloads:

- `chat_completions`
- `embeddings`

Current async job types:

- `vision_analysis`
- `embed_batch`
- `index_build`
- `cluster`

## Route

An HTTP surface exposed by Fairlead and the workload metadata attached to it.
For synchronous proxy routes, route metadata includes the Fairlead path, upstream
path, method, streaming behavior, retry policy, metric labels, and eligible
backend workloads.

Example: `POST /v1/chat/completions` routes to a selected backend's
`/v1/chat/completions`.

## Affinity

A soft routing preference that keeps related synchronous requests on the same
backend when that backend remains eligible. Affinity is keyed by
`X-Fairlead-Thread-Id` and scoped by workload so chat and embedding requests do
not accidentally pin each other.

Affinity is not a hard guarantee. Circuit state, resource eligibility, origin
locality, and retry exclusions can override it.

## Pool

A named group of backends or workers that represent a placement boundary.
Examples include `local-gpu`, `peer-gpu`, `vision-workers`, or future
`cloud-overflow`. Phase 7 makes pools first-class for both synchronous backend
routing and async worker placement.

## Node

A physical or logical machine where a backend or worker runs. Node IDs power
origin-aware locality and resource reporting.

Example: `spark-a`.

## Lease

A bounded claim on an async job by a worker. While a lease is valid, only the
holding worker can renew, complete, or fail that job. Expired leases are requeued
when attempts remain and failed when attempts are exhausted.

## Callback

An HTTP POST Fairlead sends when an async job reaches a terminal state and the
job has `callback_url`. Callback delivery is at least once when SQLite
persistence is enabled. Callback receivers should be idempotent by job ID.

## Idempotency Key

An optional caller-provided key on `POST /v1/jobs`. If a caller retries the same
submission with the same `idempotency_key`, Fairlead returns the original job
record instead of enqueueing duplicate work. Reusing the same key with a
different job request is rejected.
