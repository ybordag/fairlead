# Deferred Tests

Tests that are useful but intentionally deferred because they need heavier
fixtures or a CI-friendly demo harness. The low-risk unit and proxy tests
previously listed here have been implemented.

---

## Demo Harness

### `small_cluster_demo_exercises_routing_story`

Add an automated smoke test around the local mock demo that starts two
OpenAI-compatible backends named `spark-a` and `spark-b`, then verifies:

- same-node preference
- resource-aware peer fallback when the origin backend reports insufficient
  headroom
- peer fallback when same-node circuit is open
- same-request retry after upstream failure
- circuit recovery
- metrics output for request, fallback, retry, circuit state, resource state, and
  priority admission

**Why deferred:** The runnable demo exists in `demo/`, but it starts subprocesses
and binds local ports. It is better as a manual portfolio demo until there is a
CI-friendly integration-test harness for process lifecycle and port allocation.

---

## DGX Spark End-To-End Smoke Tests

### `dgx_two_node_vllm_smoke_test`

Add a script-driven manual or opt-in test for two DGX Spark nodes connected over
InfiniBand, with one vLLM server per node and Fairlead running on one node.
The test should verify:

- direct `GET /v1/models` works against each vLLM server
- Fairlead `GET /v1/models` lists both configured backends with node, pool, and
  workload metadata
- `X-Fairlead-Origin-Node` routes local-origin chat requests to the local vLLM
- peer-origin requests route to the peer vLLM when healthy
- stopping or blocking the peer vLLM causes same-request fallback to the local
  vLLM for retryable failures before response bytes are streamed
- restoring the peer vLLM lets health probes close the circuit again
- `/metrics` records workload, backend, node, pool, fallback, retry, and circuit
  labels after the smoke run

**Why deferred:** This requires real hostnames, GPU availability, vLLM model
startup time, network access between nodes, and controlled process lifecycle on
remote machines. It should be an opt-in deployment smoke test, not part of the
default Rust test suite.

### `dgx_resource_pressure_routes_to_peer`

Add a two-node resource-routing smoke test that reports low available VRAM for
the origin backend and sufficient VRAM for the peer backend, then verifies:

- Fairlead selects the peer backend for the affected workload
- the origin backend is not called
- metrics include `reason="resource_unavailable"`
- clearing or refreshing resource reports restores local routing

**Why deferred:** The behavior can be tested with local mocks, but the real
deployment test should validate that the documented resource-report commands and
node identifiers line up with the actual DGX Spark deployment.

### `dgx_streaming_fallback_boundary`

Add an opt-in streaming test that proves Fairlead retries only before streaming
response bytes have started:

- backend A returns a retryable 5xx before streaming; Fairlead retries backend B
- backend A starts an SSE stream and then fails mid-stream; Fairlead does not
  replay the request to backend B

**Why deferred:** The local unit suite covers the mid-stream boundary with mocks.
The DGX version is only useful once the deployment harness can reliably observe
vLLM/Fairlead logs and distinguish pre-stream failure from mid-stream failure.

---

## Future Phase Tests

### `phase_7a_pool_aware_routing_matrix`

When Phase 7A implements complete pool-aware routing, add tests for:

- workload-to-pool validation at startup
- missing, empty, and misspelled named pools
- pool fallback chains, such as local -> peer -> cloud overflow
- per-pool metrics and fallback labels
- consistent pool semantics for synchronous backends and async workers

**Why deferred:** Clew intentionally keeps only pool metadata. Complete
pool-aware placement is deferred to Phase 7A so the design can cover both
synchronous and async compute.

### `phase_6b_async_scheduler_recovery`

When Phase 6B implements async jobs, add tests for:

- queue persistence or recovery behavior after Fairlead restart
- worker lease timeout and retry
- callback retry and terminal failure
- cancellation of queued and running jobs
- priority ordering across realtime, batch, and background queues

**Why deferred:** These require the async job API, scheduler, worker registry,
leases, and job state, which do not exist yet.
