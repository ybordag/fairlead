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

### `phase_6c_worker_claims_and_leases`

When later phases build on Phase 6C/6D worker-pull claims, leases, and result
reporting, add tests for:

- concurrent cancellation versus complete/fail requests against the same leased
  job
- background lease sweep behavior, if Fairlead adds a scheduler loop instead of
  claim-time opportunistic sweeps only
- opt-in local multi-process e2e: start Fairlead, register two fake workers,
  submit jobs, claim/renew/cancel/requeue through HTTP, and assert final job
  state and metrics
- opt-in DGX Spark e2e: run Fairlead with workers on the two connected DGX Spark
  nodes, verify claim/renew behavior across nodes, then cancel and reclaim
  expired work without requiring real model execution

**Why deferred:** Phase 6B now has the in-memory job API, worker registry, queue
metrics, and non-mutating scheduler preview. These tests require mutating claims,
lease metadata, running-job state, and worker execution endpoints. Cleat covers
the claim endpoint, duplicate-claim prevention, stale worker exclusion,
unsupported job types, priority ordering, FIFO ordering, queued/running
cancellation basics, claim-time expired lease requeue/failure, lease renewal,
renewal ownership checks, and cancellation ordering around running leases and
requeued jobs. Halyard adds result endpoints, timeout state, capacity
accounting, and duration metrics. The remaining race and e2e tests need a
heavier multi-process/deployment harness. Halyard's in-process suite covers
duplicate result reports after terminal state.

### `phase_6d_worker_execution_and_utilization`

After Phase 6D, add tests for:

- configurable per-workload timeout policy, if future phases allow different
  timeout durations by job type
- concurrent claims racing against `max_concurrent_jobs` once the scheduler runs
  under a heavier multi-worker harness
- local multi-process e2e: start Fairlead, register fake workers, submit jobs,
  claim work, complete/fail work, verify terminal state, retry behavior, worker
  capacity release, timeout state, duration metrics, and utilization metrics
- opt-in DGX Spark e2e with fake async workers on the two connected nodes:
  register node-local workers, claim/renew/complete jobs from each node, verify
  capacity metrics and timeout/retry state, and keep real model execution out of
  the test unless a later workload-specific smoke test needs it
- concurrency stress: many workers claim against the same queue and limited
  `max_concurrent_jobs` values, asserting no duplicate running job leases and no
  worker exceeds configured capacity

**Why deferred:** Halyard's first slice covers completion, retryable failure
requeue, non-retryable failure, retry exhaustion, and endpoint ownership checks.
The current Halyard branch also covers in-flight accounting, capacity release on
completion/failure/cancellation/expiry, capacity rejection, and utilization
metric output. It also covers terminal job duration snapshots and metrics. The
current Halyard branch also covers explicit timeout error state for expired
leases, unknown/stale workers on result endpoints, and duplicate result reports
after terminal state. The remaining tests need configurable timeout policy or a
heavier concurrency/e2e harness.

### `phase_6e_job_persistence_and_recovery`

Phase 6E now has unit-level restart tests for queue recovery, priority/FIFO
ordering, next ID recovery, running lease preservation, cancelled state,
completed state, callback metadata, terminal result state, expired running lease
startup recovery, SQLite bootstrap migration, and endpoint-level AppState
recovery. It also covers lease renewal persistence, custom worker failure
persistence, and claiming a recovered queued job through worker endpoints after
an app rebuild. Remaining deferred tests:

- Process-level e2e restart test with an actual Fairlead process and DB file:
  submit jobs, stop Fairlead, restart with the same `JOB_DB_PATH`, verify list,
  get, worker claim, complete/fail, and metrics behavior.
- Process-level expired-lease restart test: claim a job with a short lease,
  stop Fairlead until the lease expires, restart, and verify requeue/failure
  behavior through HTTP.
- Storage write failure tests: read-only DB path, unwritable parent directory,
  disk-full simulation if feasible, and SQLite busy/locked write behavior.
- Corrupted or incompatible SQLite file behavior: invalid file contents,
  malformed JSON columns, unknown enum values, negative numeric fields, and
  future `user_version` handling.
- Multi-process safety test if Fairlead ever supports more than one active
  router against the same SQLite file. SQLite is currently scoped for a single
  Fairlead process.
- Thor/Loki e2e recovery with jobs submitted before and after Fairlead restart.

**Why deferred:** The remaining cases need a larger restart harness or real
deployment environment. The current Shackle tests cover the storage-backed
registry boundary directly.

### `phase_6f_callback_delivery`

Phase 6F now covers successful terminal callbacks, transient callback retry,
terminal callback failure metrics, callback timeout handling, callback
success/failure metrics, cancellation callbacks, no callback on retryable job
requeue, SQLite-backed pending callback delivery after registry restart, and
SQLite-backed failed callback attempt retry after registry restart. Remaining
deferred tests:

- Callback retry recovery across a full Fairlead OS-process restart.
- Duplicate callback behavior when Fairlead crashes after the receiver handles
  the callback but before Fairlead records delivery success.
- Process-level e2e callback delivery with an actual Fairlead process and
  callback receiver.
- Deployment-level callback delivery on Thor/Loki.

**Why deferred:** These need a larger process/deployment harness or controlled
crash injection. The current Stay tests cover the durable registry boundary
directly.
