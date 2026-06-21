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

### `phase_7a_process_startup_pool_policy_validation`

Add an opt-in process-level smoke test that starts the Fairlead binary with
several pool policy configurations and verifies process startup or failure:

- valid `BACKENDS` without `POOLS_JSON` starts and logs the derived `default`
  pool policy
- valid `BACKENDS_JSON`, `POOLS_JSON`, and `WORKLOAD_POOLS_JSON` starts and logs
  the configured pools
- malformed `POOLS_JSON` exits before binding a port
- `BACKENDS_JSON` that references an undeclared explicit pool exits before
  binding a port
- `WORKLOAD_POOLS_JSON` with an undeclared pool exits before binding a port
- process stderr or structured logs include the invalid config key name

**Why deferred:** The current unit tests cover the parser and validation errors
without spawning a process. A process-level test needs binary lifecycle, port
allocation, log capture, and timeout handling.

### `phase_7_pool_placement_e2e_matrix`

After Phase 7B and 7C consume the validated policy, add local and DGX Spark e2e
tests for complete pool placement behavior:

- synchronous workload pool allowlists select only eligible backend pools
- ordered pool fallback chains behave as documented
- pool policy interacts correctly with origin locality, affinity, resource
  ranking, circuit state, and same-request retry
- async workers register with pools and only claim jobs whose workload can use
  that worker pool
- strict worker pool mode accepts configured worker pools and rejects typos
  before rejected workers appear in `GET /v1/workers`
- omitted worker pools deserialize to `default`, and strict mode accepts or
  rejects that default based on whether `default` is configured or derived
- strict workload pool mode fails process startup when explicit policy is absent
  or partial, and starts when every known workload has policy
- per-pool metrics report candidate counts, selected pool/backend or worker,
  no-compatible-pool cases, fallback reasons, and capacity pressure
- local mock e2e and two-node DGX Spark e2e use the same sanitized pool config
  shape

**Why deferred:** Phase 7A intentionally stops at config and validation. The
routing, async placement, and deployment behavior belongs to Phase 7B through
7D and needs a richer process/deployment harness.

### `phase_7b_sync_pool_routing_process_e2e`

Add an opt-in process-level e2e for synchronous pool routing with local mock
OpenAI-compatible backends:

- start Fairlead with `POOLS_JSON`, `BACKENDS_JSON`, and `WORKLOAD_POOLS_JSON`
  describing at least `local-llm` and `peer-llm`
- verify chat selects the first configured workload pool even when backend order
  lists the peer pool first
- verify `X-Fairlead-Origin-Node` only affects selection inside the current pool
  and does not jump to a later pool while an earlier pool is selectable
- trip or stop every backend in the first pool and verify selection falls back
  to the second pool
- make the first-pool backend return a retryable 5xx and verify same-request
  retry moves to the next pool only after the first pool is exhausted
- verify a workload omitted from explicit `WORKLOAD_POOLS_JSON` remains
  permissive when `STRICT_WORKLOAD_POOLS` is unset or false
- verify startup fails when `STRICT_WORKLOAD_POOLS=true` and the explicit
  workload policy omits a known workload
- verify `/metrics` includes `fairlead_pool_selections_total`,
  `fairlead_pool_candidate_backends_total`, and
  `fairlead_pool_resource_ineligible_backends_total` with expected labels

**Why deferred:** The in-process Rust tests cover the routing and metric logic.
This e2e should exercise real process startup, environment parsing, port
allocation, mock process lifecycle, and Prometheus scraping.

### `phase_7b_dgx_sync_pool_routing_smoke_test`

Add an opt-in DGX Spark smoke test for synchronous pool routing:

- configure two DGX Spark nodes connected over InfiniBand with each node running
  a vLLM backend
- use pool names that match the documented local/peer deployment shape
- verify local-pool routing, peer-pool fallback, and pool metrics through real
  Fairlead HTTP calls
- report resource pressure for the first pool and verify Fairlead records
  per-pool resource-ineligible counts while selecting the later pool
- verify circuit-open recovery returns traffic to the earlier pool after health
  probes close the circuit

**Why deferred:** This needs real DGX hosts, vLLM startup, network reachability,
resource-report commands, and controlled backend lifecycle. It should be an
opt-in deployment smoke test, not part of the default Rust suite.

### `phase_7c_async_worker_pool_process_e2e`

Add an opt-in process-level e2e for async worker pool placement with local fake
workers:

- start Fairlead with `POOLS_JSON` and `WORKLOAD_POOLS_JSON` describing at
  least `vision`, `batch`, and `peer` pools
- register fake workers in different pools through real
  `POST /v1/workers/register` calls
- submit `vision_analysis` and `embed_batch` jobs through real `/v1/jobs`
  calls, with policies that allow each job type to use different pools
- verify `/v1/scheduler/preview` only returns worker/job pairs where the
  worker's pool is allowed by the job type
- verify `/v1/workers/{id}/claim` returns `204 No Content` when the worker
  supports a queued job type but its pool is not allowed
- verify a worker in an allowed pool can claim the same queued job after a
  disallowed-pool worker receives no work
- verify a workload omitted from explicit `WORKLOAD_POOLS_JSON` remains
  permissive when `STRICT_WORKLOAD_POOLS` is unset or false
- verify startup fails when `STRICT_WORKLOAD_POOLS=true` and the explicit
  workload policy omits a known async workload
- verify `/metrics` includes `fairlead_async_pool_selections_total`,
  `fairlead_async_pool_candidate_workers_total`, and
  `fairlead_async_pool_no_compatible_jobs_total` with expected pool, worker,
  node, job type, priority, and outcome labels
- verify worker pool metadata survives worker upsert and appears in
  `GET /v1/workers`

**Why deferred:** The in-process Rust tests cover the placement and metric logic.
This e2e should exercise real process startup, environment parsing, worker/job
HTTP calls, port allocation, and Prometheus scraping.

### `phase_7c_dgx_async_worker_pool_smoke_test`

Add an opt-in DGX Spark smoke test for async worker pool placement:

- run Fairlead on the two-node DGX Spark setup with fake async workers on both
  nodes, using sanitized pool names such as `vision-local`, `vision-peer`, and
  `batch`
- register workers with node and pool metadata from each host
- submit a vision job that should only be claimed by the configured vision pool
- verify a peer or batch worker receives `204 No Content` for a job outside its
  allowed pool, then verify the correct pool's worker can claim and complete it
- verify queue depth/wait, worker capacity, async pool placement, terminal
  duration, and callback metrics through `/metrics`
- repeat the same scenario after restarting Fairlead with SQLite job storage to
  confirm durable queued jobs still respect worker pool policy after restart

**Why deferred:** This requires real DGX hosts, SSH reachability, process
lifecycle control, optional SQLite persistence, and fake worker scripts running
on both nodes. It should stay opt-in and outside the default Rust test suite.

### `phase_7d_strict_worker_pool_registration_process_e2e`

Add an opt-in process-level e2e for strict worker pool validation with local
fake workers:

- start Fairlead with explicit `POOLS_JSON` and `STRICT_WORKER_POOLS=true`
- register a worker with a configured pool and verify `200 OK`
- register a worker with an unknown pool and verify `400 Bad Request`
- verify the rejected worker is absent from `GET /v1/workers`
- start with no explicit `POOLS_JSON`, keep `STRICT_WORKER_POOLS=true`, omit
  `pool` from the worker registration request, and verify the derived `default`
  pool is accepted
- start with explicit `POOLS_JSON` that omits `default`, keep
  `STRICT_WORKER_POOLS=true`, omit `pool`, and verify the defaulted registration
  is rejected
- start with `STRICT_WORKER_POOLS` unset or false and verify an ad hoc pool is
  still accepted
- verify logs or `/metrics` expose enough context to diagnose strict-mode pool
  rejection during demos

**Why deferred:** Unit and in-process endpoint tests cover the validation
boundary. This e2e needs binary lifecycle, environment setup, port allocation,
HTTP client orchestration, and log capture.

### `phase_7d_strict_workload_pool_startup_process_e2e`

Add an opt-in process-level e2e for strict workload pool validation:

- start Fairlead with `STRICT_WORKLOAD_POOLS=true` and no
  `WORKLOAD_POOLS_JSON`; verify the process exits before binding and the error
  names the missing explicit policy
- start Fairlead with `STRICT_WORKLOAD_POOLS=true` and partial
  `WORKLOAD_POOLS_JSON`; verify the process exits before binding and the error
  names the missing workload policies
- start Fairlead with `STRICT_WORKLOAD_POOLS=false` or unset and the same
  partial `WORKLOAD_POOLS_JSON`; verify the process starts and omitted
  workloads remain permissive
- start Fairlead with `STRICT_WORKLOAD_POOLS=true`, `BACKENDS_JSON` using
  derived pools, and complete `WORKLOAD_POOLS_JSON`; verify the process starts
  and logs `strict_workload_pools=true`
- issue sync and async HTTP calls against that complete strict config to verify
  Phase 7B and 7C placement behavior is unchanged after startup validation
- scrape `/metrics` after those calls and verify pool labels still reflect the
  configured policy

**Why deferred:** The unit tests cover parser behavior and derived-pool
interaction. This e2e needs binary lifecycle, port allocation, startup failure
assertions, log capture, fake backends, fake workers, and real HTTP calls.

### `phase_7d_dgx_strict_pool_registration_smoke_test`

Add an opt-in DGX Spark smoke test for strict worker pool validation:

- run Fairlead on the two-node DGX Spark setup with sanitized pool names for
  the local and peer node
- register fake workers from both DGX Spark nodes with configured pool names and
  verify both appear in `GET /v1/workers`
- attempt a typo pool registration from one node and verify Fairlead rejects it
  without changing the registered worker list
- submit an async job after the failed registration and verify only compatible,
  configured-pool workers can claim it
- scrape `/metrics` after the run and verify pool labels remain bounded to the
  configured names plus expected outcome labels

**Why deferred:** This requires the real two-node DGX Spark environment, SSH
reachability, fake worker lifecycle management on both nodes, and deployment
log collection.

### `phase_7d_future_cloud_overflow_pool_e2e_plan`

When cloud provider adapters and cloud overflow pools are implemented in a
future phase, add e2e coverage for mixed local/peer/cloud placement:

- configure ordered workload pools such as `local-gpu`, `peer-gpu`, and
  `cloud-overflow`
- verify local routing wins when local resources are healthy
- exhaust or mark local resources unavailable and verify Fairlead falls back to
  peer before cloud when policy orders pools that way
- exhaust local and peer capacity and verify Fairlead admits cloud overflow only
  for workloads whose policy allows that pool
- verify strict worker pool validation rejects accidental cloud pool typos
- verify cloud credentials, provider identifiers, cost labels, and rate-limit
  metadata are redacted or sanitized in logs and metrics

**Why deferred:** Fairlead does not yet have provider adapters, cloud worker
registration, cost/rate-limit policy, or a CI-safe cloud test fixture. This
belongs with the future cloud overflow phase rather than Phase 7D.

### `phase_8a_worker_lifecycle_process_e2e`

Add an opt-in process-level e2e for worker drain, reactivation, and
deregistration:

- start Fairlead with local fake workers and SQLite job storage
- register two workers for the same job type
- drain one worker and verify preview/claim uses the other worker
- reactivate the drained worker and verify it can claim new work again
- delete an idle worker and verify it disappears from `GET /v1/workers`
- claim a job, delete the busy worker, verify delete returns `202 Accepted`,
  then complete the held job successfully while the worker is draining
- renew a held lease after the busy worker has been deregistered into draining
  state
- report retryable and terminal failures after the busy worker has been
  deregistered into draining state
- verify a retryable failure from a draining worker is reassigned to another
  compatible worker rather than reclaimed by the draining worker
- verify repeated drain/reactivate/delete calls are safe when fake workers are
  concurrently polling
- restart Fairlead after worker lifecycle operations and verify documented
  in-memory worker registry behavior remains clear
- restart Fairlead with SQLite jobs while a worker was draining before restart,
  and verify pending/running job recovery remains understandable despite the
  intentionally in-memory worker registry
- scrape `/metrics` and verify worker availability includes
  `status="draining"` while drained workers are registered
- run the same lifecycle sequence against two DGX Spark nodes with one worker on
  each node and verify drain on the local node causes claims to move to the peer

**Why deferred:** The in-process tests cover the registry and endpoint behavior.
This e2e needs process lifecycle management, port allocation, fake worker
scripts, SQLite job storage, restart assertions, and optional DGX Spark access.

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
SQLite-backed failed callback attempt retry after registry restart. It also
covers recovery-loop delivery of pending SQLite callbacks and in-process
in-flight callback de-duplication. Remaining deferred tests:

- Callback retry recovery across a full Fairlead OS-process restart.
- Duplicate callback behavior when Fairlead crashes after the receiver handles
  the callback but before Fairlead records delivery success.
- Process-level e2e callback delivery with an actual Fairlead process and
  callback receiver.
- Deployment-level callback delivery on Thor/Loki.

**Why deferred:** These need a larger process/deployment harness or controlled
crash injection. The current Stay tests cover the durable registry boundary
directly.
