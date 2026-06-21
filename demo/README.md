# Local Demos

Fairlead includes GPU-free demos that run entirely on localhost:

- `run_routing_demo.sh` shows synchronous OpenAI-compatible routing.
- `run_async_jobs_demo.sh` shows async job submission, worker-pull execution,
  terminal callback delivery, SQLite job state, and metrics.

Both demos source `shared_pool_policy.sh`, which defines one pool vocabulary for
sync and async work:

- `local-llm` for OpenAI-compatible chat and embedding backends.
- `vision` for user-triggered vision jobs.
- `batch` for background and batch work.

The shared policy enables `STRICT_WORKLOAD_POOLS=true` and
`STRICT_WORKER_POOLS=true`, so the demos fail fast if workload policy is
incomplete or a worker registers with a misspelled pool. This is the same shape
recommended for production-like demos, while local experiments can still leave
the strict flags unset.

---

## Routing Demo

This demo runs Fairlead against two tiny OpenAI-compatible mock backends named
`spark-a` and `spark-b`. It does not require GPUs, vLLM, Docker, or external
provider credentials.

Run it from the repo root:

```bash
./demo/run_routing_demo.sh
```

The script starts:

- mock backend `spark-a` on `127.0.0.1:18101`
- mock backend `spark-b` on `127.0.0.1:18102`
- Fairlead on `127.0.0.1:17000`

The mock backends are both in the `local-llm` pool. The routing demo therefore
shows locality, affinity, resource pressure, circuit fallback, retry, and
metrics within one eligible workload pool.

Ports can be overridden:

```bash
FAIRLEAD_DEMO_SPARK_A_PORT=19101 \
FAIRLEAD_DEMO_SPARK_B_PORT=19102 \
FAIRLEAD_DEMO_PORT=19000 \
./demo/run_routing_demo.sh
```

## What It Shows

The runner performs assertions for:

1. `spark-a` origin routes to `spark-a`.
2. `spark-b` origin routes to `spark-b`.
3. Resource-aware routing skips `spark-a` after it reports insufficient VRAM
   headroom, so a `spark-a` origin request falls back to `spark-b`.
4. `spark-a` returns one upstream `500`, and Fairlead retries `spark-b` in the
   same request.
5. `spark-a` is healthy again but its circuit is still open, so Fairlead falls
   back to `spark-b`.
6. After cooldown, `spark-a` recovers through the half-open request path.
7. `/metrics` exposes request, retry, fallback, circuit-state, resource, and
   priority-admission metrics.
8. JSON tracing logs include `request completed` routing events.

The script writes logs to:

```text
target/routing-demo/
```

Those logs are generated artifacts and should not be committed.

---

## Async Jobs Demo

This demo runs Fairlead with SQLite-backed async job state and a tiny local
callback receiver. It does not require GPUs, vLLM, Docker, or external provider
credentials.

Run it from the repo root:

```bash
./demo/run_async_jobs_demo.sh
```

The script starts:

- callback receiver on `127.0.0.1:18110`
- Fairlead on `127.0.0.1:17010`
- SQLite job store at `target/async-demo/fairlead-jobs.sqlite3`

The fake worker registers in the `vision` pool. The async demo therefore shows
worker-pull placement through the same `WORKLOAD_POOLS_JSON` vocabulary used by
the synchronous routing demo.

Ports can be overridden:

```bash
FAIRLEAD_ASYNC_DEMO_PORT=19010 \
FAIRLEAD_ASYNC_DEMO_CALLBACK_PORT=19110 \
./demo/run_async_jobs_demo.sh
```

## What It Shows

The runner performs assertions for:

1. A `vision_analysis` worker registers with capacity metadata.
2. A batch `vision_analysis` job is submitted with a callback URL.
3. `/v1/scheduler/preview` selects the compatible worker without mutating state.
4. The worker claims the job and receives a lease.
5. The worker completes the job with a result payload.
6. The callback receiver gets the terminal job payload.
7. Fairlead records the callback as delivered in the SQLite-backed job state.
8. `/metrics` exposes queue, worker, duration, and callback metrics.

The script writes logs, the SQLite DB, and the received callback payload to:

```text
target/async-demo/
```

Those files are generated artifacts and should not be committed.
