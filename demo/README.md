# Bluewater Local Demo

This demo runs Fairlead against two tiny OpenAI-compatible mock backends named
`spark-a` and `spark-b`. It does not require GPUs, vLLM, Docker, or external
provider credentials.

Run it from the repo root:

```bash
./demo/run_bluewater_demo.sh
```

The script starts:

- mock backend `spark-a` on `127.0.0.1:18101`
- mock backend `spark-b` on `127.0.0.1:18102`
- Fairlead on `127.0.0.1:17000`

Ports can be overridden:

```bash
FAIRLEAD_DEMO_SPARK_A_PORT=19101 \
FAIRLEAD_DEMO_SPARK_B_PORT=19102 \
FAIRLEAD_DEMO_PORT=19000 \
./demo/run_bluewater_demo.sh
```

## What It Shows

The runner performs assertions for:

1. `spark-a` origin routes to `spark-a`.
2. `spark-b` origin routes to `spark-b`.
3. `spark-a` returns one upstream `500`, and Fairlead retries `spark-b` in the
   same request.
4. `spark-a` is healthy again but its circuit is still open, so Fairlead falls
   back to `spark-b`.
5. After cooldown, `spark-a` recovers through the half-open request path.
6. `/metrics` exposes request, retry, fallback, and circuit-state counters.
7. JSON tracing logs include `request completed` routing events.

The script writes logs to:

```text
target/bluewater-demo/
```

Those logs are generated artifacts and should not be committed.
