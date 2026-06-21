# Deferred Tests

Tests that are useful but intentionally deferred because they need heavier
fixtures, log capture, or a runnable demo harness. The low-risk unit and proxy
tests previously listed here have been implemented.

---

## Demo Harness

### `small_cluster_demo_exercises_routing_story`

Once the local mock demo exists, add a smoke test or script assertion that starts
two mock OpenAI-compatible backends named `spark-a` and `spark-b`, then verifies:

- same-node preference
- peer fallback when same-node circuit is open
- same-request retry after upstream failure
- circuit recovery
- metrics output for request, fallback, retry, and circuit state

**Why deferred:** The mock demo does not exist yet. This belongs with the
Bluewater small-cluster demo task rather than the current unit/integration test
cleanup.
