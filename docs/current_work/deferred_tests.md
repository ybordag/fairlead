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
