# Deferred Tests

Tests that are useful but intentionally deferred because they need heavier
fixtures, log capture, or a runnable demo harness. The low-risk unit and proxy
tests previously listed here have been implemented.

---

## `src/proxy/mod.rs`

### `structured_tracing_fields_are_emitted`

Capture tracing output for one successful request and one fallback/retry request.
Assert that the final request event includes:

- request ID
- workload
- origin node
- affinity key
- selected backend
- retry count
- fallback reason
- status
- outcome

**Why deferred:** Capturing `tracing_subscriber` output reliably in async tests
requires test-specific subscriber setup and isolation. The fields are currently
covered indirectly by compile-time validation of the tracing calls and by the
matching metrics assertions.

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
