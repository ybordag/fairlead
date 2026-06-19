# Deferred Tests

Tests that were identified during phase reviews but not implemented at the time.
Each entry names the test, describes what it covers, and notes why it was deferred.
Pick these up before promoting the relevant module to production use.

---

## `src/config.rs`

### `invalid_circuit_failure_threshold_returns_err`
`Config::from_env()` should return `Err` when `CIRCUIT_FAILURE_THRESHOLD` is not a
valid `u32` (e.g. `"abc"` or `"-1"`).

**Why deferred:** The same parse pattern is already tested for `PORT`
(`invalid_port_returns_err`). Low risk — copy-paste error in the error message is
the only realistic failure mode.

---

### `invalid_circuit_cooldown_secs_returns_err`
Same as above for `CIRCUIT_COOLDOWN_SECS`.

---

### `invalid_health_probe_interval_returns_err`
Same as above for `HEALTH_PROBE_INTERVAL_SECS`.

---

## `src/router/fallback.rs`

### `preferred_at_index_zero_falls_back_when_open`
`select_backend` with `preferred = Some(0)` where `backends[0]` has an open
circuit should fall back to `backends[1]`.

Currently we only test the reverse direction (`preferred = Some(1)`, falls back to
`Some(0)`). The code is symmetric but this specific path — where the preferred index
equals the first chain position — has a subtle skip-check interaction worth verifying
explicitly.

**Why deferred:** Low risk given the unit tests cover the adjacent cases. One
additional test.

---

## `src/metrics.rs`

### `metrics_content_type_is_prometheus_format`
`GET /metrics` response must carry `content-type: text/plain; version=0.0.4`.
Prometheus scrapers use this header to select the correct parser.

**Why deferred:** Functional correctness was prioritised; header verification is
low risk but worth adding before wiring a real Prometheus scraper.

---

### `metrics_escapes_quotes_in_backend_label`
A backend URL containing a double-quote character (`"`) must be escaped to `\"`
in the Prometheus label so the output remains valid.

The sanitisation (`replace('"', "\\\"")`) is present in the code but the path is
never exercised by any test.

**Why deferred:** Realistic backend URLs don't contain quotes; added defensively.

---

## `src/proxy/mod.rs`

### `affinity_follows_circuit_after_connection_failure`
The connection-error equivalent of `affinity_follows_circuit_after_5xx_degradation`.

When the preferred backend becomes unreachable (reqwest returns `Err`, Fairlead
returns 502), affinity is intentionally not updated. The thread keeps retrying the
unreachable backend on each request, accumulating failures until the circuit opens,
at which point `select_backend` falls back to a healthy backend and the affinity
map is updated.

The 5xx path is tested end-to-end. The 502/connection-error path shares the same
`record_failure()` + early-return code but is not explicitly tested.

**Why deferred:** Behaviour is structurally identical to the 5xx path; single
additional integration test.

---

### `embeddings_uses_fallback_chain_when_first_backend_open`
Explicit integration test that `POST /v1/embeddings` also benefits from the
fallback chain when the first backend's circuit is open.

Currently the fallback tests only exercise `POST /v1/chat/completions`.
Both endpoints call the same `forward()` function, so this is implicit coverage —
but explicit coverage guards against a future refactor that accidentally diverges
the two handlers.

**Why deferred:** Implicit coverage is strong; explicit test is defensive.

---

### `affinity_preserved_across_streaming_requests`
`X-Fairlead-Thread-Id` with `"stream": true` should record and respect affinity
the same way non-streaming requests do. Both paths go through `forward()`, so this
is implicit — but worth one explicit test to catch any future split of the handlers.

**Why deferred:** Same reasoning as embeddings fallback above.

---

### `no_thread_id_does_not_pollute_affinity_map`
A request sent without `X-Fairlead-Thread-Id` must not insert any entry into the
affinity map. Currently verified implicitly (most proxy tests don't include the
header and affinity tests start from a known-empty map), but never asserted
directly.

**Why deferred:** Implicit coverage; map insertion only happens in one explicit
`if let Some(ref tid)` branch.
