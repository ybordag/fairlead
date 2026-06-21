#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_A_PORT="${FAIRLEAD_DEMO_SPARK_A_PORT:-18101}"
SPARK_B_PORT="${FAIRLEAD_DEMO_SPARK_B_PORT:-18102}"
FAIRLEAD_PORT="${FAIRLEAD_DEMO_PORT:-17000}"
LOG_DIR="${ROOT_DIR}/target/routing-demo"
FAIRLEAD_LOG="${LOG_DIR}/fairlead.log"

mkdir -p "${LOG_DIR}"

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

wait_for_http() {
  local url="$1"
  local label="$2"
  for _ in $(seq 1 100); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for ${label}: ${url}" >&2
  return 1
}

set_mode() {
  local port="$1"
  local mode="$2"
  curl -fsS "http://127.0.0.1:${port}/control/mode" \
    -H 'content-type: application/json' \
    -d "{\"mode\":\"${mode}\"}" >/dev/null
}

report_resource() {
  local node="$1"
  local backend="$2"
  local reserved_vram_mb="$3"
  local load="$4"
  curl -fsS "http://127.0.0.1:${FAIRLEAD_PORT}/v1/resources/report" \
    -H 'content-type: application/json' \
    -d "{\"node_id\":\"${node}\",\"backend_id\":\"${backend}\",\"total_vram_mb\":64000,\"reserved_vram_mb\":${reserved_vram_mb},\"current_load\":${load}}" >/dev/null
}

chat() {
  local origin="$1"
  local thread="$2"
  curl -fsS "http://127.0.0.1:${FAIRLEAD_PORT}/v1/chat/completions" \
    -H 'content-type: application/json' \
    -H "X-Fairlead-Origin-Node: ${origin}" \
    -H "X-Fairlead-Thread-Id: ${thread}" \
    -H "X-Request-Id: demo-${thread}" \
    -d '{"model":"mock","messages":[{"role":"user","content":"route me"}]}'
}

assert_source() {
  local response="$1"
  local expected="$2"
  RESPONSE="${response}" EXPECTED="${expected}" python3 - <<'PY'
import json
import os

body = json.loads(os.environ["RESPONSE"])
actual = body["fairlead_demo"]["source"]
expected = os.environ["EXPECTED"]
if actual != expected:
    raise SystemExit(f"expected source {expected}, got {actual}: {body}")
print(f"  ok: routed to {actual}")
PY
}

print_metric() {
  local pattern="$1"
  local label="$2"
  echo "  ${label}:"
  curl -fsS "http://127.0.0.1:${FAIRLEAD_PORT}/metrics" | grep "${pattern}" || true
}

echo "Starting mock OpenAI-compatible backends..."
python3 "${ROOT_DIR}/demo/mock_openai_backend.py" --node spark-a --port "${SPARK_A_PORT}" \
  >"${LOG_DIR}/spark-a.log" 2>&1 &
PIDS+=("$!")
python3 "${ROOT_DIR}/demo/mock_openai_backend.py" --node spark-b --port "${SPARK_B_PORT}" \
  >"${LOG_DIR}/spark-b.log" 2>&1 &
PIDS+=("$!")

wait_for_http "http://127.0.0.1:${SPARK_A_PORT}/control/state" "spark-a mock backend"
wait_for_http "http://127.0.0.1:${SPARK_B_PORT}/control/state" "spark-b mock backend"

BACKENDS_JSON="$(cat <<JSON
[
  {
    "id": "spark-a-vllm",
    "url": "http://127.0.0.1:${SPARK_A_PORT}/v1",
    "node_id": "spark-a",
    "pool": "local-llm",
    "workloads": ["chat_completions", "embeddings"]
  },
  {
    "id": "spark-b-vllm",
    "url": "http://127.0.0.1:${SPARK_B_PORT}/v1",
    "node_id": "spark-b",
    "pool": "local-llm",
    "workloads": ["chat_completions", "embeddings"]
  }
]
JSON
)"

echo "Starting Fairlead on http://127.0.0.1:${FAIRLEAD_PORT}..."
(
  cd "${ROOT_DIR}"
  PORT="${FAIRLEAD_PORT}" \
  BACKENDS_JSON="${BACKENDS_JSON}" \
  CIRCUIT_FAILURE_THRESHOLD=1 \
  CIRCUIT_COOLDOWN_SECS=2 \
  HEALTH_PROBE_INTERVAL_SECS=60 \
  RESOURCE_AWARE_ROUTING=true \
  CHAT_COMPLETIONS_REQUIRED_VRAM_MB=1024 \
  EMBEDDINGS_REQUIRED_VRAM_MB=512 \
  PRIORITY_REALTIME_LIMIT=8 \
  PRIORITY_BATCH_LIMIT=4 \
  PRIORITY_BACKGROUND_LIMIT=2 \
  LOG_FORMAT=json \
  LOG_LEVEL=info \
  cargo run
) >"${FAIRLEAD_LOG}" 2>&1 &
PIDS+=("$!")

wait_for_http "http://127.0.0.1:${FAIRLEAD_PORT}/health" "Fairlead"
report_resource "spark-a" "spark-a-vllm" 16000 0.20
report_resource "spark-b" "spark-b-vllm" 16000 0.20

echo
echo "1. Same-node locality: spark-a origin prefers spark-a"
response="$(chat "spark-a" "local-a")"
assert_source "${response}" "spark-a"

echo
echo "2. Same-node locality: spark-b origin prefers spark-b"
response="$(chat "spark-b" "local-b")"
assert_source "${response}" "spark-b"

echo
echo "3. Resource-aware fallback: spark-a lacks reported VRAM headroom, so spark-b handles it"
report_resource "spark-a" "spark-a-vllm" 63500 0.95
response="$(chat "spark-a" "resource-a")"
assert_source "${response}" "spark-b"
report_resource "spark-a" "spark-a-vllm" 16000 0.20

echo
echo "4. Same-request retry: spark-a fails once, Fairlead retries spark-b"
set_mode "${SPARK_A_PORT}" "fail_once"
response="$(chat "spark-a" "retry-a")"
assert_source "${response}" "spark-b"

echo
echo "5. Circuit-open fallback: spark-a is healthy again, but its circuit is still open"
response="$(chat "spark-a" "fallback-a")"
assert_source "${response}" "spark-b"

echo
echo "6. Recovery: after cooldown, spark-a is tried again and succeeds"
sleep 3
response="$(chat "spark-a" "recovered-a")"
assert_source "${response}" "spark-a"

echo
echo "7. Metrics show the routing story"
print_metric 'fairlead_requests_total' 'requests'
print_metric 'fairlead_retries_total' 'same-request retries'
print_metric 'fairlead_fallbacks_total' 'fallback selections'
print_metric 'fairlead_circuit_state' 'circuit state'
print_metric 'fairlead_resource_vram_available_mb' 'resource headroom'
print_metric 'fairlead_priority_limit' 'priority limits'
print_metric 'fairlead_priority_in_flight' 'priority in-flight'

echo
echo "8. Structured trace sample"
grep '"request completed"' "${FAIRLEAD_LOG}" | tail -n 3 || true

echo
echo "Routing demo completed successfully."
echo "Logs are in ${LOG_DIR}"
