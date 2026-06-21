#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAIRLEAD_PORT="${FAIRLEAD_ASYNC_DEMO_PORT:-17010}"
CALLBACK_PORT="${FAIRLEAD_ASYNC_DEMO_CALLBACK_PORT:-18110}"
LOG_DIR="${ROOT_DIR}/target/async-demo"
FAIRLEAD_LOG="${LOG_DIR}/fairlead.log"
CALLBACK_LOG="${LOG_DIR}/callback.log"
CALLBACK_PAYLOAD="${LOG_DIR}/callback-payload.json"
JOB_DB="${LOG_DIR}/fairlead-jobs.sqlite3"

mkdir -p "${LOG_DIR}"
rm -f "${FAIRLEAD_LOG}" "${CALLBACK_LOG}" "${CALLBACK_PAYLOAD}" "${JOB_DB}" "${JOB_DB}-shm" "${JOB_DB}-wal"

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

wait_for_callback() {
  for _ in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:${CALLBACK_PORT}/state" | python3 -c 'import json, sys; raise SystemExit(0 if json.load(sys.stdin)["count"] > 0 else 1)' >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for callback delivery" >&2
  return 1
}

post_json() {
  local url="$1"
  local body="$2"
  curl -fsS "${url}" -H 'content-type: application/json' -d "${body}"
}

assert_json_field() {
  local body="$1"
  local expr="$2"
  local expected="$3"
  BODY="${body}" EXPR="${expr}" EXPECTED="${expected}" python3 - <<'PY'
import json
import os

body = json.loads(os.environ["BODY"])
value = body
for part in os.environ["EXPR"].split("."):
    value = value[part]
actual = str(value)
expected = os.environ["EXPECTED"]
if actual != expected:
    raise SystemExit(f"expected {os.environ['EXPR']}={expected}, got {actual}: {body}")
print(f"  ok: {os.environ['EXPR']}={actual}")
PY
}

extract_json_field() {
  local body="$1"
  local expr="$2"
  BODY="${body}" EXPR="${expr}" python3 - <<'PY'
import json
import os

body = json.loads(os.environ["BODY"])
value = body
for part in os.environ["EXPR"].split("."):
    value = value[part]
print(value)
PY
}

print_metric() {
  local pattern="$1"
  local label="$2"
  echo "  ${label}:"
  curl -fsS "http://127.0.0.1:${FAIRLEAD_PORT}/metrics" | grep "${pattern}" || true
}

echo "Starting callback receiver..."
python3 "${ROOT_DIR}/demo/async_callback_receiver.py" \
  --port "${CALLBACK_PORT}" \
  --payload-file "${CALLBACK_PAYLOAD}" \
  >"${CALLBACK_LOG}" 2>&1 &
PIDS+=("$!")
wait_for_http "http://127.0.0.1:${CALLBACK_PORT}/state" "callback receiver"

echo "Starting Fairlead async job service on http://127.0.0.1:${FAIRLEAD_PORT}..."
(
  cd "${ROOT_DIR}"
  PORT="${FAIRLEAD_PORT}" \
  JOB_STORE=sqlite \
  JOB_DB_PATH="${JOB_DB}" \
  CALLBACK_MAX_ATTEMPTS=3 \
  CALLBACK_TIMEOUT_SECS=2 \
  CALLBACK_RETRY_DELAY_MS=100 \
  LOG_FORMAT=json \
  LOG_LEVEL=info \
  cargo run
) >"${FAIRLEAD_LOG}" 2>&1 &
PIDS+=("$!")
wait_for_http "http://127.0.0.1:${FAIRLEAD_PORT}/health" "Fairlead"

echo
echo "1. Register a vision worker"
worker_response="$(post_json "http://127.0.0.1:${FAIRLEAD_PORT}/v1/workers/register" '{
  "id": "vision-worker",
  "endpoint_url": "http://127.0.0.1:19000",
  "node_id": "spark-a",
  "job_types": ["vision_analysis"],
  "max_concurrent_jobs": 1,
  "available_vram_mb": 24000
}')"
assert_json_field "${worker_response}" "worker.id" "vision-worker"
assert_json_field "${worker_response}" "worker.available_job_slots" "1"

echo
echo "2. Submit an async vision job with a callback URL"
submit_response="$(post_json "http://127.0.0.1:${FAIRLEAD_PORT}/v1/jobs" "{
  \"type\": \"vision_analysis\",
  \"priority\": \"batch\",
  \"payload\": {
    \"image_ref\": \"demo://plant.jpg\",
    \"requested_checks\": [\"plant_health\", \"pest_detection\"]
  },
  \"callback_url\": \"http://127.0.0.1:${CALLBACK_PORT}/callback\"
}")"
job_id="$(extract_json_field "${submit_response}" "job.id")"
assert_json_field "${submit_response}" "job.status" "queued"
assert_json_field "${submit_response}" "job.priority" "batch"

echo
echo "3. Preview and claim the queued job"
preview_response="$(curl -fsS "http://127.0.0.1:${FAIRLEAD_PORT}/v1/scheduler/preview")"
assert_json_field "${preview_response}" "assignment.job.id" "${job_id}"
assert_json_field "${preview_response}" "assignment.worker.id" "vision-worker"

claim_response="$(curl -fsS -X POST "http://127.0.0.1:${FAIRLEAD_PORT}/v1/workers/vision-worker/claim")"
assert_json_field "${claim_response}" "job.id" "${job_id}"
assert_json_field "${claim_response}" "job.status" "running"
assert_json_field "${claim_response}" "job.lease.worker_id" "vision-worker"

echo
echo "4. Complete the job and receive the terminal callback"
complete_response="$(post_json "http://127.0.0.1:${FAIRLEAD_PORT}/v1/workers/vision-worker/jobs/${job_id}/complete" '{
  "result": {
    "summary": "healthy plant demo result",
    "detections": [
      {"label": "leaf", "confidence": 0.98},
      {"label": "no_visible_pests", "confidence": 0.91}
    ]
  }
}')"
assert_json_field "${complete_response}" "job.status" "complete"
wait_for_callback

callback_payload="$(cat "${CALLBACK_PAYLOAD}")"
assert_json_field "${callback_payload}" "job.id" "${job_id}"
assert_json_field "${callback_payload}" "job.status" "complete"
assert_json_field "${callback_payload}" "job.callback.status" "pending"

echo
echo "5. Poll Fairlead and verify callback state persisted as delivered"
for _ in $(seq 1 50); do
  job_response="$(curl -fsS "http://127.0.0.1:${FAIRLEAD_PORT}/v1/jobs/${job_id}")"
  if BODY="${job_response}" python3 -c 'import json, os, sys; body=json.loads(os.environ["BODY"]); sys.exit(0 if body["job"]["callback"]["status"] == "delivered" else 1)' >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
job_response="$(curl -fsS "http://127.0.0.1:${FAIRLEAD_PORT}/v1/jobs/${job_id}")"
assert_json_field "${job_response}" "job.callback.status" "delivered"
assert_json_field "${job_response}" "job.callback.last_http_status" "200"

echo
echo "6. Metrics show async queue, worker, duration, and callback outcomes"
print_metric 'fairlead_job_queue_depth' 'queue depth'
print_metric 'fairlead_workers' 'worker availability'
print_metric 'fairlead_worker_in_flight_jobs' 'worker in-flight'
print_metric 'fairlead_job_duration_seconds_count' 'job duration count'
print_metric 'fairlead_job_callbacks_total' 'callback delivery'

echo
echo "Async jobs demo completed successfully."
echo "Logs and callback payload are in ${LOG_DIR}"
