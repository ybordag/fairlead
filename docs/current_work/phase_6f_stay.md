# Phase 6F Stay

## Goal

Close the async job loop for callers that want Fairlead to push terminal status
instead of requiring polling forever.

## Scope

Stay includes:

- Callback delivery when a terminal job has `callback_url`.
- Callback delivery metrics separate from compute job state.
- Retry and timeout policy for callback delivery.
- Durable callback delivery state for SQLite-backed restart recovery.
- Final async demo and documentation updates.

Stay does not include:

- Temporal integration.
- Multi-step workflow orchestration.
- Domain-specific Rhizome state transitions.

## First Slice

Implemented:

- Added asynchronous one-shot callback delivery for terminal jobs.
- Sends `{"job": ...}` to the submitted `callback_url`.
- Dispatches callbacks after successful completion, terminal failure,
  cancellation, and exhausted lease timeout failures.
- Does not dispatch callbacks for retryable failures that requeue a job.
- Added callback delivery metrics by job type, terminal status, outcome, and
  callback HTTP status.
- Added tests for successful completion callbacks, failed callback metrics,
  cancellation callbacks, and no callback on retryable requeue.

## Second Slice

Implemented:

- Added bounded callback retry policy.
- Added per-attempt callback timeout policy.
- Added config for callback delivery:
  - `CALLBACK_MAX_ATTEMPTS`, default `3`.
  - `CALLBACK_TIMEOUT_SECS`, default `5`.
  - `CALLBACK_RETRY_DELAY_MS`, default `250`.
- Records callback metrics for each delivery attempt, so transient failures
  before a later success remain visible.
- Retries failed callback attempts after non-2xx responses, request errors, or
  timeouts.
- Stops retrying after the first successful 2xx response.
- Added tests for transient failure followed by success, timeout handling, and
  callback config validation.

## Third Slice

Implemented:

- Added durable callback delivery state to each terminal job:
  - `pending` until a callback receives a 2xx response.
  - `delivered` after a successful callback.
  - attempt count, last attempt timestamp, delivered timestamp, last HTTP
    status, and last error.
- Persisted callback delivery state in the SQLite job store.
- Added a callback dispatcher that keeps an in-process in-flight guard so a
  recovery sweep does not dispatch the same callback twice concurrently.
- Added a recovery loop that scans pending callback jobs after startup and
  retries delivery.
- Preserved at-least-once callback semantics across restarts. If Fairlead
  crashes after the receiver handles a callback but before Fairlead records
  success, the callback can be sent again after restart.
- Added SQLite restart tests for:
  - pending callback delivery after registry restart.
  - failed callback attempt retry after registry restart.
- Updated storage migration coverage for `callback_state_json`.
- Added focused tests for recovery-loop callback delivery and in-process
  in-flight callback de-duplication.

## Fourth Slice

Implemented:

- Added `demo/run_async_jobs_demo.sh`, a local GPU-free async jobs demo.
- Added `demo/async_callback_receiver.py`, a tiny callback receiver used by the
  demo.
- The demo verifies worker registration, async job submission, scheduler
  preview, worker claim, completion, callback receipt, persisted delivered
  callback state, and queue/worker/duration/callback metrics.

Remaining Stay work:

- None. Phase 6F is ready for final PR checks.
