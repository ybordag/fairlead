# Phase 6F Stay

## Goal

Close the async job loop for callers that want Fairlead to push terminal status
instead of requiring polling forever.

## Scope

Stay includes:

- Callback delivery when a terminal job has `callback_url`.
- Callback delivery metrics separate from compute job state.
- Retry and timeout policy for callback delivery.
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

Remaining Stay work:

- Decide whether callback state should become durable in SQLite.
- Add process-level or demo-level callback e2e coverage.
