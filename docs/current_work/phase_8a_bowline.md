# Phase 8A: Bowline

Branch: `bowline`

Goal: add worker lifecycle controls so operators and workers can drain,
reactivate, and deregister async workers without dropping held jobs.

## Completed So Far

- Added worker `draining` state to worker snapshots and `/v1/workers` output.
- Added `POST /v1/workers/{id}/drain`.
- Added `POST /v1/workers/{id}/reactivate`.
- Added `DELETE /v1/workers/{id}`.
- Idle deregistration removes the worker immediately.
- Busy deregistration marks the worker draining and returns `202 Accepted`,
  keeping the worker registered so held leases can renew, complete, or fail.
- Scheduler preview and worker claims skip draining workers for new work.
- Worker availability metrics now report `status="draining"`.

## Remaining

- Run the final validation gate before PR.

## Semantics

Draining is a graceful stop for new work, not a hard shutdown. A draining worker
can still heartbeat and report results for jobs it already holds. This avoids
the dangerous failure mode where Fairlead forgets a worker while a lease is
still running.

`DELETE /v1/workers/{id}` is intentionally conservative:

- idle worker: remove it from the registry and return `204 No Content`
- busy worker: mark it draining and return the worker snapshot with
  `202 Accepted`
- missing worker: return `404 Not Found`
