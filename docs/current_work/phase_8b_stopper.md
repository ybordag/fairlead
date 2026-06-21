# Phase 8B: Stopper

Branch: `stopper`

Goal: add explicit terminal-job retention and pruning so long-running Fairlead
processes can bound completed job history without weakening callback delivery.

## Completed So Far

- Added `JobRetentionPolicy` to `JobRegistry`.
- Added `JOB_RETENTION_SECS`, defaulting to 86400 seconds.
- Added `JOB_PRUNE_LIMIT`, defaulting to 1000 jobs per prune call.
- Added `POST /v1/jobs/prune`.
- Pruning removes only terminal jobs older than the configured retention age.
- Pruning skips terminal jobs with pending callbacks so callback delivery can
  continue.
- Pruning persists removed jobs to SQLite when `JOB_STORE=sqlite` is enabled.
- Added `fairlead_job_prunes_total{status}` metrics.
- Added tests for config parsing, retention age, per-call limits, pending
  callback protection, SQLite persistence, endpoint responses, and metrics.
- Added edge tests for delivered callback pruning, queued/running job
  preservation, and cumulative prune metrics across bounded prune calls.
- Completed the 8B test audit:
  - in-process tests cover config parsing, registry pruning policy, callback
    safety, queue/running preservation, endpoint response shape, metrics, and
    SQLite persistence
  - process-level restart, timing, callback receiver, large-database, and
    concurrency tests are documented in `deferred_tests.md`
  - background pruning is scoped to Phase 8D as a maintenance-loop user of the
    explicit 8B pruning policy

## Remaining

- Ready for PR.

## Semantics

Pruning is explicit in Phase 8B. Operators or later maintenance loops call
`POST /v1/jobs/prune`; Fairlead does not yet run a background pruning loop.
Background invocation belongs to Phase 8D.

Eligible jobs must be:

- terminal: `complete`, `failed`, or `cancelled`
- older than `JOB_RETENTION_SECS` based on terminal `updated_at`
- not waiting on callback delivery

The per-call `JOB_PRUNE_LIMIT` keeps one prune operation bounded. A later
background loop can call the same registry operation repeatedly if needed.
