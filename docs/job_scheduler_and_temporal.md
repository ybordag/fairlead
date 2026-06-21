# Fairlead Job Scheduler and Temporal

This note captures the current design decision for image workflows and other
async compute jobs.

## Short Answer

Fairlead should build a compute job scheduler and manager. Temporal is not
needed early because the expected jobs are bounded compute tasks, not
long-running business workflows.

Fairlead can run for days, weeks, or indefinitely as a service while individual
jobs stay short-lived. If an image-processing job runs longer than a few
minutes, that is probably an execution failure, not a normal workflow that needs
durable multi-day orchestration.

## Boundaries

```text
k3s / Docker:
  run containers
  restart crashed services
  expose services
  apply node labels and GPU placement constraints

Fairlead:
  accept compute jobs
  queue by priority
  select workers/backends by health, node, workload, and resources
  track job attempts
  enforce timeouts and leases
  retry failed compute attempts
  expose job status
  deliver callbacks

Rhizome:
  own garden/user/domain state
  create VisionJob records
  interpret model results
  create incidents, interactions, and user-visible records
  reconcile pending domain work

Temporal, if added later:
  durable multi-step workflow orchestration
  long waits
  fanout/fanin
  cross-service retries and compensation
  recovery of product workflows after crashes
```

Fairlead should know where compute can run. It should not know what a plant
diagnosis means or which user-facing record should be created.

## Scheduler Model

The Fairlead scheduler should treat jobs as bounded state machines:

```text
pending
  -> leased/running
  -> succeeded
  -> failed
  -> cancelled
```

Failure paths:

```text
leased/running
  -> timed_out -> retry_pending or failed
  -> worker_lost -> retry_pending or failed
  -> cancelled
```

The key mechanism is a lease. When a worker starts a job, Fairlead records the
worker ID and a `lease_expires_at` timestamp. The worker must complete or
heartbeat before the lease expires. If it does not, Fairlead releases any
resource reservation and either retries the job on another worker or marks it
failed.

This avoids holding an open process relationship for days. Fairlead only stores
job state and watches bounded leases.

## Initial Job Scope

The async job system should include:

- `POST /v1/jobs` to submit a job and return `job_id`.
- `GET /v1/jobs/{id}` to read status.
- `DELETE /v1/jobs/{id}` to cancel queued or running work when supported.
- Worker registration and heartbeat.
- Priority levels: `realtime`, `batch`, and `background`.
- Job statuses: `queued`, `running`, `succeeded`, `failed`, `cancelled`.
- Attempt count, retry limit, timeout, lease expiration, and last error.
- Callback URL and callback delivery state.
- Resource reservation/release around running attempts.
- Retention and pruning for completed jobs.

The first useful workload is `vision_analysis`: a user-triggered image workflow
that should outrank background indexing but yield to realtime chat or retrieval.

## Persistence

An in-memory queue is acceptable for a prototype demo, but production-like
Fairlead should persist job state. The scheduler mostly needs durable state
transitions, not a massive distributed datastore.

Core records look relational:

```text
job_id
status
priority
workload_kind
worker_id
attempt_count
lease_expires_at
created_at
updated_at
input_ref
result_ref
error
callback_url
callback_status
```

Common scheduler operations must be atomic:

```text
claim the oldest pending high-priority job
mark it running
increment attempt_count
set lease_expires_at
reserve resources
```

Recommended persistence path:

- **SQLite first:** good for a local appliance, single Fairlead process, demos,
  and portfolio deployment. It is durable, inspectable, transactional, and does
  not require another service.
- **Postgres later:** better when multiple Fairlead instances need to coordinate
  safely. Row locking and transactions map well to job claiming and leases.
- **Redis optionally:** useful for fast queues, pub/sub, rate limits, or caches,
  but less ideal as the canonical job history and recovery database.
- **NATS JetStream or RabbitMQ optionally:** useful when broker semantics become
  more important than direct SQL-backed job state.
- **Kafka/Redpanda:** useful for high-throughput event logs and replay, likely
  overkill for Fairlead's early scheduler.
- **Cassandra:** not a good fit unless Fairlead becomes a massive distributed
  event store. It makes ordering, leases, transactions, and debugging harder than
  this project needs.

## When Temporal Becomes Worth It

Temporal is useful if Rhizome workflows become more complex than bounded compute
jobs. Signs that Temporal is justified:

- Steps wait for hours or days.
- A workflow fans out to many workers and collects partial results.
- Only failed branches should retry.
- Multiple services must be updated with compensation logic.
- Cancellations can happen halfway through a multi-step workflow.
- Recovery after service restart would otherwise require rebuilding a workflow
  engine inside Rhizome or Fairlead.

Until then, Fairlead should provide compute job orchestration, and Rhizome should
own the domain-level VisionJob state machine.

## Design Rule

Build Fairlead's scheduler as a compute control plane, not as a general-purpose
workflow engine.

