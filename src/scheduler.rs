use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::{
    jobs::{
        CompleteJobRequest, FailJobRequest, FinishJobResult, JobFailure, JobRecord, JobRegistry,
        RenewJobLeaseResult,
    },
    metrics::AsyncPoolDecisionLabels,
    workers::{AcquireWorkerSlotResult, WorkerRegistry, WorkerSnapshot},
    AppState,
};

const DEFAULT_LEASE_DURATION_MS: u128 = 30_000;

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerPreview {
    pub job: JobRecord,
    pub worker: WorkerSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerPreviewResponse {
    pub assignment: SchedulerPreview,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobClaimResponse {
    pub job: JobRecord,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobResultResponse {
    pub job: JobRecord,
}

pub async fn preview_next_assignment_handler(State(state): State<AppState>) -> Response {
    match preview_next_assignment_with_policy(&state.jobs, &state.workers, &state.workload_pools)
        .await
    {
        Some(assignment) => Json(SchedulerPreviewResponse { assignment }).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

pub async fn claim_worker_job_handler(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
) -> Response {
    sweep_expired_leases(&state).await;

    let worker = match state.workers.try_acquire_slot(&worker_id).await {
        AcquireWorkerSlotResult::Acquired(worker) => worker,
        AcquireWorkerSlotResult::AtCapacity(_) => {
            return (StatusCode::CONFLICT, "worker is at capacity").into_response();
        }
        AcquireWorkerSlotResult::Stale(_) => {
            return (StatusCode::CONFLICT, "worker is stale").into_response();
        }
        AcquireWorkerSlotResult::NotFound => {
            return (StatusCode::NOT_FOUND, "worker not found").into_response();
        }
    };

    match state
        .jobs
        .claim_next_for_worker_in_pool(
            &worker.id,
            &worker.job_types,
            &worker.pool,
            &state.workload_pools,
            DEFAULT_LEASE_DURATION_MS,
        )
        .await
    {
        Some(job) => {
            record_async_pool_selected(&state, &worker, &job).await;
            Json(JobClaimResponse { job }).into_response()
        }
        None => {
            state.workers.release_slot(&worker.id).await;
            record_async_pool_no_compatible_job(&state, &worker).await;
            StatusCode::NO_CONTENT.into_response()
        }
    }
}

pub async fn renew_worker_job_lease_handler(
    State(state): State<AppState>,
    Path((worker_id, job_id)): Path<(String, String)>,
) -> Response {
    let Some(worker) = state.workers.get(&worker_id).await else {
        return (StatusCode::NOT_FOUND, "worker not found").into_response();
    };
    if worker.stale {
        return (StatusCode::CONFLICT, "worker is stale").into_response();
    }

    sweep_expired_leases(&state).await;

    match state
        .jobs
        .renew_lease(&job_id, &worker.id, DEFAULT_LEASE_DURATION_MS)
        .await
    {
        RenewJobLeaseResult::Renewed(job) => Json(JobClaimResponse { job }).into_response(),
        RenewJobLeaseResult::NotRunning(job) => {
            (StatusCode::CONFLICT, Json(JobClaimResponse { job })).into_response()
        }
        RenewJobLeaseResult::LeaseNotHeld(job) => {
            (StatusCode::CONFLICT, Json(JobClaimResponse { job })).into_response()
        }
        RenewJobLeaseResult::Expired(job) => {
            (StatusCode::CONFLICT, Json(JobClaimResponse { job })).into_response()
        }
        RenewJobLeaseResult::NotFound => (StatusCode::NOT_FOUND, "job not found").into_response(),
    }
}

pub async fn complete_worker_job_handler(
    State(state): State<AppState>,
    Path((worker_id, job_id)): Path<(String, String)>,
    Json(request): Json<CompleteJobRequest>,
) -> Response {
    let Some(worker) = state.workers.get(&worker_id).await else {
        return (StatusCode::NOT_FOUND, "worker not found").into_response();
    };
    if worker.stale {
        return (StatusCode::CONFLICT, "worker is stale").into_response();
    }

    sweep_expired_leases(&state).await;

    match state
        .jobs
        .complete_lease(&job_id, &worker.id, request.result)
        .await
    {
        FinishJobResult::Completed(job) => {
            state.workers.release_slot(&worker.id).await;
            state.callback_dispatcher.dispatch(
                state.client.clone(),
                state.metrics.clone(),
                state.callback_policy,
                state.jobs.clone(),
                job.id.clone(),
            );
            Json(JobResultResponse { job }).into_response()
        }
        FinishJobResult::NotRunning(job)
        | FinishJobResult::LeaseNotHeld(job)
        | FinishJobResult::Expired(job) => {
            (StatusCode::CONFLICT, Json(JobResultResponse { job })).into_response()
        }
        FinishJobResult::NotFound => (StatusCode::NOT_FOUND, "job not found").into_response(),
        FinishJobResult::Requeued(_) | FinishJobResult::Failed(_) => {
            unreachable!("completion cannot requeue or fail a job")
        }
    }
}

pub async fn fail_worker_job_handler(
    State(state): State<AppState>,
    Path((worker_id, job_id)): Path<(String, String)>,
    Json(request): Json<FailJobRequest>,
) -> Response {
    let Some(worker) = state.workers.get(&worker_id).await else {
        return (StatusCode::NOT_FOUND, "worker not found").into_response();
    };
    if worker.stale {
        return (StatusCode::CONFLICT, "worker is stale").into_response();
    }
    let error = request.error.trim();
    if error.is_empty() {
        return (StatusCode::BAD_REQUEST, "error cannot be empty").into_response();
    }

    sweep_expired_leases(&state).await;

    let failure = JobFailure {
        message: error.to_string(),
        retryable: request.retryable,
    };
    match state.jobs.fail_lease(&job_id, &worker.id, failure).await {
        FinishJobResult::Requeued(job) => {
            state.workers.release_slot(&worker.id).await;
            Json(JobResultResponse { job }).into_response()
        }
        FinishJobResult::Failed(job) => {
            state.workers.release_slot(&worker.id).await;
            state.callback_dispatcher.dispatch(
                state.client.clone(),
                state.metrics.clone(),
                state.callback_policy,
                state.jobs.clone(),
                job.id.clone(),
            );
            Json(JobResultResponse { job }).into_response()
        }
        FinishJobResult::NotRunning(job)
        | FinishJobResult::LeaseNotHeld(job)
        | FinishJobResult::Expired(job) => {
            (StatusCode::CONFLICT, Json(JobResultResponse { job })).into_response()
        }
        FinishJobResult::NotFound => (StatusCode::NOT_FOUND, "job not found").into_response(),
        FinishJobResult::Completed(_) => unreachable!("failure cannot complete a job"),
    }
}

pub async fn sweep_expired_leases(state: &AppState) {
    let report = state.jobs.requeue_expired_leases().await;
    for worker_id in report.released_workers {
        state.workers.release_slot(&worker_id).await;
    }
    for job in report.failed_jobs {
        state.callback_dispatcher.dispatch(
            state.client.clone(),
            state.metrics.clone(),
            state.callback_policy,
            state.jobs.clone(),
            job.id,
        );
    }
}

#[cfg(test)]
pub async fn preview_next_assignment(
    jobs: &JobRegistry,
    workers: &WorkerRegistry,
) -> Option<SchedulerPreview> {
    let workload_pools = crate::config::WorkloadPoolPolicy::default();
    preview_next_assignment_with_policy(jobs, workers, &workload_pools).await
}

pub async fn preview_next_assignment_with_policy(
    jobs: &JobRegistry,
    workers: &WorkerRegistry,
    workload_pools: &crate::config::WorkloadPoolPolicy,
) -> Option<SchedulerPreview> {
    preview_from_snapshots(
        jobs.queued_jobs_by_priority().await,
        workers.list().await,
        workload_pools,
    )
}

fn preview_from_snapshots(
    queued_jobs: Vec<JobRecord>,
    workers: Vec<WorkerSnapshot>,
    workload_pools: &crate::config::WorkloadPoolPolicy,
) -> Option<SchedulerPreview> {
    for job in queued_jobs {
        if let Some(worker) = workers.iter().find(|worker| {
            !worker.stale
                && worker.job_types.contains(&job.kind)
                && workload_pools.allows(job.kind.as_str(), &worker.pool)
        }) {
            return Some(SchedulerPreview {
                job,
                worker: worker.clone(),
            });
        }
    }

    None
}

async fn record_async_pool_selected(state: &AppState, worker: &WorkerSnapshot, job: &JobRecord) {
    let workers = state.workers.list().await;
    let candidate_workers = count_candidate_workers_for_job(&workers, job, &state.workload_pools);
    state.metrics.record_async_pool_decision(
        AsyncPoolDecisionLabels {
            kind: job.kind.as_str().to_string(),
            priority: job.priority.as_str().to_string(),
            pool: worker.pool.clone(),
            worker: worker.id.clone(),
            node: worker.node_id.clone().unwrap_or_default(),
            outcome: "selected".into(),
        },
        candidate_workers,
        0,
    );
}

async fn record_async_pool_no_compatible_job(state: &AppState, worker: &WorkerSnapshot) {
    let queued_jobs = state.jobs.queued_jobs_by_priority().await;
    let mut blocked_by_kind_priority = std::collections::BTreeMap::new();
    for job in queued_jobs {
        if worker.job_types.contains(&job.kind)
            && !state.workload_pools.allows(job.kind.as_str(), &worker.pool)
        {
            *blocked_by_kind_priority
                .entry((
                    job.kind.as_str().to_string(),
                    job.priority.as_str().to_string(),
                ))
                .or_insert(0) += 1;
        }
    }

    if blocked_by_kind_priority.is_empty() {
        state.metrics.record_async_pool_decision(
            AsyncPoolDecisionLabels {
                kind: String::new(),
                priority: String::new(),
                pool: worker.pool.clone(),
                worker: worker.id.clone(),
                node: worker.node_id.clone().unwrap_or_default(),
                outcome: "no_compatible_job".into(),
            },
            0,
            0,
        );
        return;
    }

    for ((kind, priority), no_compatible_jobs) in blocked_by_kind_priority {
        state.metrics.record_async_pool_decision(
            AsyncPoolDecisionLabels {
                kind,
                priority,
                pool: worker.pool.clone(),
                worker: worker.id.clone(),
                node: worker.node_id.clone().unwrap_or_default(),
                outcome: "no_compatible_job".into(),
            },
            0,
            no_compatible_jobs,
        );
    }
}

fn count_candidate_workers_for_job(
    workers: &[WorkerSnapshot],
    job: &JobRecord,
    workload_pools: &crate::config::WorkloadPoolPolicy,
) -> usize {
    workers
        .iter()
        .filter(|worker| {
            !worker.stale
                && worker.job_types.contains(&job.kind)
                && workload_pools.allows(job.kind.as_str(), &worker.pool)
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        build_router,
        config::Priority,
        jobs::{JobKind, SubmitJobRequest},
        metrics::RoutingMetrics,
        priority::PriorityLimiter,
        resources::{ResourceRegistry, ResourceRoutingPolicy},
        router::SessionAffinity,
        workers::RegisterWorkerRequest,
        AppState,
    };
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::post,
        Json, Router,
    };
    use serde_json::{json, Value};
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    fn test_state(jobs: JobRegistry, workers: WorkerRegistry) -> AppState {
        test_state_with_callback_policy(jobs, workers, crate::callbacks::CallbackPolicy::default())
    }

    fn test_state_with_workload_pools(
        jobs: JobRegistry,
        workers: WorkerRegistry,
        workload_pools: crate::config::WorkloadPoolPolicy,
    ) -> AppState {
        AppState {
            workload_pools,
            ..test_state(jobs, workers)
        }
    }

    fn test_state_with_callback_policy(
        jobs: JobRegistry,
        workers: WorkerRegistry,
        callback_policy: crate::callbacks::CallbackPolicy,
    ) -> AppState {
        AppState {
            client: reqwest::Client::new(),
            backends: vec![],
            workload_pools: crate::config::WorkloadPoolPolicy::default(),
            worker_pool_ids: vec![],
            strict_worker_pools: false,
            affinity: SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            callback_policy,
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: ResourceRegistry::default(),
            resource_policy: ResourceRoutingPolicy::default(),
            priority_limiter: PriorityLimiter::default(),
            jobs,
            workers,
        }
    }

    fn unique_db_path(prefix: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fairlead-scheduler-{prefix}-{unique}.sqlite3"))
    }

    async fn start_callback_target(status: StatusCode) -> (String, mpsc::Receiver<Value>) {
        let (tx, rx) = mpsc::channel(8);
        let app = Router::new().route(
            "/callback",
            post(move |Json(value): Json<Value>| {
                let tx = tx.clone();
                async move {
                    tx.send(value).await.unwrap();
                    status
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/callback"), rx)
    }

    async fn start_sequence_callback_target(
        statuses: Vec<StatusCode>,
    ) -> (String, mpsc::Receiver<Value>) {
        let (tx, rx) = mpsc::channel(8);
        let statuses = Arc::new(Mutex::new(VecDeque::from(statuses)));
        let app = Router::new().route(
            "/callback",
            post(move |Json(value): Json<Value>| {
                let tx = tx.clone();
                let statuses = statuses.clone();
                async move {
                    tx.send(value).await.unwrap();
                    statuses
                        .lock()
                        .expect("callback statuses mutex poisoned")
                        .pop_front()
                        .unwrap_or(StatusCode::OK)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/callback"), rx)
    }

    async fn start_delayed_callback_target(
        delay: Duration,
        status: StatusCode,
    ) -> (String, mpsc::Receiver<Value>) {
        let (tx, rx) = mpsc::channel(8);
        let app = Router::new().route(
            "/callback",
            post(move |Json(value): Json<Value>| {
                let tx = tx.clone();
                async move {
                    tx.send(value).await.unwrap();
                    tokio::time::sleep(delay).await;
                    status
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/callback"), rx)
    }

    async fn response_text(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn wait_for_metric(app: Router, needle: &str) {
        for _ in 0..20 {
            let metrics = app
                .clone()
                .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
                .await
                .unwrap();
            let body = response_text(metrics).await;
            if body.contains(needle) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("metric not found: {needle}");
    }

    async fn wait_for_callback_status(
        jobs: &JobRegistry,
        job_id: &str,
        status: crate::jobs::JobCallbackStatus,
        attempts: u32,
    ) {
        for _ in 0..50 {
            let job = jobs.get(job_id).await.unwrap();
            if job
                .callback
                .as_ref()
                .is_some_and(|callback| callback.status == status && callback.attempts == attempts)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("callback status not observed for {job_id}");
    }

    #[tokio::test]
    async fn preview_selects_realtime_job_before_earlier_lower_priority_job() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "worker-a".into(),
                endpoint_url: "http://worker-a:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::IndexBuild, JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let preview = preview_next_assignment(&jobs, &workers).await.unwrap();
        assert_eq!(preview.job.id, "job-2");
        assert_eq!(preview.job.kind, JobKind::EmbedBatch);
        assert_eq!(preview.worker.id, "worker-a");
    }

    #[tokio::test]
    async fn preview_preserves_fifo_order_within_same_priority() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let preview = preview_next_assignment(&jobs, &workers).await.unwrap();
        assert_eq!(preview.job.id, "job-1");
    }

    #[tokio::test]
    async fn preview_skips_jobs_without_matching_available_workers() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "embed-worker".into(),
                endpoint_url: "http://embed-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let preview = preview_next_assignment(&jobs, &workers).await.unwrap();
        assert_eq!(preview.job.id, "job-2");
        assert_eq!(preview.worker.id, "embed-worker");
    }

    #[tokio::test]
    async fn preview_skips_workers_outside_workload_pool_policy() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        let workload_pools = crate::config::WorkloadPoolPolicy::new(BTreeMap::from([(
            "vision_analysis".to_string(),
            vec!["vision".to_string()],
        )]));

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "peer-worker".into(),
                endpoint_url: "http://peer-worker:9000".into(),
                node_id: None,
                pool: "peer".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        assert!(
            preview_next_assignment_with_policy(&jobs, &workers, &workload_pools)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn preview_selects_worker_from_allowed_pool() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        let workload_pools = crate::config::WorkloadPoolPolicy::new(BTreeMap::from([(
            "vision_analysis".to_string(),
            vec!["vision".to_string()],
        )]));

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        for (id, pool) in [("peer-worker", "peer"), ("vision-worker", "vision")] {
            workers
                .register(RegisterWorkerRequest {
                    id: id.into(),
                    endpoint_url: format!("http://{id}:9000"),
                    node_id: None,
                    pool: pool.into(),
                    job_types: vec![JobKind::VisionAnalysis],
                    max_concurrent_jobs: None,
                    available_vram_mb: None,
                })
                .await
                .unwrap();
        }

        let preview = preview_next_assignment_with_policy(&jobs, &workers, &workload_pools)
            .await
            .unwrap();
        assert_eq!(preview.worker.id, "vision-worker");
        assert_eq!(preview.worker.pool, "vision");
    }

    #[tokio::test]
    async fn preview_ignores_stale_workers() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::new(Duration::ZERO);

        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "stale-worker".into(),
                endpoint_url: "http://stale-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        assert!(preview_next_assignment(&jobs, &workers).await.is_none());
    }

    #[tokio::test]
    async fn preview_is_none_when_queue_is_empty() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        workers
            .register(RegisterWorkerRequest {
                id: "worker-a".into(),
                endpoint_url: "http://worker-a:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        assert!(preview_next_assignment(&jobs, &workers).await.is_none());
    }

    #[tokio::test]
    async fn preview_endpoint_reports_next_assignment_without_mutating_job() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: Some("node-a".into()),
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs.clone(), workers));
        let response = app
            .oneshot(
                Request::get("/v1/scheduler/preview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["assignment"]["job"]["id"], "job-1");
        assert_eq!(value["assignment"]["worker"]["id"], "vision-worker");
        assert_eq!(
            jobs.get("job-1").await.unwrap().status,
            crate::jobs::JobStatus::Queued
        );
    }

    #[tokio::test]
    async fn preview_endpoint_returns_no_content_when_no_assignment_exists() {
        let app = build_router(test_state(
            JobRegistry::default(),
            WorkerRegistry::default(),
        ));

        let response = app
            .oneshot(
                Request::get("/v1/scheduler/preview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn worker_claim_endpoint_leases_matching_job() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        let workload_pools = crate::config::WorkloadPoolPolicy::new(BTreeMap::from([(
            "vision_analysis".to_string(),
            vec!["default".to_string()],
        )]));

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        for (id, node_id, pool) in [
            ("vision-worker", Some("node-a"), "default"),
            ("vision-worker-b", Some("node-b"), "default"),
            ("peer-worker", Some("node-c"), "peer"),
        ] {
            workers
                .register(RegisterWorkerRequest {
                    id: id.into(),
                    endpoint_url: format!("http://{id}:9000"),
                    node_id: node_id.map(str::to_string),
                    pool: pool.into(),
                    job_types: vec![JobKind::VisionAnalysis],
                    max_concurrent_jobs: None,
                    available_vram_mb: None,
                })
                .await
                .unwrap();
        }

        let app = build_router(test_state_with_workload_pools(
            jobs.clone(),
            workers,
            workload_pools,
        ));
        let response = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/vision-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["status"], "running");
        assert_eq!(value["job"]["attempts"], 1);
        assert_eq!(value["job"]["lease"]["worker_id"], "vision-worker");
        assert_eq!(jobs.queue_snapshots().await, vec![]);

        let metrics = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let metrics = response_text(metrics).await;
        assert!(metrics.contains(
            "fairlead_async_pool_selections_total{type=\"vision_analysis\",priority=\"batch\",pool=\"default\",worker=\"vision-worker\",node=\"node-a\",outcome=\"selected\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_async_pool_candidate_workers_total{type=\"vision_analysis\",priority=\"batch\",pool=\"default\",worker=\"vision-worker\",node=\"node-a\",outcome=\"selected\"} 2"
        ));
        assert!(metrics.contains(
            "fairlead_async_pool_no_compatible_jobs_total{type=\"vision_analysis\",priority=\"batch\",pool=\"default\",worker=\"vision-worker\",node=\"node-a\",outcome=\"selected\"} 0"
        ));
    }

    #[tokio::test]
    async fn worker_claim_endpoint_skips_jobs_outside_worker_pool() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        let workload_pools = crate::config::WorkloadPoolPolicy::new(BTreeMap::from([
            ("vision_analysis".to_string(), vec!["vision".to_string()]),
            ("embed_batch".to_string(), vec!["default".to_string()]),
        ]));

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "default-worker".into(),
                endpoint_url: "http://default-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis, JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state_with_workload_pools(
            jobs.clone(),
            workers.clone(),
            workload_pools,
        ));
        let response = app
            .oneshot(
                Request::post("/v1/workers/default-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["id"], "job-2");
        assert_eq!(value["job"]["type"], "embed_batch");
        assert_eq!(
            jobs.get("job-1").await.unwrap().status,
            crate::jobs::JobStatus::Queued
        );
        assert_eq!(
            workers.get("default-worker").await.unwrap().in_flight_jobs,
            1
        );
    }

    #[tokio::test]
    async fn worker_claim_endpoint_returns_no_content_when_worker_pool_is_not_allowed() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        let workload_pools = crate::config::WorkloadPoolPolicy::new(BTreeMap::from([(
            "vision_analysis".to_string(),
            vec!["vision".to_string()],
        )]));

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "peer-worker".into(),
                endpoint_url: "http://peer-worker:9000".into(),
                node_id: None,
                pool: "peer".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state_with_workload_pools(
            jobs.clone(),
            workers.clone(),
            workload_pools,
        ));
        let response = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/peer-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            jobs.get("job-1").await.unwrap().status,
            crate::jobs::JobStatus::Queued
        );
        assert_eq!(workers.get("peer-worker").await.unwrap().in_flight_jobs, 0);

        let metrics = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let metrics = response_text(metrics).await;
        assert!(metrics.contains(
            "fairlead_async_pool_selections_total{type=\"vision_analysis\",priority=\"batch\",pool=\"peer\",worker=\"peer-worker\",node=\"\",outcome=\"no_compatible_job\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_async_pool_candidate_workers_total{type=\"vision_analysis\",priority=\"batch\",pool=\"peer\",worker=\"peer-worker\",node=\"\",outcome=\"no_compatible_job\"} 0"
        ));
        assert!(metrics.contains(
            "fairlead_async_pool_no_compatible_jobs_total{type=\"vision_analysis\",priority=\"batch\",pool=\"peer\",worker=\"peer-worker\",node=\"\",outcome=\"no_compatible_job\"} 2"
        ));
    }

    #[tokio::test]
    async fn worker_claim_endpoint_keeps_omitted_workload_pool_policy_permissive() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        let workload_pools = crate::config::WorkloadPoolPolicy::new(BTreeMap::from([(
            "vision_analysis".to_string(),
            vec!["vision".to_string()],
        )]));

        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "embed-worker".into(),
                endpoint_url: "http://embed-worker:9000".into(),
                node_id: None,
                pool: "peer".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let preview = preview_next_assignment_with_policy(&jobs, &workers, &workload_pools)
            .await
            .unwrap();
        assert_eq!(preview.job.kind, JobKind::EmbedBatch);
        assert_eq!(preview.worker.id, "embed-worker");

        let app = build_router(test_state_with_workload_pools(
            jobs.clone(),
            workers,
            workload_pools,
        ));
        let response = app
            .oneshot(
                Request::post("/v1/workers/embed-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["type"], "embed_batch");
    }

    #[tokio::test]
    async fn worker_claim_endpoint_prevents_duplicate_claims() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "embed-worker".into(),
                endpoint_url: "http://embed-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let first = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/embed-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let second = app
            .oneshot(
                Request::post("/v1/workers/embed-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn worker_claim_endpoint_rejects_worker_at_capacity() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        for _ in 0..2 {
            jobs.submit(SubmitJobRequest {
                kind: JobKind::VisionAnalysis,
                priority: Priority::Batch,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();
        }
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: Some(1),
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers.clone()));
        let first = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/vision-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let second = app
            .oneshot(
                Request::post("/v1/workers/vision-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::CONFLICT);
        let worker = workers.get("vision-worker").await.unwrap();
        assert_eq!(worker.in_flight_jobs, 1);
        assert_eq!(worker.available_job_slots, Some(0));
    }

    #[tokio::test]
    async fn worker_claim_endpoint_requeues_expired_leases_before_claiming() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("old-worker", &[JobKind::VisionAnalysis], 0)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs.clone(), workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/vision-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["attempts"], 2);
        assert_eq!(value["job"]["error"]["message"], "attempt timed out");
        assert_eq!(value["job"]["error"]["retryable"], true);
        assert_eq!(value["job"]["lease"]["worker_id"], "vision-worker");
        assert_eq!(
            jobs.get("job-1").await.unwrap().status,
            crate::jobs::JobStatus::Running
        );
    }

    #[tokio::test]
    async fn worker_result_endpoints_release_worker_capacity() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        for _ in 0..2 {
            jobs.submit(SubmitJobRequest {
                kind: JobKind::EmbedBatch,
                priority: Priority::Realtime,
                payload: Value::Null,
                callback_url: None,
            })
            .await
            .unwrap();
        }
        workers
            .register(RegisterWorkerRequest {
                id: "embed-worker".into(),
                endpoint_url: "http://embed-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: Some(1),
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers.clone()));
        let claim = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/embed-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(claim.status(), StatusCode::OK);
        assert_eq!(workers.get("embed-worker").await.unwrap().in_flight_jobs, 1);

        let complete = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/embed-worker/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"result": {"ok": true}}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete.status(), StatusCode::OK);
        assert_eq!(workers.get("embed-worker").await.unwrap().in_flight_jobs, 0);

        let second_claim = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/embed-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_claim.status(), StatusCode::OK);

        let fail = app
            .oneshot(
                Request::post("/v1/workers/embed-worker/jobs/job-2/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "temporary failure", "retryable": true}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fail.status(), StatusCode::OK);
        assert_eq!(workers.get("embed-worker").await.unwrap().in_flight_jobs, 0);
    }

    #[tokio::test]
    async fn complete_worker_job_delivers_success_callback() {
        let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: json!({"image": "rose.jpg"}),
            callback_url: Some(callback_url),
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("vision-worker", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let complete = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"result": {"healthy": true}}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete.status(), StatusCode::OK);

        let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(callback["job"]["id"], "job-1");
        assert_eq!(callback["job"]["status"], "complete");
        assert_eq!(callback["job"]["result"], json!({"healthy": true}));

        wait_for_metric(
            app,
            "fairlead_job_callbacks_total{type=\"vision_analysis\",status=\"complete\",outcome=\"success\",http_status=\"200\"} 1",
        )
        .await;
    }

    #[tokio::test]
    async fn terminal_worker_failure_delivers_failure_callback_metric() {
        let (callback_url, mut callbacks) =
            start_callback_target(StatusCode::INTERNAL_SERVER_ERROR).await;
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: Some(callback_url),
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("cluster-worker", &[JobKind::Cluster], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "cluster-worker".into(),
                endpoint_url: "http://cluster-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let fail = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/cluster-worker/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "bad input", "retryable": false}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fail.status(), StatusCode::OK);

        let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(callback["job"]["id"], "job-1");
        assert_eq!(callback["job"]["status"], "failed");
        assert_eq!(callback["job"]["error"]["message"], "bad input");

        wait_for_metric(
            app,
            "fairlead_job_callbacks_total{type=\"cluster\",status=\"failed\",outcome=\"failure\",http_status=\"500\"} 1",
        )
        .await;
    }

    #[tokio::test]
    async fn callback_delivery_retries_after_transient_failure() {
        let (callback_url, mut callbacks) =
            start_sequence_callback_target(vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK])
                .await;
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: Some(callback_url),
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("vision-worker", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state_with_callback_policy(
            jobs,
            workers,
            crate::callbacks::CallbackPolicy {
                max_attempts: 2,
                timeout: Duration::from_secs(1),
                retry_delay: Duration::from_millis(1),
            },
        ));
        let complete = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"result": {"ok": true}}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete.status(), StatusCode::OK);

        let first = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first["job"]["id"], "job-1");
        assert_eq!(second["job"]["id"], "job-1");

        wait_for_metric(
            app.clone(),
            "fairlead_job_callbacks_total{type=\"vision_analysis\",status=\"complete\",outcome=\"failure\",http_status=\"500\"} 1",
        )
        .await;
        wait_for_metric(
            app,
            "fairlead_job_callbacks_total{type=\"vision_analysis\",status=\"complete\",outcome=\"success\",http_status=\"200\"} 1",
        )
        .await;
    }

    #[tokio::test]
    async fn callback_delivery_timeout_records_failure_metric() {
        let (callback_url, mut callbacks) =
            start_delayed_callback_target(Duration::from_millis(200), StatusCode::OK).await;
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: Some(callback_url),
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("cluster-worker", &[JobKind::Cluster], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "cluster-worker".into(),
                endpoint_url: "http://cluster-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state_with_callback_policy(
            jobs,
            workers,
            crate::callbacks::CallbackPolicy {
                max_attempts: 1,
                timeout: Duration::from_millis(10),
                retry_delay: Duration::from_millis(0),
            },
        ));
        let fail = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/cluster-worker/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "bad input", "retryable": false}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fail.status(), StatusCode::OK);

        let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(callback["job"]["id"], "job-1");

        wait_for_metric(
            app,
            "fairlead_job_callbacks_total{type=\"cluster\",status=\"failed\",outcome=\"failure\",http_status=\"0\"} 1",
        )
        .await;
    }

    #[tokio::test]
    async fn pending_sqlite_callback_is_delivered_after_registry_restart() {
        let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
        let path = unique_db_path("callback-restart");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let submitted = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::VisionAnalysis,
                priority: Priority::Batch,
                payload: Value::Null,
                callback_url: Some(callback_url),
            })
            .await
            .unwrap();
        jobs.claim_next_for_worker("vision-worker", &[submitted.kind], 30_000)
            .await
            .unwrap();
        jobs.complete_lease(&submitted.id, "vision-worker", json!({"ok": true}))
            .await;

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        assert_eq!(restored.pending_callback_jobs().await.len(), 1);

        crate::callbacks::dispatch_pending_callbacks(
            crate::callbacks::CallbackDispatcher::default(),
            reqwest::Client::new(),
            RoutingMetrics::default(),
            crate::callbacks::CallbackPolicy {
                max_attempts: 1,
                timeout: Duration::from_secs(1),
                retry_delay: Duration::from_millis(0),
            },
            restored.clone(),
        )
        .await;

        let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(callback["job"]["id"], submitted.id);
        wait_for_callback_status(
            &restored,
            &submitted.id,
            crate::jobs::JobCallbackStatus::Delivered,
            1,
        )
        .await;

        let persisted =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        assert!(persisted.pending_callback_jobs().await.is_empty());
        assert_eq!(
            persisted
                .get(&submitted.id)
                .await
                .unwrap()
                .callback
                .unwrap()
                .status,
            crate::jobs::JobCallbackStatus::Delivered
        );
    }

    #[tokio::test]
    async fn failed_sqlite_callback_attempt_retries_after_registry_restart() {
        let (callback_url, mut callbacks) =
            start_sequence_callback_target(vec![StatusCode::INTERNAL_SERVER_ERROR, StatusCode::OK])
                .await;
        let path = unique_db_path("callback-retry-restart");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let submitted = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::Cluster,
                priority: Priority::Background,
                payload: Value::Null,
                callback_url: Some(callback_url),
            })
            .await
            .unwrap();
        jobs.claim_next_for_worker("cluster-worker", &[submitted.kind], 30_000)
            .await
            .unwrap();
        jobs.complete_lease(&submitted.id, "cluster-worker", json!({"ok": true}))
            .await;

        crate::callbacks::dispatch_pending_callbacks(
            crate::callbacks::CallbackDispatcher::default(),
            reqwest::Client::new(),
            RoutingMetrics::default(),
            crate::callbacks::CallbackPolicy {
                max_attempts: 1,
                timeout: Duration::from_secs(1),
                retry_delay: Duration::from_millis(0),
            },
            jobs.clone(),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        wait_for_callback_status(
            &jobs,
            &submitted.id,
            crate::jobs::JobCallbackStatus::Pending,
            1,
        )
        .await;

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        crate::callbacks::dispatch_pending_callbacks(
            crate::callbacks::CallbackDispatcher::default(),
            reqwest::Client::new(),
            RoutingMetrics::default(),
            crate::callbacks::CallbackPolicy {
                max_attempts: 1,
                timeout: Duration::from_secs(1),
                retry_delay: Duration::from_millis(0),
            },
            restored.clone(),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        wait_for_callback_status(
            &restored,
            &submitted.id,
            crate::jobs::JobCallbackStatus::Delivered,
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn callback_recovery_loop_delivers_pending_sqlite_callback() {
        let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
        let path = unique_db_path("callback-recovery-loop");
        let jobs =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        let submitted = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::VisionAnalysis,
                priority: Priority::Batch,
                payload: Value::Null,
                callback_url: Some(callback_url),
            })
            .await
            .unwrap();
        jobs.claim_next_for_worker("vision-worker", &[submitted.kind], 30_000)
            .await
            .unwrap();
        jobs.complete_lease(&submitted.id, "vision-worker", json!({"ok": true}))
            .await;

        let restored =
            JobRegistry::with_store(crate::storage::SqliteJobStore::open(&path).unwrap()).unwrap();
        crate::callbacks::spawn_callback_recovery_loop(
            crate::callbacks::CallbackDispatcher::default(),
            reqwest::Client::new(),
            RoutingMetrics::default(),
            crate::callbacks::CallbackPolicy {
                max_attempts: 1,
                timeout: Duration::from_secs(1),
                retry_delay: Duration::from_millis(10),
            },
            restored.clone(),
        );

        let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(callback["job"]["id"], submitted.id);
        wait_for_callback_status(
            &restored,
            &submitted.id,
            crate::jobs::JobCallbackStatus::Delivered,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn callback_dispatcher_deduplicates_in_flight_job_delivery() {
        let (callback_url, mut callbacks) =
            start_delayed_callback_target(Duration::from_millis(150), StatusCode::OK).await;
        let jobs = JobRegistry::default();
        let submitted = jobs
            .submit(SubmitJobRequest {
                kind: JobKind::Cluster,
                priority: Priority::Background,
                payload: Value::Null,
                callback_url: Some(callback_url),
            })
            .await
            .unwrap();
        jobs.claim_next_for_worker("cluster-worker", &[submitted.kind], 30_000)
            .await
            .unwrap();
        jobs.complete_lease(&submitted.id, "cluster-worker", json!({"ok": true}))
            .await;

        let dispatcher = crate::callbacks::CallbackDispatcher::default();
        let policy = crate::callbacks::CallbackPolicy {
            max_attempts: 1,
            timeout: Duration::from_secs(1),
            retry_delay: Duration::from_millis(0),
        };
        dispatcher.dispatch(
            reqwest::Client::new(),
            RoutingMetrics::default(),
            policy,
            jobs.clone(),
            submitted.id.clone(),
        );
        dispatcher.dispatch(
            reqwest::Client::new(),
            RoutingMetrics::default(),
            policy,
            jobs.clone(),
            submitted.id.clone(),
        );

        let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(callback["job"]["id"], submitted.id);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), callbacks.recv())
                .await
                .is_err()
        );
        wait_for_callback_status(
            &jobs,
            &submitted.id,
            crate::jobs::JobCallbackStatus::Delivered,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn retryable_worker_failure_does_not_deliver_callback() {
        let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: Some(callback_url),
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("embed-worker", &[JobKind::EmbedBatch], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "embed-worker".into(),
                endpoint_url: "http://embed-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let fail = app
            .oneshot(
                Request::post("/v1/workers/embed-worker/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "temporary", "retryable": true}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fail.status(), StatusCode::OK);

        let callback = tokio::time::timeout(Duration::from_millis(100), callbacks.recv()).await;
        assert!(callback.is_err());
    }

    #[tokio::test]
    async fn cancelling_leased_job_releases_worker_capacity() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: Some(1),
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers.clone()));
        let claim = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/vision-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(claim.status(), StatusCode::OK);
        assert_eq!(
            workers.get("vision-worker").await.unwrap().in_flight_jobs,
            1
        );

        let cancel = app
            .oneshot(
                Request::delete("/v1/jobs/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::OK);
        assert_eq!(
            workers.get("vision-worker").await.unwrap().in_flight_jobs,
            0
        );
    }

    #[tokio::test]
    async fn cancelling_job_delivers_callback() {
        let (callback_url, mut callbacks) = start_callback_target(StatusCode::OK).await;
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: Some(callback_url),
        })
        .await
        .unwrap();

        let app = build_router(test_state(jobs, workers));
        let cancel = app
            .clone()
            .oneshot(
                Request::delete("/v1/jobs/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::OK);

        let callback = tokio::time::timeout(Duration::from_secs(2), callbacks.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(callback["job"]["id"], "job-1");
        assert_eq!(callback["job"]["status"], "cancelled");

        wait_for_metric(
            app,
            "fairlead_job_callbacks_total{type=\"vision_analysis\",status=\"cancelled\",outcome=\"success\",http_status=\"200\"} 1",
        )
        .await;
    }

    #[tokio::test]
    async fn expired_lease_sweep_releases_worker_capacity_before_claim() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "old-worker".into(),
                endpoint_url: "http://old-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: Some(1),
                available_vram_mb: None,
            })
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "new-worker".into(),
                endpoint_url: "http://new-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: Some(1),
                available_vram_mb: None,
            })
            .await
            .unwrap();
        workers.try_acquire_slot("old-worker").await;
        jobs.claim_next_for_worker("old-worker", &[JobKind::VisionAnalysis], 0)
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers.clone()));
        let claim = app
            .oneshot(
                Request::post("/v1/workers/new-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(claim.status(), StatusCode::OK);
        assert_eq!(workers.get("old-worker").await.unwrap().in_flight_jobs, 0);
        assert_eq!(workers.get("new-worker").await.unwrap().in_flight_jobs, 1);
    }

    #[tokio::test]
    async fn worker_renew_endpoint_extends_held_lease() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        let claimed = jobs
            .claim_next_for_worker("vision-worker", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        let original_expires_at = claimed.lease.unwrap().expires_at_unix_ms;
        tokio::time::sleep(Duration::from_millis(5)).await;
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs.clone(), workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/renew")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["status"], "running");
        assert_eq!(value["job"]["lease"]["worker_id"], "vision-worker");
        assert!(
            value["job"]["lease"]["expires_at_unix_ms"]
                .as_u64()
                .unwrap() as u128
                > original_expires_at
        );
    }

    #[tokio::test]
    async fn worker_complete_endpoint_marks_held_job_complete() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("vision-worker", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs.clone(), workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"result": {"label": "healthy"}}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["status"], "complete");
        assert_eq!(value["job"]["result"], json!({"label": "healthy"}));
        assert!(value["job"]["error"].is_null());
        assert!(value["job"]["lease"].is_null());
        assert_eq!(
            jobs.get("job-1").await.unwrap().status,
            crate::jobs::JobStatus::Complete
        );
    }

    #[tokio::test]
    async fn worker_complete_endpoint_rejects_worker_that_does_not_hold_lease() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::EmbedBatch], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "worker-b".into(),
                endpoint_url: "http://worker-b:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/worker-b/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"result": null}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["status"], "running");
        assert_eq!(value["job"]["lease"]["worker_id"], "worker-a");
    }

    #[tokio::test]
    async fn worker_complete_endpoint_rejects_unknown_and_stale_workers() {
        let jobs = JobRegistry::default();
        let stale_workers = WorkerRegistry::new(Duration::ZERO);

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        stale_workers
            .register(RegisterWorkerRequest {
                id: "worker-a".into(),
                endpoint_url: "http://worker-a:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let unknown_app = build_router(test_state(jobs.clone(), WorkerRegistry::default()));
        let unknown = unknown_app
            .oneshot(
                Request::post("/v1/workers/worker-a/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"result": null}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

        let stale_app = build_router(test_state(jobs, stale_workers));
        let stale = stale_app
            .oneshot(
                Request::post("/v1/workers/worker-a/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"result": null}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn duplicate_worker_result_reports_do_not_change_terminal_job() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: Some(1),
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs.clone(), workers.clone()));
        let claim = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/vision-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(claim.status(), StatusCode::OK);

        let complete = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"result": {"label": "healthy"}}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(complete.status(), StatusCode::OK);

        let duplicate_complete = app
            .clone()
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/complete")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"result": {"label": "changed"}}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(duplicate_complete.status(), StatusCode::CONFLICT);

        let late_fail = app
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "late failure", "retryable": true}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(late_fail.status(), StatusCode::CONFLICT);

        let job = jobs.get("job-1").await.unwrap();
        assert_eq!(job.status, crate::jobs::JobStatus::Complete);
        assert_eq!(job.result, Some(json!({"label": "healthy"})));
        assert!(job.error.is_none());
        assert_eq!(
            workers.get("vision-worker").await.unwrap().in_flight_jobs,
            0
        );
    }

    #[tokio::test]
    async fn worker_fail_endpoint_requeues_retryable_failure() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("index-worker", &[JobKind::IndexBuild], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "index-worker".into(),
                endpoint_url: "http://index-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::IndexBuild],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs.clone(), workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/index-worker/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "temporary worker failure", "retryable": true}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["status"], "queued");
        assert_eq!(value["job"]["error"]["message"], "temporary worker failure");
        assert_eq!(value["job"]["error"]["retryable"], true);
        assert!(value["job"]["lease"].is_null());
        assert_eq!(
            jobs.queued_jobs_by_priority()
                .await
                .iter()
                .map(|job| job.id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-1"]
        );
    }

    #[tokio::test]
    async fn worker_fail_endpoint_marks_non_retryable_failure_failed() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("cluster-worker", &[JobKind::Cluster], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "cluster-worker".into(),
                endpoint_url: "http://cluster-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/cluster-worker/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "invalid request", "retryable": false}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["status"], "failed");
        assert_eq!(value["job"]["error"]["message"], "invalid request");
        assert_eq!(value["job"]["error"]["retryable"], false);
        assert!(value["job"]["lease"].is_null());
    }

    #[tokio::test]
    async fn worker_fail_endpoint_rejects_empty_error() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        workers
            .register(RegisterWorkerRequest {
                id: "worker-a".into(),
                endpoint_url: "http://worker-a:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/worker-a/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"error": "   "}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn worker_fail_endpoint_rejects_unknown_and_stale_workers() {
        let jobs = JobRegistry::default();
        let stale_workers = WorkerRegistry::new(Duration::ZERO);

        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::Cluster], 30_000)
            .await
            .unwrap();
        stale_workers
            .register(RegisterWorkerRequest {
                id: "worker-a".into(),
                endpoint_url: "http://worker-a:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let unknown_app = build_router(test_state(jobs.clone(), WorkerRegistry::default()));
        let unknown = unknown_app
            .oneshot(
                Request::post("/v1/workers/worker-a/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "worker failed", "retryable": true}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

        let stale_app = build_router(test_state(jobs, stale_workers));
        let stale = stale_app
            .oneshot(
                Request::post("/v1/workers/worker-a/jobs/job-1/fail")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"error": "worker failed", "retryable": true}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn worker_renew_endpoint_rejects_unknown_or_stale_worker() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::EmbedBatch,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("embed-worker", &[JobKind::EmbedBatch], 30_000)
            .await
            .unwrap();

        let unknown_app = build_router(test_state(jobs.clone(), WorkerRegistry::default()));
        let unknown_response = unknown_app
            .oneshot(
                Request::post("/v1/workers/embed-worker/jobs/job-1/renew")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown_response.status(), StatusCode::NOT_FOUND);

        let stale_workers = WorkerRegistry::new(Duration::ZERO);
        stale_workers
            .register(RegisterWorkerRequest {
                id: "embed-worker".into(),
                endpoint_url: "http://embed-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();
        let stale_app = build_router(test_state(jobs, stale_workers));
        let stale_response = stale_app
            .oneshot(
                Request::post("/v1/workers/embed-worker/jobs/job-1/renew")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn worker_renew_endpoint_rejects_worker_that_does_not_hold_lease() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("worker-a", &[JobKind::Cluster], 30_000)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "worker-b".into(),
                endpoint_url: "http://worker-b:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/worker-b/jobs/job-1/renew")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["lease"]["worker_id"], "worker-a");
    }

    #[tokio::test]
    async fn worker_renew_endpoint_requeues_expired_lease_before_renewal() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("vision-worker", &[JobKind::VisionAnalysis], 0)
            .await
            .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs.clone(), workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/renew")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["status"], "queued");
        assert_eq!(value["job"]["error"]["message"], "attempt timed out");
        assert_eq!(value["job"]["error"]["retryable"], true);
        assert!(value["job"]["lease"].is_null());
        assert_eq!(
            jobs.queued_jobs_by_priority()
                .await
                .iter()
                .map(|job| job.id.as_str())
                .collect::<Vec<_>>(),
            vec!["job-1"]
        );
    }

    #[tokio::test]
    async fn worker_renew_endpoint_rejects_cancelled_running_job() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("vision-worker", &[JobKind::VisionAnalysis], 30_000)
            .await
            .unwrap();
        jobs.cancel("job-1").await;
        workers
            .register(RegisterWorkerRequest {
                id: "vision-worker".into(),
                endpoint_url: "http://vision-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/vision-worker/jobs/job-1/renew")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["job"]["status"], "cancelled");
        assert_eq!(value["job"]["lease"]["worker_id"], "vision-worker");
    }

    #[tokio::test]
    async fn worker_claim_endpoint_skips_cancelled_requeued_job() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.claim_next_for_worker("old-worker", &[JobKind::Cluster], 0)
            .await
            .unwrap();
        jobs.requeue_expired_leases().await;
        jobs.cancel("job-1").await;
        workers
            .register(RegisterWorkerRequest {
                id: "cluster-worker".into(),
                endpoint_url: "http://cluster-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs.clone(), workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/cluster-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            jobs.get("job-1").await.unwrap().status,
            crate::jobs::JobStatus::Cancelled
        );
        assert!(jobs.queue_snapshots().await.is_empty());
    }

    #[tokio::test]
    async fn worker_renew_endpoint_returns_not_found_for_missing_job() {
        let workers = WorkerRegistry::default();
        workers
            .register(RegisterWorkerRequest {
                id: "worker-a".into(),
                endpoint_url: "http://worker-a:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(JobRegistry::default(), workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/worker-a/jobs/missing/renew")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn worker_claim_endpoint_rejects_unknown_worker() {
        let app = build_router(test_state(
            JobRegistry::default(),
            WorkerRegistry::default(),
        ));

        let response = app
            .oneshot(
                Request::post("/v1/workers/missing/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn worker_claim_endpoint_rejects_stale_worker() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::new(Duration::ZERO);

        jobs.submit(SubmitJobRequest {
            kind: JobKind::Cluster,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "cluster-worker".into(),
                endpoint_url: "http://cluster-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::Cluster],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/cluster-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn worker_claim_endpoint_returns_no_content_without_compatible_job() {
        let jobs = JobRegistry::default();
        let workers = WorkerRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        workers
            .register(RegisterWorkerRequest {
                id: "embed-worker".into(),
                endpoint_url: "http://embed-worker:9000".into(),
                node_id: None,
                pool: "default".into(),
                job_types: vec![JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        let app = build_router(test_state(jobs, workers));
        let response = app
            .oneshot(
                Request::post("/v1/workers/embed-worker/claim")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}
