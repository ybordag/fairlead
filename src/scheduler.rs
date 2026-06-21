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
    workers::{WorkerRegistry, WorkerSnapshot},
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
    match preview_next_assignment(&state.jobs, &state.workers).await {
        Some(assignment) => Json(SchedulerPreviewResponse { assignment }).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

pub async fn claim_worker_job_handler(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
) -> Response {
    let Some(worker) = state.workers.get(&worker_id).await else {
        return (StatusCode::NOT_FOUND, "worker not found").into_response();
    };
    if worker.stale {
        return (StatusCode::CONFLICT, "worker is stale").into_response();
    }

    state.jobs.requeue_expired_leases().await;

    match state
        .jobs
        .claim_next_for_worker(&worker.id, &worker.job_types, DEFAULT_LEASE_DURATION_MS)
        .await
    {
        Some(job) => Json(JobClaimResponse { job }).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
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

    state.jobs.requeue_expired_leases().await;

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

    state.jobs.requeue_expired_leases().await;

    match state
        .jobs
        .complete_lease(&job_id, &worker.id, request.result)
        .await
    {
        FinishJobResult::Completed(job) => Json(JobResultResponse { job }).into_response(),
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

    state.jobs.requeue_expired_leases().await;

    let failure = JobFailure {
        message: error.to_string(),
        retryable: request.retryable,
    };
    match state.jobs.fail_lease(&job_id, &worker.id, failure).await {
        FinishJobResult::Requeued(job) | FinishJobResult::Failed(job) => {
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

pub async fn preview_next_assignment(
    jobs: &JobRegistry,
    workers: &WorkerRegistry,
) -> Option<SchedulerPreview> {
    preview_from_snapshots(jobs.queued_jobs_by_priority().await, workers.list().await)
}

fn preview_from_snapshots(
    queued_jobs: Vec<JobRecord>,
    workers: Vec<WorkerSnapshot>,
) -> Option<SchedulerPreview> {
    for job in queued_jobs {
        if let Some(worker) = workers
            .iter()
            .find(|worker| !worker.stale && worker.job_types.contains(&job.kind))
        {
            return Some(SchedulerPreview {
                job,
                worker: worker.clone(),
            });
        }
    }

    None
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
    use axum::{body::Body, http::Request};
    use serde_json::{json, Value};
    use std::time::Duration;
    use tower::ServiceExt;

    fn test_state(jobs: JobRegistry, workers: WorkerRegistry) -> AppState {
        AppState {
            client: reqwest::Client::new(),
            backends: vec![],
            affinity: SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            resources: ResourceRegistry::default(),
            resource_policy: ResourceRoutingPolicy::default(),
            priority_limiter: PriorityLimiter::default(),
            jobs,
            workers,
        }
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
        assert_eq!(value["job"]["status"], "running");
        assert_eq!(value["job"]["attempts"], 1);
        assert_eq!(value["job"]["lease"]["worker_id"], "vision-worker");
        assert_eq!(jobs.queue_snapshots().await, vec![]);
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
        assert_eq!(value["job"]["lease"]["worker_id"], "vision-worker");
        assert_eq!(
            jobs.get("job-1").await.unwrap().status,
            crate::jobs::JobStatus::Running
        );
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
