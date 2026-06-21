use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::{
    jobs::{JobRecord, JobRegistry},
    workers::{WorkerRegistry, WorkerSnapshot},
    AppState,
};

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerPreview {
    pub job: JobRecord,
    pub worker: WorkerSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerPreviewResponse {
    pub assignment: SchedulerPreview,
}

pub async fn preview_next_assignment_handler(State(state): State<AppState>) -> Response {
    match preview_next_assignment(&state.jobs, &state.workers).await {
        Some(assignment) => Json(SchedulerPreviewResponse { assignment }).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
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
    use serde_json::Value;
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
}
