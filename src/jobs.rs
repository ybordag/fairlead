use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

use crate::{config::Priority, AppState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    VisionAnalysis,
    EmbedBatch,
    IndexBuild,
    Cluster,
}

impl JobKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::VisionAnalysis => "vision_analysis",
            Self::EmbedBatch => "embed_batch",
            Self::IndexBuild => "index_build",
            Self::Cluster => "cluster",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Complete,
    Failed,
    Cancelled,
}

impl JobStatus {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubmitJobRequest {
    #[serde(rename = "type")]
    pub kind: JobKind,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub callback_url: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobRecord {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: JobKind,
    pub priority: Priority,
    pub status: JobStatus,
    pub payload: Value,
    pub callback_url: Option<String>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub created_at_unix_ms: u128,
    pub updated_at_unix_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobResponse {
    pub job: JobRecord,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobListResponse {
    pub jobs: Vec<JobRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobQueueSnapshot {
    pub priority: &'static str,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub depth: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JobQueueWaitSnapshot {
    pub priority: &'static str,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub count: usize,
    pub wait_seconds_sum: f64,
    pub wait_seconds_max: f64,
}

#[derive(Clone, Default)]
pub struct JobRegistry {
    inner: Arc<RwLock<JobRegistryInner>>,
}

#[derive(Default)]
struct JobRegistryInner {
    next_id: u64,
    jobs: HashMap<String, JobRecord>,
    order: Vec<String>,
    queues: JobQueues,
}

#[derive(Default)]
struct JobQueues {
    realtime: VecDeque<String>,
    batch: VecDeque<String>,
    background: VecDeque<String>,
}

impl JobQueues {
    fn for_priority_mut(&mut self, priority: Priority) -> &mut VecDeque<String> {
        match priority {
            Priority::Realtime => &mut self.realtime,
            Priority::Batch => &mut self.batch,
            Priority::Background => &mut self.background,
        }
    }

    fn iter(&self) -> impl Iterator<Item = (Priority, &VecDeque<String>)> {
        [
            (Priority::Realtime, &self.realtime),
            (Priority::Batch, &self.batch),
            (Priority::Background, &self.background),
        ]
        .into_iter()
    }

    fn remove(&mut self, priority: Priority, id: &str) {
        let queue = self.for_priority_mut(priority);
        if let Some(position) = queue.iter().position(|queued_id| queued_id == id) {
            queue.remove(position);
        }
    }
}

impl JobRegistry {
    pub async fn submit(&self, request: SubmitJobRequest) -> Result<JobRecord, String> {
        let callback_url = normalize_callback_url(request.callback_url)?;
        let mut guard = self.inner.write().await;
        guard.next_id += 1;
        let now = unix_ms();
        let job = JobRecord {
            id: format!("job-{}", guard.next_id),
            kind: request.kind,
            priority: request.priority,
            status: JobStatus::Queued,
            payload: request.payload,
            callback_url,
            attempts: 0,
            max_attempts: 3,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        guard.jobs.insert(job.id.clone(), job.clone());
        guard.order.push(job.id.clone());
        guard
            .queues
            .for_priority_mut(job.priority)
            .push_back(job.id.clone());
        Ok(job)
    }

    pub async fn get(&self, id: &str) -> Option<JobRecord> {
        self.inner.read().await.jobs.get(id).cloned()
    }

    pub async fn list(&self) -> Vec<JobRecord> {
        let guard = self.inner.read().await;
        guard
            .order
            .iter()
            .filter_map(|id| guard.jobs.get(id).cloned())
            .collect()
    }

    pub async fn queue_snapshots(&self) -> Vec<JobQueueSnapshot> {
        let guard = self.inner.read().await;
        let mut snapshots: Vec<JobQueueSnapshot> = Vec::new();

        for (priority, queue) in guard.queues.iter() {
            for id in queue {
                let Some(job) = guard.jobs.get(id) else {
                    continue;
                };
                if job.status != JobStatus::Queued {
                    continue;
                }

                if let Some(snapshot) = snapshots
                    .iter_mut()
                    .find(|s| s.priority == priority.as_str() && s.kind == job.kind.as_str())
                {
                    snapshot.depth += 1;
                } else {
                    snapshots.push(JobQueueSnapshot {
                        priority: priority.as_str(),
                        kind: job.kind.as_str(),
                        depth: 1,
                    });
                }
            }
        }

        snapshots
            .sort_by_key(|snapshot| (priority_rank(snapshot.priority), kind_rank(snapshot.kind)));
        snapshots
    }

    pub async fn queue_wait_snapshots(&self) -> Vec<JobQueueWaitSnapshot> {
        let now = unix_ms();
        let guard = self.inner.read().await;
        let mut snapshots: Vec<JobQueueWaitSnapshot> = Vec::new();

        for (priority, queue) in guard.queues.iter() {
            for id in queue {
                let Some(job) = guard.jobs.get(id) else {
                    continue;
                };
                if job.status != JobStatus::Queued {
                    continue;
                }

                let wait_seconds = now.saturating_sub(job.created_at_unix_ms) as f64 / 1000.0;

                if let Some(snapshot) = snapshots
                    .iter_mut()
                    .find(|s| s.priority == priority.as_str() && s.kind == job.kind.as_str())
                {
                    snapshot.count += 1;
                    snapshot.wait_seconds_sum += wait_seconds;
                    snapshot.wait_seconds_max = snapshot.wait_seconds_max.max(wait_seconds);
                } else {
                    snapshots.push(JobQueueWaitSnapshot {
                        priority: priority.as_str(),
                        kind: job.kind.as_str(),
                        count: 1,
                        wait_seconds_sum: wait_seconds,
                        wait_seconds_max: wait_seconds,
                    });
                }
            }
        }

        snapshots
            .sort_by_key(|snapshot| (priority_rank(snapshot.priority), kind_rank(snapshot.kind)));
        snapshots
    }

    pub async fn queued_jobs_by_priority(&self) -> Vec<JobRecord> {
        let guard = self.inner.read().await;
        let mut jobs = Vec::new();

        for (_priority, queue) in guard.queues.iter() {
            for id in queue {
                let Some(job) = guard.jobs.get(id) else {
                    continue;
                };
                if job.status == JobStatus::Queued {
                    jobs.push(job.clone());
                }
            }
        }

        jobs
    }

    pub async fn cancel(&self, id: &str) -> CancelJobResult {
        let mut guard = self.inner.write().await;
        let Some(job) = guard.jobs.get_mut(id) else {
            return CancelJobResult::NotFound;
        };

        if job.status.is_terminal() {
            return CancelJobResult::AlreadyTerminal(job.clone());
        }

        let priority = job.priority;
        job.status = JobStatus::Cancelled;
        job.updated_at_unix_ms = unix_ms();
        let job = job.clone();
        guard.queues.remove(priority, id);
        CancelJobResult::Cancelled(job)
    }
}

pub enum CancelJobResult {
    Cancelled(JobRecord),
    AlreadyTerminal(JobRecord),
    NotFound,
}

pub async fn submit_job(
    State(state): State<AppState>,
    Json(request): Json<SubmitJobRequest>,
) -> Response {
    match state.jobs.submit(request).await {
        Ok(job) => (StatusCode::ACCEPTED, Json(JobResponse { job })).into_response(),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

pub async fn list_jobs(State(state): State<AppState>) -> Json<JobListResponse> {
    Json(JobListResponse {
        jobs: state.jobs.list().await,
    })
}

pub async fn get_job(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.jobs.get(&id).await {
        Some(job) => Json(JobResponse { job }).into_response(),
        None => (StatusCode::NOT_FOUND, "job not found").into_response(),
    }
}

pub async fn cancel_job(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.jobs.cancel(&id).await {
        CancelJobResult::Cancelled(job) => Json(JobResponse { job }).into_response(),
        CancelJobResult::AlreadyTerminal(job) => {
            (StatusCode::CONFLICT, Json(JobResponse { job })).into_response()
        }
        CancelJobResult::NotFound => (StatusCode::NOT_FOUND, "job not found").into_response(),
    }
}

fn normalize_callback_url(callback_url: Option<String>) -> Result<Option<String>, String> {
    let Some(callback_url) = callback_url else {
        return Ok(None);
    };
    let trimmed = callback_url.trim();
    if trimmed.is_empty() {
        return Err("callback_url cannot be empty".into());
    }
    Ok(Some(trimmed.to_string()))
}

fn priority_rank(priority: &str) -> u8 {
    match priority {
        "realtime" => 0,
        "batch" => 1,
        "background" => 2,
        _ => 3,
    }
}

fn kind_rank(kind: &str) -> u8 {
    match kind {
        "vision_analysis" => 0,
        "embed_batch" => 1,
        "index_build" => 2,
        "cluster" => 3,
        _ => 4,
    }
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        build_router,
        metrics::RoutingMetrics,
        priority::PriorityLimiter,
        resources::{ResourceRegistry, ResourceRoutingPolicy},
        router::SessionAffinity,
        AppState,
    };
    use axum::{
        body::Body,
        http::{Method, Request, StatusCode},
    };
    use serde_json::json;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        AppState {
            client: reqwest::Client::new(),
            backends: vec![],
            affinity: SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            resources: ResourceRegistry::default(),
            resource_policy: ResourceRoutingPolicy::default(),
            priority_limiter: PriorityLimiter::default(),
            jobs: JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        }
    }

    #[tokio::test]
    async fn submit_job_returns_accepted_queued_job() {
        let app = build_router(test_state());

        let response = app
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "type": "vision_analysis",
                            "priority": "batch",
                            "payload": {"image_id": "image-1"},
                            "callback_url": " http://rhizome/jobs/job-1 "
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let value = response_json(response).await;
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["type"], "vision_analysis");
        assert_eq!(value["job"]["priority"], "batch");
        assert_eq!(value["job"]["status"], "queued");
        assert_eq!(value["job"]["payload"], json!({"image_id": "image-1"}));
        assert_eq!(value["job"]["callback_url"], "http://rhizome/jobs/job-1");
        assert_eq!(value["job"]["attempts"], 0);
        assert_eq!(value["job"]["max_attempts"], 3);
    }

    #[tokio::test]
    async fn submitted_job_can_be_fetched() {
        let app = build_router(test_state());

        let submit = app
            .clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "embed_batch", "payload": {"texts": ["a", "b"]}})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit.status(), StatusCode::ACCEPTED);

        let response = app
            .oneshot(Request::get("/v1/jobs/job-1").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["job"]["id"], "job-1");
        assert_eq!(value["job"]["type"], "embed_batch");
        assert_eq!(value["job"]["priority"], "realtime");
    }

    #[tokio::test]
    async fn list_jobs_returns_jobs_in_submission_order() {
        let app = build_router(test_state());

        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"type": "vision_analysis"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"type": "embed_batch"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(Request::get("/v1/jobs").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["jobs"][0]["id"], "job-1");
        assert_eq!(value["jobs"][0]["type"], "vision_analysis");
        assert_eq!(value["jobs"][1]["id"], "job-2");
        assert_eq!(value["jobs"][1]["type"], "embed_batch");
    }

    #[tokio::test]
    async fn queue_snapshots_count_queued_jobs_by_priority_and_type() {
        let jobs = JobRegistry::default();

        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Realtime,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
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

        assert_eq!(
            jobs.queue_snapshots().await,
            vec![
                JobQueueSnapshot {
                    priority: "realtime",
                    kind: "vision_analysis",
                    depth: 2,
                },
                JobQueueSnapshot {
                    priority: "batch",
                    kind: "embed_batch",
                    depth: 1,
                },
            ]
        );
    }

    #[tokio::test]
    async fn cancel_job_marks_queued_job_cancelled() {
        let app = build_router(test_state());

        let submit = app
            .clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"type": "index_build"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submit.status(), StatusCode::ACCEPTED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/jobs/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["job"]["status"], "cancelled");

        let response = app
            .oneshot(Request::get("/v1/jobs/job-1").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let value = response_json(response).await;
        assert_eq!(value["job"]["status"], "cancelled");
    }

    #[tokio::test]
    async fn cancelling_job_removes_it_from_queue_depth() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.cancel("job-1").await;

        assert_eq!(
            jobs.queue_snapshots().await,
            vec![JobQueueSnapshot {
                priority: "background",
                kind: "index_build",
                depth: 1,
            }]
        );
    }

    #[tokio::test]
    async fn queued_jobs_by_priority_returns_priority_order_and_skips_cancelled_jobs() {
        let jobs = JobRegistry::default();
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
        jobs.submit(SubmitJobRequest {
            kind: JobKind::VisionAnalysis,
            priority: Priority::Batch,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.cancel("job-2").await;

        let queued = jobs.queued_jobs_by_priority().await;
        assert_eq!(
            queued.iter().map(|job| job.id.as_str()).collect::<Vec<_>>(),
            vec!["job-3", "job-1"]
        );
    }

    #[tokio::test]
    async fn queue_wait_snapshots_measure_queued_job_age_by_priority_and_type() {
        let jobs = JobRegistry::default();

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

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let snapshots = jobs.queue_wait_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].priority, "batch");
        assert_eq!(snapshots[0].kind, "vision_analysis");
        assert_eq!(snapshots[0].count, 2);
        assert!(snapshots[0].wait_seconds_sum > 0.0);
        assert!(snapshots[0].wait_seconds_max > 0.0);
    }

    #[tokio::test]
    async fn cancelling_job_removes_it_from_queue_wait_snapshots() {
        let jobs = JobRegistry::default();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();
        jobs.submit(SubmitJobRequest {
            kind: JobKind::IndexBuild,
            priority: Priority::Background,
            payload: Value::Null,
            callback_url: None,
        })
        .await
        .unwrap();

        jobs.cancel("job-1").await;

        let snapshots = jobs.queue_wait_snapshots().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].priority, "background");
        assert_eq!(snapshots[0].kind, "index_build");
        assert_eq!(snapshots[0].count, 1);
    }

    #[tokio::test]
    async fn metrics_reports_queue_depth_and_wait_time_for_queued_jobs() {
        let app = build_router(test_state());

        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "vision_analysis", "priority": "batch"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "vision_analysis", "priority": "batch"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "cluster", "priority": "background"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        app.clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/jobs/job-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let response = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let metrics = response_text(response).await;
        assert!(metrics
            .contains("fairlead_job_queue_depth{priority=\"batch\",type=\"vision_analysis\"} 2"));
        assert!(metrics.contains(
            "fairlead_job_queue_wait_seconds_sum{priority=\"batch\",type=\"vision_analysis\"} "
        ));
        assert!(metrics.contains(
            "fairlead_job_queue_wait_seconds_max{priority=\"batch\",type=\"vision_analysis\"} "
        ));
        assert!(
            !metrics.contains("fairlead_job_queue_depth{priority=\"background\",type=\"cluster\"}")
        );
        assert!(!metrics.contains(
            "fairlead_job_queue_wait_seconds_sum{priority=\"background\",type=\"cluster\"}"
        ));
        assert!(!metrics.contains(
            "fairlead_job_queue_wait_seconds_max{priority=\"background\",type=\"cluster\"}"
        ));
    }

    #[tokio::test]
    async fn cancel_terminal_job_returns_conflict() {
        let app = build_router(test_state());

        app.clone()
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"type": "cluster"}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        app.clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/jobs/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/v1/jobs/job-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let value = response_json(response).await;
        assert_eq!(value["job"]["status"], "cancelled");
    }

    #[tokio::test]
    async fn get_unknown_job_returns_404() {
        let app = build_router(test_state());

        let response = app
            .oneshot(
                Request::get("/v1/jobs/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn empty_callback_url_returns_400() {
        let app = build_router(test_state());

        let response = app
            .oneshot(
                Request::post("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"type": "vision_analysis", "callback_url": "   "}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn response_text(response: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}
