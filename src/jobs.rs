use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
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

#[derive(Clone, Default)]
pub struct JobRegistry {
    inner: Arc<RwLock<JobRegistryInner>>,
}

#[derive(Default)]
struct JobRegistryInner {
    next_id: u64,
    jobs: HashMap<String, JobRecord>,
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
        Ok(job)
    }

    pub async fn get(&self, id: &str) -> Option<JobRecord> {
        self.inner.read().await.jobs.get(id).cloned()
    }

    pub async fn cancel(&self, id: &str) -> CancelJobResult {
        let mut guard = self.inner.write().await;
        let Some(job) = guard.jobs.get_mut(id) else {
            return CancelJobResult::NotFound;
        };

        if job.status.is_terminal() {
            return CancelJobResult::AlreadyTerminal(job.clone());
        }

        job.status = JobStatus::Cancelled;
        job.updated_at_unix_ms = unix_ms();
        CancelJobResult::Cancelled(job.clone())
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
}
