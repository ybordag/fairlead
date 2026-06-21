use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

use crate::{jobs::JobKind, AppState};

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterWorkerRequest {
    pub id: String,
    pub endpoint_url: String,
    #[serde(default)]
    pub node_id: Option<String>,
    pub job_types: Vec<JobKind>,
    #[serde(default)]
    pub max_concurrent_jobs: Option<usize>,
    #[serde(default)]
    pub available_vram_mb: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerSnapshot {
    pub id: String,
    pub endpoint_url: String,
    pub node_id: Option<String>,
    pub job_types: Vec<JobKind>,
    pub max_concurrent_jobs: Option<usize>,
    pub available_vram_mb: Option<u64>,
    pub registered_at_unix_ms: u128,
    pub last_seen_unix_ms: u128,
    pub age_seconds: f64,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerResponse {
    pub worker: WorkerSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkerListResponse {
    pub workers: Vec<WorkerSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerAvailabilitySnapshot {
    pub job_type: &'static str,
    pub status: &'static str,
    pub count: usize,
}

#[derive(Clone)]
pub struct WorkerRegistry {
    inner: Arc<RwLock<WorkerRegistryInner>>,
    stale_after: Duration,
}

#[derive(Default)]
struct WorkerRegistryInner {
    workers: HashMap<String, WorkerEntry>,
}

#[derive(Debug, Clone)]
struct WorkerEntry {
    id: String,
    endpoint_url: String,
    node_id: Option<String>,
    job_types: Vec<JobKind>,
    max_concurrent_jobs: Option<usize>,
    available_vram_mb: Option<u64>,
    registered_at: SystemTime,
    last_seen_at: SystemTime,
    observed_at: Instant,
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl WorkerRegistry {
    pub fn new(stale_after: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(WorkerRegistryInner::default())),
            stale_after,
        }
    }

    pub async fn register(&self, request: RegisterWorkerRequest) -> Result<WorkerSnapshot, String> {
        let entry = validate_worker(request)?;
        let now = SystemTime::now();
        let mut guard = self.inner.write().await;

        let registered_at = guard
            .workers
            .get(&entry.id)
            .map(|existing| existing.registered_at)
            .unwrap_or(now);

        let entry = WorkerEntry {
            registered_at,
            last_seen_at: now,
            observed_at: Instant::now(),
            ..entry
        };
        let snapshot = snapshot_from_entry(&entry, self.stale_after);
        guard.workers.insert(entry.id.clone(), entry);
        Ok(snapshot)
    }

    pub async fn heartbeat(&self, id: &str) -> Option<WorkerSnapshot> {
        let mut guard = self.inner.write().await;
        let worker = guard.workers.get_mut(id)?;
        worker.last_seen_at = SystemTime::now();
        worker.observed_at = Instant::now();
        Some(snapshot_from_entry(worker, self.stale_after))
    }

    pub async fn list(&self) -> Vec<WorkerSnapshot> {
        let guard = self.inner.read().await;
        let mut workers: Vec<_> = guard
            .workers
            .values()
            .map(|entry| snapshot_from_entry(entry, self.stale_after))
            .collect();
        workers.sort_by(|a, b| a.id.cmp(&b.id));
        workers
    }

    pub async fn availability_snapshots(&self) -> Vec<WorkerAvailabilitySnapshot> {
        let workers = self.list().await;
        let mut snapshots: Vec<WorkerAvailabilitySnapshot> = Vec::new();

        for worker in workers {
            let status = if worker.stale { "stale" } else { "available" };
            for job_type in worker.job_types {
                if let Some(snapshot) = snapshots.iter_mut().find(|snapshot| {
                    snapshot.job_type == job_type.as_str() && snapshot.status == status
                }) {
                    snapshot.count += 1;
                } else {
                    snapshots.push(WorkerAvailabilitySnapshot {
                        job_type: job_type.as_str(),
                        status,
                        count: 1,
                    });
                }
            }
        }

        snapshots.sort_by_key(|snapshot| {
            (
                job_type_rank(snapshot.job_type),
                status_rank(snapshot.status),
            )
        });
        snapshots
    }
}

pub async fn register_worker(
    State(state): State<AppState>,
    Json(request): Json<RegisterWorkerRequest>,
) -> Response {
    match state.workers.register(request).await {
        Ok(worker) => Json(WorkerResponse { worker }).into_response(),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

pub async fn heartbeat_worker(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    match state.workers.heartbeat(&id).await {
        Some(worker) => Json(WorkerResponse { worker }).into_response(),
        None => (StatusCode::NOT_FOUND, "worker not found").into_response(),
    }
}

pub async fn list_workers(State(state): State<AppState>) -> Json<WorkerListResponse> {
    Json(WorkerListResponse {
        workers: state.workers.list().await,
    })
}

fn validate_worker(request: RegisterWorkerRequest) -> Result<WorkerEntry, String> {
    let id = request.id.trim();
    if id.is_empty() {
        return Err("worker id cannot be empty".into());
    }

    let endpoint_url = request.endpoint_url.trim();
    if endpoint_url.is_empty() {
        return Err("endpoint_url cannot be empty".into());
    }
    if !(endpoint_url.starts_with("http://") || endpoint_url.starts_with("https://")) {
        return Err("endpoint_url must start with http:// or https://".into());
    }

    if request.job_types.is_empty() {
        return Err("worker must support at least one job type".into());
    }

    if request.max_concurrent_jobs == Some(0) {
        return Err("max_concurrent_jobs must be greater than zero".into());
    }

    let node_id = request
        .node_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    Ok(WorkerEntry {
        id: id.to_string(),
        endpoint_url: endpoint_url.to_string(),
        node_id,
        job_types: request.job_types,
        max_concurrent_jobs: request.max_concurrent_jobs,
        available_vram_mb: request.available_vram_mb,
        registered_at: SystemTime::now(),
        last_seen_at: SystemTime::now(),
        observed_at: Instant::now(),
    })
}

fn snapshot_from_entry(entry: &WorkerEntry, stale_after: Duration) -> WorkerSnapshot {
    let age = entry.observed_at.elapsed();
    WorkerSnapshot {
        id: entry.id.clone(),
        endpoint_url: entry.endpoint_url.clone(),
        node_id: entry.node_id.clone(),
        job_types: entry.job_types.clone(),
        max_concurrent_jobs: entry.max_concurrent_jobs,
        available_vram_mb: entry.available_vram_mb,
        registered_at_unix_ms: unix_ms(entry.registered_at),
        last_seen_unix_ms: unix_ms(entry.last_seen_at),
        age_seconds: age.as_secs_f64(),
        stale: age >= stale_after,
    }
}

fn unix_ms(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn job_type_rank(job_type: &str) -> u8 {
    match job_type {
        "vision_analysis" => 0,
        "embed_batch" => 1,
        "index_build" => 2,
        "cluster" => 3,
        _ => 4,
    }
}

fn status_rank(status: &str) -> u8 {
    match status {
        "available" => 0,
        "stale" => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        build_router,
        jobs::JobRegistry,
        metrics::RoutingMetrics,
        priority::PriorityLimiter,
        resources::{ResourceRegistry, ResourceRoutingPolicy},
        router::SessionAffinity,
        AppState,
    };
    use axum::{body::Body, http::Request};
    use serde_json::json;
    use tower::ServiceExt;

    fn test_state(workers: WorkerRegistry) -> AppState {
        AppState {
            client: reqwest::Client::new(),
            backends: vec![],
            affinity: SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            resources: ResourceRegistry::default(),
            resource_policy: ResourceRoutingPolicy::default(),
            priority_limiter: PriorityLimiter::default(),
            jobs: JobRegistry::default(),
            workers,
        }
    }

    #[tokio::test]
    async fn register_worker_returns_worker_metadata() {
        let app = build_router(test_state(WorkerRegistry::default()));

        let response = app
            .oneshot(
                Request::post("/v1/workers/register")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "id": "vision-a",
                            "endpoint_url": " http://vision-a:9000 ",
                            "node_id": " node-a ",
                            "job_types": ["vision_analysis", "embed_batch"],
                            "max_concurrent_jobs": 2,
                            "available_vram_mb": 24576
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["worker"]["id"], "vision-a");
        assert_eq!(value["worker"]["endpoint_url"], "http://vision-a:9000");
        assert_eq!(value["worker"]["node_id"], "node-a");
        assert_eq!(
            value["worker"]["job_types"],
            json!(["vision_analysis", "embed_batch"])
        );
        assert_eq!(value["worker"]["max_concurrent_jobs"], 2);
        assert_eq!(value["worker"]["available_vram_mb"], 24576);
        assert_eq!(value["worker"]["stale"], false);
    }

    #[tokio::test]
    async fn register_worker_upserts_existing_worker() {
        let app = build_router(test_state(WorkerRegistry::default()));

        for endpoint_url in ["http://worker-a:9000", "http://worker-a:9001"] {
            app.clone()
                .oneshot(
                    Request::post("/v1/workers/register")
                        .header("content-type", "application/json")
                        .body(Body::from(
                            json!({
                                "id": "worker-a",
                                "endpoint_url": endpoint_url,
                                "job_types": ["index_build"]
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let response = app
            .oneshot(Request::get("/v1/workers").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["workers"].as_array().unwrap().len(), 1);
        assert_eq!(value["workers"][0]["endpoint_url"], "http://worker-a:9001");
    }

    #[tokio::test]
    async fn heartbeat_refreshes_registered_worker() {
        let workers = WorkerRegistry::new(Duration::ZERO);
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
        let app = build_router(test_state(workers));

        let response = app
            .oneshot(
                Request::post("/v1/workers/worker-a/heartbeat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["worker"]["id"], "worker-a");
        assert_eq!(value["worker"]["stale"], true);
    }

    #[tokio::test]
    async fn heartbeat_unknown_worker_returns_404() {
        let app = build_router(test_state(WorkerRegistry::default()));

        let response = app
            .oneshot(
                Request::post("/v1/workers/missing/heartbeat")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_workers_sorts_by_id_and_marks_stale() {
        let workers = WorkerRegistry::new(Duration::ZERO);
        workers
            .register(RegisterWorkerRequest {
                id: "worker-b".into(),
                endpoint_url: "http://worker-b:9000".into(),
                node_id: None,
                job_types: vec![JobKind::VisionAnalysis],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();
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
        let app = build_router(test_state(workers));

        let response = app
            .oneshot(Request::get("/v1/workers").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let value = response_json(response).await;
        assert_eq!(value["workers"][0]["id"], "worker-a");
        assert_eq!(value["workers"][0]["stale"], true);
        assert_eq!(value["workers"][1]["id"], "worker-b");
    }

    #[tokio::test]
    async fn invalid_worker_registration_returns_400() {
        let app = build_router(test_state(WorkerRegistry::default()));

        for body in [
            json!({"id": " ", "endpoint_url": "http://worker:9000", "job_types": ["cluster"]}),
            json!({"id": "worker", "endpoint_url": " ", "job_types": ["cluster"]}),
            json!({"id": "worker", "endpoint_url": "worker:9000", "job_types": ["cluster"]}),
            json!({"id": "worker", "endpoint_url": "http://worker:9000", "job_types": []}),
            json!({"id": "worker", "endpoint_url": "http://worker:9000", "job_types": ["cluster"], "max_concurrent_jobs": 0}),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/v1/workers/register")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn availability_snapshots_count_workers_by_job_type_and_status() {
        let workers = WorkerRegistry::new(Duration::ZERO);
        workers
            .register(RegisterWorkerRequest {
                id: "worker-a".into(),
                endpoint_url: "http://worker-a:9000".into(),
                node_id: None,
                job_types: vec![JobKind::VisionAnalysis, JobKind::EmbedBatch],
                max_concurrent_jobs: None,
                available_vram_mb: None,
            })
            .await
            .unwrap();

        assert_eq!(
            workers.availability_snapshots().await,
            vec![
                WorkerAvailabilitySnapshot {
                    job_type: "vision_analysis",
                    status: "stale",
                    count: 1,
                },
                WorkerAvailabilitySnapshot {
                    job_type: "embed_batch",
                    status: "stale",
                    count: 1,
                },
            ]
        );
    }

    #[tokio::test]
    async fn metrics_reports_worker_availability_by_job_type_and_status() {
        let app = build_router(test_state(WorkerRegistry::default()));

        app.clone()
            .oneshot(
                Request::post("/v1/workers/register")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "id": "vision-a",
                            "endpoint_url": "http://vision-a:9000",
                            "job_types": ["vision_analysis", "embed_batch"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = app
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let metrics = response_text(response).await;
        assert!(
            metrics.contains("fairlead_workers{type=\"vision_analysis\",status=\"available\"} 1")
        );
        assert!(metrics.contains("fairlead_workers{type=\"embed_batch\",status=\"available\"} 1"));
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
