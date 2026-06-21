use axum::{
    extract::State,
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

use crate::{config::WorkloadKind, AppState};

#[derive(Debug, Clone)]
pub struct ResourceRoutingPolicy {
    pub enabled: bool,
    pub chat_completions_required_vram_mb: u64,
    pub embeddings_required_vram_mb: u64,
}

impl Default for ResourceRoutingPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            chat_completions_required_vram_mb: 1024,
            embeddings_required_vram_mb: 512,
        }
    }
}

impl ResourceRoutingPolicy {
    pub fn required_vram_mb(&self, workload: &WorkloadKind) -> u64 {
        match workload {
            WorkloadKind::ChatCompletions => self.chat_completions_required_vram_mb,
            WorkloadKind::Embeddings => self.embeddings_required_vram_mb,
        }
    }
}

#[derive(Clone)]
pub struct ResourceRegistry {
    inner: Arc<RwLock<ResourceRegistryInner>>,
    stale_after: Duration,
}

#[derive(Default)]
struct ResourceRegistryInner {
    reports: HashMap<ResourceKey, ResourceEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResourceKey {
    node_id: String,
    backend_id: String,
}

#[derive(Debug, Clone)]
struct ResourceEntry {
    report: ResourceReport,
    observed_at: Instant,
    reported_at: SystemTime,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceReportRequest {
    pub node_id: String,
    #[serde(default)]
    pub backend_id: Option<String>,
    pub total_vram_mb: u64,
    pub reserved_vram_mb: u64,
    #[serde(default)]
    pub current_load: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceReport {
    pub node_id: String,
    pub backend_id: Option<String>,
    pub total_vram_mb: u64,
    pub reserved_vram_mb: u64,
    pub available_vram_mb: u64,
    pub current_load: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResourceSnapshot {
    #[serde(flatten)]
    pub report: ResourceReport,
    pub reported_at_unix_ms: u128,
    pub age_seconds: f64,
    pub stale: bool,
}

#[derive(Debug, Serialize)]
pub struct ResourceListResponse {
    pub resources: Vec<ResourceSnapshot>,
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl ResourceRegistry {
    pub fn new(stale_after: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ResourceRegistryInner::default())),
            stale_after,
        }
    }

    pub async fn report(&self, request: ResourceReportRequest) -> Result<ResourceSnapshot, String> {
        let report = validate_report(request)?;
        let key = ResourceKey {
            node_id: report.node_id.clone(),
            backend_id: report.backend_id.clone().unwrap_or_default(),
        };
        let entry = ResourceEntry {
            report,
            observed_at: Instant::now(),
            reported_at: SystemTime::now(),
        };

        let snapshot = snapshot_from_entry(&entry, self.stale_after);
        self.inner.write().await.reports.insert(key, entry);
        Ok(snapshot)
    }

    pub async fn snapshots(&self) -> Vec<ResourceSnapshot> {
        let guard = self.inner.read().await;
        let mut snapshots: Vec<_> = guard
            .reports
            .values()
            .map(|entry| snapshot_from_entry(entry, self.stale_after))
            .collect();
        snapshots.sort_by(|a, b| {
            a.report
                .node_id
                .cmp(&b.report.node_id)
                .then_with(|| a.report.backend_id.cmp(&b.report.backend_id))
        });
        snapshots
    }

    pub async fn fresh_report(
        &self,
        node_id: &str,
        backend_id: Option<&str>,
    ) -> Option<ResourceReport> {
        let key = ResourceKey {
            node_id: node_id.to_string(),
            backend_id: backend_id.unwrap_or("").to_string(),
        };
        let guard = self.inner.read().await;
        let entry = guard.reports.get(&key)?;
        if entry.observed_at.elapsed() > self.stale_after {
            return None;
        }
        Some(entry.report.clone())
    }

    pub async fn fresh_backend_report(
        &self,
        node_id: &str,
        backend_id: &str,
    ) -> Option<ResourceReport> {
        if let Some(report) = self.fresh_report(node_id, Some(backend_id)).await {
            return Some(report);
        }
        self.fresh_report(node_id, None).await
    }
}

pub async fn report_resources(
    State(state): State<AppState>,
    Json(request): Json<ResourceReportRequest>,
) -> Response {
    match state.resources.report(request).await {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

pub async fn list_resources(State(state): State<AppState>) -> Json<ResourceListResponse> {
    Json(ResourceListResponse {
        resources: state.resources.snapshots().await,
    })
}

fn validate_report(request: ResourceReportRequest) -> Result<ResourceReport, String> {
    let node_id = request.node_id.trim();
    if node_id.is_empty() {
        return Err("node_id cannot be empty".into());
    }

    let backend_id = request
        .backend_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    if request.reserved_vram_mb > request.total_vram_mb {
        return Err("reserved_vram_mb cannot exceed total_vram_mb".into());
    }

    if let Some(load) = request.current_load {
        if !(0.0..=1.0).contains(&load) {
            return Err("current_load must be between 0.0 and 1.0".into());
        }
    }

    Ok(ResourceReport {
        node_id: node_id.to_string(),
        backend_id,
        total_vram_mb: request.total_vram_mb,
        reserved_vram_mb: request.reserved_vram_mb,
        available_vram_mb: request.total_vram_mb - request.reserved_vram_mb,
        current_load: request.current_load,
    })
}

fn snapshot_from_entry(entry: &ResourceEntry, stale_after: Duration) -> ResourceSnapshot {
    let age = entry.observed_at.elapsed();
    ResourceSnapshot {
        report: entry.report.clone(),
        reported_at_unix_ms: entry
            .reported_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        age_seconds: age.as_secs_f64(),
        stale: age > stale_after,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{build_router, metrics::RoutingMetrics, router::SessionAffinity, AppState};
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use serde_json::json;
    use tower::ServiceExt;

    fn report(node_id: &str, backend_id: Option<&str>) -> ResourceReportRequest {
        ResourceReportRequest {
            node_id: node_id.to_string(),
            backend_id: backend_id.map(str::to_string),
            total_vram_mb: 64_000,
            reserved_vram_mb: 16_000,
            current_load: Some(0.25),
        }
    }

    #[tokio::test]
    async fn report_registers_resource_snapshot() {
        let registry = ResourceRegistry::new(Duration::from_secs(30));
        let snapshot = registry
            .report(report("spark-a", Some("spark-a-vllm")))
            .await
            .unwrap();

        assert_eq!(snapshot.report.node_id, "spark-a");
        assert_eq!(snapshot.report.backend_id.as_deref(), Some("spark-a-vllm"));
        assert_eq!(snapshot.report.total_vram_mb, 64_000);
        assert_eq!(snapshot.report.reserved_vram_mb, 16_000);
        assert_eq!(snapshot.report.available_vram_mb, 48_000);
        assert_eq!(snapshot.report.current_load, Some(0.25));
        assert!(!snapshot.stale);
    }

    #[tokio::test]
    async fn report_updates_existing_node_backend_pair() {
        let registry = ResourceRegistry::new(Duration::from_secs(30));
        registry
            .report(report("spark-a", Some("spark-a-vllm")))
            .await
            .unwrap();
        registry
            .report(ResourceReportRequest {
                node_id: "spark-a".into(),
                backend_id: Some("spark-a-vllm".into()),
                total_vram_mb: 64_000,
                reserved_vram_mb: 60_000,
                current_load: Some(0.9),
            })
            .await
            .unwrap();

        let snapshots = registry.snapshots().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].report.available_vram_mb, 4_000);
        assert_eq!(snapshots[0].report.current_load, Some(0.9));
    }

    #[tokio::test]
    async fn fresh_report_returns_none_after_stale_timeout() {
        let registry = ResourceRegistry::new(Duration::from_millis(10));
        registry
            .report(report("spark-a", Some("spark-a-vllm")))
            .await
            .unwrap();

        assert!(registry
            .fresh_report("spark-a", Some("spark-a-vllm"))
            .await
            .is_some());

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(registry
            .fresh_report("spark-a", Some("spark-a-vllm"))
            .await
            .is_none());
        assert!(registry.snapshots().await[0].stale);
    }

    #[tokio::test]
    async fn fresh_backend_report_falls_back_to_node_level_report() {
        let registry = ResourceRegistry::default();
        registry.report(report("spark-a", None)).await.unwrap();

        let report = registry
            .fresh_backend_report("spark-a", "spark-a-vllm")
            .await
            .unwrap();

        assert_eq!(report.node_id, "spark-a");
        assert_eq!(report.backend_id, None);
        assert_eq!(report.available_vram_mb, 48_000);
    }

    #[tokio::test]
    async fn invalid_report_rejects_reserved_vram_above_total() {
        let registry = ResourceRegistry::default();
        let result = registry
            .report(ResourceReportRequest {
                node_id: "spark-a".into(),
                backend_id: None,
                total_vram_mb: 10,
                reserved_vram_mb: 11,
                current_load: None,
            })
            .await;

        assert_eq!(
            result.unwrap_err(),
            "reserved_vram_mb cannot exceed total_vram_mb"
        );
    }

    #[tokio::test]
    async fn invalid_report_rejects_load_outside_zero_to_one() {
        let registry = ResourceRegistry::default();
        let result = registry
            .report(ResourceReportRequest {
                node_id: "spark-a".into(),
                backend_id: None,
                total_vram_mb: 10,
                reserved_vram_mb: 1,
                current_load: Some(1.1),
            })
            .await;

        assert_eq!(
            result.unwrap_err(),
            "current_load must be between 0.0 and 1.0"
        );
    }

    fn test_state(resources: ResourceRegistry) -> AppState {
        AppState {
            client: reqwest::Client::new(),
            backends: vec![],
            affinity: SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            resources,
            resource_policy: ResourceRoutingPolicy::default(),
            priority_limiter: crate::priority::PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        }
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn post_report_endpoint_registers_resources() {
        let resources = ResourceRegistry::default();
        let app = build_router(test_state(resources.clone()));

        let response = app
            .oneshot(
                Request::post("/v1/resources/report")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "node_id": "spark-a",
                            "backend_id": "spark-a-vllm",
                            "total_vram_mb": 64000,
                            "reserved_vram_mb": 16000,
                            "current_load": 0.25
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let json = response_json(response).await;
        assert_eq!(json["node_id"], "spark-a");
        assert_eq!(json["backend_id"], "spark-a-vllm");
        assert_eq!(json["available_vram_mb"], 48_000);

        let snapshots = resources.snapshots().await;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].report.node_id, "spark-a");
    }

    #[tokio::test]
    async fn get_resources_endpoint_lists_snapshots() {
        let resources = ResourceRegistry::default();
        resources
            .report(report("spark-a", Some("spark-a-vllm")))
            .await
            .unwrap();
        let app = build_router(test_state(resources));

        let response = app
            .oneshot(Request::get("/v1/resources").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let json = response_json(response).await;
        assert_eq!(json["resources"][0]["node_id"], "spark-a");
        assert_eq!(json["resources"][0]["backend_id"], "spark-a-vllm");
        assert_eq!(json["resources"][0]["available_vram_mb"], 48_000);
    }

    #[tokio::test]
    async fn post_report_endpoint_rejects_invalid_payload() {
        let app = build_router(test_state(ResourceRegistry::default()));

        let response = app
            .oneshot(
                Request::post("/v1/resources/report")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "node_id": "",
                            "total_vram_mb": 10,
                            "reserved_vram_mb": 1
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert!(std::str::from_utf8(&bytes)
            .unwrap()
            .contains("node_id cannot be empty"));
    }
}
