use axum::{extract::State, response::Response};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{router::CircuitState, AppState};

#[derive(Clone, Default)]
pub struct RoutingMetrics {
    inner: Arc<Mutex<RoutingMetricsInner>>,
}

#[derive(Default)]
struct RoutingMetricsInner {
    requests: HashMap<RequestLabels, RequestAggregate>,
    retries: HashMap<RetryLabels, u64>,
    fallbacks: HashMap<FallbackLabels, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestLabels {
    pub workload: String,
    pub backend: String,
    pub node: String,
    pub pool: String,
    pub origin_node: String,
    pub status: u16,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RetryLabels {
    pub workload: String,
    pub backend: String,
    pub node: String,
    pub pool: String,
    pub origin_node: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FallbackLabels {
    pub workload: String,
    pub backend: String,
    pub node: String,
    pub pool: String,
    pub origin_node: String,
    pub reason: String,
}

#[derive(Default)]
struct RequestAggregate {
    count: u64,
    latency_sum_seconds: f64,
}

impl RoutingMetrics {
    pub fn record_request(&self, labels: RequestLabels, latency: Duration) {
        let mut guard = self.inner.lock().expect("routing metrics mutex poisoned");
        let aggregate = guard.requests.entry(labels).or_default();
        aggregate.count += 1;
        aggregate.latency_sum_seconds += latency.as_secs_f64();
    }

    pub fn record_retry(&self, labels: RetryLabels) {
        let mut guard = self.inner.lock().expect("routing metrics mutex poisoned");
        *guard.retries.entry(labels).or_default() += 1;
    }

    pub fn record_fallback(&self, labels: FallbackLabels) {
        let mut guard = self.inner.lock().expect("routing metrics mutex poisoned");
        *guard.fallbacks.entry(labels).or_default() += 1;
    }

    fn render(&self) -> String {
        let guard = self.inner.lock().expect("routing metrics mutex poisoned");
        let mut body = String::from(
            "# HELP fairlead_requests_total Synchronous proxy requests by routing outcome\n\
             # TYPE fairlead_requests_total counter\n",
        );

        for (labels, aggregate) in &guard.requests {
            body.push_str(&format!(
                "fairlead_requests_total{{workload=\"{}\",backend=\"{}\",node=\"{}\",pool=\"{}\",origin_node=\"{}\",status=\"{}\",outcome=\"{}\"}} {}\n",
                prometheus_escape(&labels.workload),
                prometheus_escape(&labels.backend),
                prometheus_escape(&labels.node),
                prometheus_escape(&labels.pool),
                prometheus_escape(&labels.origin_node),
                labels.status,
                prometheus_escape(&labels.outcome),
                aggregate.count,
            ));
        }

        body.push_str(
            "# HELP fairlead_request_latency_seconds Synchronous proxy request latency\n\
             # TYPE fairlead_request_latency_seconds summary\n",
        );

        for (labels, aggregate) in &guard.requests {
            body.push_str(&format!(
                "fairlead_request_latency_seconds_count{{workload=\"{}\",backend=\"{}\",node=\"{}\",pool=\"{}\",origin_node=\"{}\",status=\"{}\",outcome=\"{}\"}} {}\n",
                prometheus_escape(&labels.workload),
                prometheus_escape(&labels.backend),
                prometheus_escape(&labels.node),
                prometheus_escape(&labels.pool),
                prometheus_escape(&labels.origin_node),
                labels.status,
                prometheus_escape(&labels.outcome),
                aggregate.count,
            ));
            body.push_str(&format!(
                "fairlead_request_latency_seconds_sum{{workload=\"{}\",backend=\"{}\",node=\"{}\",pool=\"{}\",origin_node=\"{}\",status=\"{}\",outcome=\"{}\"}} {:.6}\n",
                prometheus_escape(&labels.workload),
                prometheus_escape(&labels.backend),
                prometheus_escape(&labels.node),
                prometheus_escape(&labels.pool),
                prometheus_escape(&labels.origin_node),
                labels.status,
                prometheus_escape(&labels.outcome),
                aggregate.latency_sum_seconds,
            ));
        }

        body.push_str(
            "# HELP fairlead_retries_total Same-request retry attempts by failed backend and reason\n\
             # TYPE fairlead_retries_total counter\n",
        );

        for (labels, count) in &guard.retries {
            body.push_str(&format!(
                "fairlead_retries_total{{workload=\"{}\",backend=\"{}\",node=\"{}\",pool=\"{}\",origin_node=\"{}\",reason=\"{}\"}} {}\n",
                prometheus_escape(&labels.workload),
                prometheus_escape(&labels.backend),
                prometheus_escape(&labels.node),
                prometheus_escape(&labels.pool),
                prometheus_escape(&labels.origin_node),
                prometheus_escape(&labels.reason),
                count,
            ));
        }

        body.push_str(
            "# HELP fairlead_fallbacks_total Fallback selections by selected backend and reason\n\
             # TYPE fairlead_fallbacks_total counter\n",
        );

        for (labels, count) in &guard.fallbacks {
            body.push_str(&format!(
                "fairlead_fallbacks_total{{workload=\"{}\",backend=\"{}\",node=\"{}\",pool=\"{}\",origin_node=\"{}\",reason=\"{}\"}} {}\n",
                prometheus_escape(&labels.workload),
                prometheus_escape(&labels.backend),
                prometheus_escape(&labels.node),
                prometheus_escape(&labels.pool),
                prometheus_escape(&labels.origin_node),
                prometheus_escape(&labels.reason),
                count,
            ));
        }

        body
    }
}

pub async fn metrics(State(state): State<AppState>) -> Response<String> {
    let mut body = String::from(
        "# HELP fairlead_circuit_state Circuit state per backend (0=closed 1=half_open 2=open)\n\
         # TYPE fairlead_circuit_state gauge\n",
    );

    for backend in &state.backends {
        let value: u8 = {
            let guard = backend.circuit.read().await;
            match guard.state() {
                CircuitState::Closed => 0,
                CircuitState::HalfOpen => 1,
                CircuitState::Open { .. } => 2,
            }
        };
        // Escape any quotes so Prometheus labels remain valid.
        let backend_id = prometheus_escape(&backend.id);
        let url = prometheus_escape(&backend.url);
        let node = prometheus_escape(backend.node_id.as_deref().unwrap_or(""));
        let pool = prometheus_escape(&backend.pool);
        body.push_str(&format!(
            "fairlead_circuit_state{{backend=\"{backend_id}\",url=\"{url}\",node=\"{node}\",pool=\"{pool}\"}} {value}\n"
        ));
    }
    body.push_str(&state.metrics.render());
    body.push_str(&render_resource_metrics(&state).await);

    Response::builder()
        .status(200)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(body)
        .unwrap()
}

async fn render_resource_metrics(state: &AppState) -> String {
    let snapshots = state.resources.snapshots().await;
    let mut body = String::from(
        "# HELP fairlead_resource_vram_total_mb Reported total VRAM by node/backend\n\
         # TYPE fairlead_resource_vram_total_mb gauge\n\
         # HELP fairlead_resource_vram_reserved_mb Reported reserved VRAM by node/backend\n\
         # TYPE fairlead_resource_vram_reserved_mb gauge\n\
         # HELP fairlead_resource_vram_available_mb Reported available VRAM by node/backend\n\
         # TYPE fairlead_resource_vram_available_mb gauge\n\
         # HELP fairlead_resource_load Reported normalized load by node/backend\n\
         # TYPE fairlead_resource_load gauge\n\
         # HELP fairlead_resource_report_stale Whether the latest resource report is stale (0=fresh 1=stale)\n\
         # TYPE fairlead_resource_report_stale gauge\n",
    );

    for snapshot in snapshots {
        let node = prometheus_escape(&snapshot.report.node_id);
        let backend = prometheus_escape(snapshot.report.backend_id.as_deref().unwrap_or(""));
        let stale = u8::from(snapshot.stale);
        body.push_str(&format!(
            "fairlead_resource_vram_total_mb{{node=\"{node}\",backend=\"{backend}\"}} {}\n",
            snapshot.report.total_vram_mb,
        ));
        body.push_str(&format!(
            "fairlead_resource_vram_reserved_mb{{node=\"{node}\",backend=\"{backend}\"}} {}\n",
            snapshot.report.reserved_vram_mb,
        ));
        body.push_str(&format!(
            "fairlead_resource_vram_available_mb{{node=\"{node}\",backend=\"{backend}\"}} {}\n",
            snapshot.report.available_vram_mb,
        ));
        if let Some(load) = snapshot.report.current_load {
            body.push_str(&format!(
                "fairlead_resource_load{{node=\"{node}\",backend=\"{backend}\"}} {:.6}\n",
                load,
            ));
        }
        body.push_str(&format!(
            "fairlead_resource_report_stale{{node=\"{node}\",backend=\"{backend}\"}} {stale}\n",
        ));
    }

    body
}

fn prometheus_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::BackendState;
    use axum::{body::Body, routing::get, Router};
    use http_body_util::BodyExt;
    use std::time::Duration;
    use tower::ServiceExt;

    fn router_with_backends(backends: Vec<BackendState>) -> Router {
        let state = AppState {
            client: reqwest::Client::new(),
            backends,
            affinity: crate::router::SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            resources: crate::resources::ResourceRegistry::default(),
        };
        router_with_state(state)
    }

    fn router_with_state(state: AppState) -> Router {
        Router::new()
            .route("/metrics", get(metrics))
            .with_state(state)
    }

    async fn body_text(resp: axum::response::Response) -> String {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn metrics_reports_closed_for_healthy_backend() {
        let backends = vec![BackendState::new(
            "http://node-a:8000/v1".into(),
            3,
            Duration::from_secs(30),
        )];
        let app = router_with_backends(backends);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let text = body_text(resp).await;
        assert!(text.contains(
            "fairlead_circuit_state{backend=\"backend-0\",url=\"http://node-a:8000/v1\",node=\"\",pool=\"default\"} 0"
        ));
    }

    #[tokio::test]
    async fn metrics_content_type_is_prometheus_format() {
        let app = router_with_backends(vec![]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert_eq!(content_type, "text/plain; version=0.0.4; charset=utf-8");
    }

    #[tokio::test]
    async fn metrics_escapes_quotes_in_backend_label() {
        let backend = BackendState::from_config(
            crate::config::BackendConfig {
                id: "node-\"a".into(),
                url: "http://node-a:8000/v1?name=\"quoted\"".into(),
                node_id: Some("node-\"a".into()),
                pool: "local-\"llm".into(),
                workloads: crate::config::WorkloadKind::default_proxy_workloads(),
                health_path: None,
            },
            3,
            Duration::from_secs(30),
        );
        let app = router_with_backends(vec![backend]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(text.contains("backend=\"node-\\\"a\""));
        assert!(text.contains("url=\"http://node-a:8000/v1?name=\\\"quoted\\\"\""));
        assert!(text.contains("node=\"node-\\\"a\""));
        assert!(text.contains("pool=\"local-\\\"llm\""));
    }

    #[tokio::test]
    async fn metrics_reports_open_after_failures() {
        let backend = BackendState::new("http://node-b:8000/v1".into(), 1, Duration::from_secs(30));
        // Trip the circuit manually.
        backend.circuit.write().await.record_failure();

        let app = router_with_backends(vec![backend]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(text.contains(
            "fairlead_circuit_state{backend=\"backend-0\",url=\"http://node-b:8000/v1\",node=\"\",pool=\"default\"} 2"
        ));
    }

    #[tokio::test]
    async fn metrics_reports_half_open() {
        let backend =
            BackendState::new("http://node-a:8000/v1".into(), 1, Duration::from_millis(10));
        backend.circuit.write().await.record_failure();
        tokio::time::sleep(Duration::from_millis(20)).await;
        // is_available() transitions Open → HalfOpen once the cooldown has elapsed.
        backend.circuit.write().await.is_available();

        let app = router_with_backends(vec![backend]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(
            text.contains(
                "fairlead_circuit_state{backend=\"backend-0\",url=\"http://node-a:8000/v1\",node=\"\",pool=\"default\"} 1"
            ),
            "HalfOpen should report value 1, got:\n{text}"
        );
    }

    #[tokio::test]
    async fn metrics_reports_multiple_backends() {
        let healthy = BackendState::new("http://node-a:8000/v1".into(), 3, Duration::from_secs(30));
        let broken = BackendState::new("http://node-b:8000/v1".into(), 1, Duration::from_secs(30));
        broken.circuit.write().await.record_failure();

        let app = router_with_backends(vec![healthy, broken]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(text.contains(
            "fairlead_circuit_state{backend=\"backend-0\",url=\"http://node-a:8000/v1\",node=\"\",pool=\"default\"} 0"
        ));
        assert!(text.contains(
            "fairlead_circuit_state{backend=\"backend-0\",url=\"http://node-b:8000/v1\",node=\"\",pool=\"default\"} 2"
        ));
    }

    #[tokio::test]
    async fn metrics_reports_backend_metadata_labels() {
        let backend = BackendState::from_config(
            crate::config::BackendConfig {
                id: "node-a-vllm".into(),
                url: "http://node-a:8000/v1".into(),
                node_id: Some("node-a".into()),
                pool: "local-llm".into(),
                workloads: crate::config::WorkloadKind::default_proxy_workloads(),
                health_path: None,
            },
            3,
            Duration::from_secs(30),
        );
        let app = router_with_backends(vec![backend]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(text.contains(
            "fairlead_circuit_state{backend=\"node-a-vllm\",url=\"http://node-a:8000/v1\",node=\"node-a\",pool=\"local-llm\"} 0"
        ));
    }

    #[tokio::test]
    async fn metrics_empty_when_no_backends() {
        let app = router_with_backends(vec![]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let text = body_text(resp).await;
        assert!(text.contains("# HELP fairlead_circuit_state"));
        assert!(!text.contains("fairlead_circuit_state{"));
    }

    #[tokio::test]
    async fn metrics_reports_resource_snapshots() {
        let resources = crate::resources::ResourceRegistry::default();
        resources
            .report(crate::resources::ResourceReportRequest {
                node_id: "node-a".into(),
                backend_id: Some("node-a-vllm".into()),
                total_vram_mb: 64_000,
                reserved_vram_mb: 16_000,
                current_load: Some(0.25),
            })
            .await
            .unwrap();
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![],
            affinity: crate::router::SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            resources,
        };
        let app = router_with_state(state);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(text.contains(
            "fairlead_resource_vram_total_mb{node=\"node-a\",backend=\"node-a-vllm\"} 64000"
        ));
        assert!(text.contains(
            "fairlead_resource_vram_reserved_mb{node=\"node-a\",backend=\"node-a-vllm\"} 16000"
        ));
        assert!(text.contains(
            "fairlead_resource_vram_available_mb{node=\"node-a\",backend=\"node-a-vllm\"} 48000"
        ));
        assert!(text
            .contains("fairlead_resource_load{node=\"node-a\",backend=\"node-a-vllm\"} 0.250000"));
        assert!(text
            .contains("fairlead_resource_report_stale{node=\"node-a\",backend=\"node-a-vllm\"} 0"));
    }
}
