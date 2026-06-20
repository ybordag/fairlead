use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::config::{BackendConfig, WorkloadKind};

use super::circuit::CircuitBreaker;

/// Shared state for one configured backend URL.
/// Cloning produces a new handle to the same circuit breaker (Arc, not a deep copy).
#[derive(Clone)]
pub struct BackendState {
    /// Stable identifier used in metrics and future routing policy.
    pub id: String,
    /// Base URL including the API prefix, e.g. `http://node-a:8000/v1`.
    pub url: String,
    /// Optional node identity for locality-aware routing.
    pub node_id: Option<String>,
    /// Backend pool name. Phase 4 routes everything through the default pool.
    pub pool: String,
    /// Workloads this backend can serve.
    pub workloads: Vec<WorkloadKind>,
    /// URL used by background health probes.
    pub health_url: String,
    /// Circuit breaker shared between the request path and the health-probe task.
    pub circuit: Arc<RwLock<CircuitBreaker>>,
}

impl BackendState {
    pub fn new(url: String, failure_threshold: u32, cooldown: Duration) -> Self {
        Self::from_config(
            BackendConfig::from_legacy_url(0, url),
            failure_threshold,
            cooldown,
        )
    }

    pub fn from_config(config: BackendConfig, failure_threshold: u32, cooldown: Duration) -> Self {
        let health_url = derive_health_url(&config.url, config.health_path.as_deref());
        Self {
            id: config.id,
            url: config.url,
            node_id: config.node_id,
            pool: config.pool,
            workloads: config.workloads,
            health_url,
            circuit: Arc::new(RwLock::new(CircuitBreaker::new(
                failure_threshold,
                cooldown,
            ))),
        }
    }
}

fn derive_health_url(base_url: &str, health_path: Option<&str>) -> String {
    match health_path.map(str::trim) {
        Some(path) if path.starts_with("http://") || path.starts_with("https://") => {
            path.to_string()
        }
        Some(path) if path.starts_with('/') => {
            join_origin_path(base_url, path).unwrap_or_else(|| {
                format!(
                    "{}/{}",
                    base_url.trim_end_matches('/'),
                    path.trim_start_matches('/')
                )
            })
        }
        Some(path) => append_url_path(base_url, path),
        None => append_url_path(base_url, "models"),
    }
}

fn append_url_path(base_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn join_origin_path(base_url: &str, path: &str) -> Option<String> {
    let mut url = reqwest::Url::parse(base_url).ok()?;
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::super::circuit::CircuitState;
    use super::*;
    use axum::{http::StatusCode, routing::get, Router};

    /// BackendState::clone must produce a new Arc handle pointing at the same
    /// RwLock, not a deep copy.  Axum clones AppState on every request, so all
    /// handler copies must share one circuit breaker — otherwise failures recorded
    /// in one handler never affect any other.
    #[tokio::test]
    async fn clone_shares_circuit_not_copies() {
        let original =
            BackendState::new("http://node-a:8000/v1".into(), 1, Duration::from_secs(30));
        let cloned = original.clone();

        original.circuit.write().await.record_failure();

        let guard = cloned.circuit.read().await;
        assert!(
            matches!(guard.state(), CircuitState::Open { .. }),
            "cloned BackendState must share the Arc — circuit state not reflected in clone"
        );
    }

    #[test]
    fn from_config_preserves_node_aware_metadata() {
        let backend = BackendState::from_config(
            BackendConfig {
                id: "node-a-vllm".into(),
                url: "http://node-a:8000/v1".into(),
                node_id: Some("node-a".into()),
                pool: "local-llm".into(),
                workloads: vec![WorkloadKind::ChatCompletions],
                health_path: None,
            },
            3,
            Duration::from_secs(30),
        );

        assert_eq!(backend.id, "node-a-vllm");
        assert_eq!(backend.url, "http://node-a:8000/v1");
        assert_eq!(backend.node_id.as_deref(), Some("node-a"));
        assert_eq!(backend.pool, "local-llm");
        assert_eq!(backend.workloads, vec![WorkloadKind::ChatCompletions]);
        assert_eq!(backend.health_url, "http://node-a:8000/v1/models");
    }

    #[test]
    fn legacy_constructor_uses_default_metadata() {
        let backend = BackendState::new("http://node-a:8000/v1".into(), 3, Duration::from_secs(30));

        assert_eq!(backend.id, "backend-0");
        assert_eq!(backend.node_id, None);
        assert_eq!(backend.pool, crate::config::DEFAULT_BACKEND_POOL);
        assert_eq!(backend.workloads, WorkloadKind::default_proxy_workloads());
        assert_eq!(backend.health_url, "http://node-a:8000/v1/models");
    }

    #[test]
    fn configured_absolute_health_path_uses_backend_origin() {
        let backend = BackendState::from_config(
            BackendConfig {
                id: "node-a-vllm".into(),
                url: "http://node-a:8000/v1".into(),
                node_id: Some("node-a".into()),
                pool: "local-llm".into(),
                workloads: WorkloadKind::default_proxy_workloads(),
                health_path: Some("/health".into()),
            },
            3,
            Duration::from_secs(30),
        );

        assert_eq!(backend.health_url, "http://node-a:8000/health");
    }

    #[test]
    fn configured_relative_health_path_uses_backend_base_url() {
        let backend = BackendState::from_config(
            BackendConfig {
                id: "node-a-vllm".into(),
                url: "http://node-a:8000/v1".into(),
                node_id: Some("node-a".into()),
                pool: "local-llm".into(),
                workloads: WorkloadKind::default_proxy_workloads(),
                health_path: Some("ready".into()),
            },
            3,
            Duration::from_secs(30),
        );

        assert_eq!(backend.health_url, "http://node-a:8000/v1/ready");
    }

    #[test]
    fn configured_health_url_can_be_absolute() {
        let backend = BackendState::from_config(
            BackendConfig {
                id: "node-a-vllm".into(),
                url: "http://node-a:8000/v1".into(),
                node_id: Some("node-a".into()),
                pool: "local-llm".into(),
                workloads: WorkloadKind::default_proxy_workloads(),
                health_path: Some("http://node-a:9000/ready".into()),
            },
            3,
            Duration::from_secs(30),
        );

        assert_eq!(backend.health_url, "http://node-a:9000/ready");
    }

    async fn start_probe_target(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{}/v1", addr)
    }

    /// A successful probe closes the circuit. Importantly it does so directly —
    /// bypassing HalfOpen — because the probe is an explicit health check, not a
    /// speculative user request. An Open circuit with a long cooldown (so
    /// is_available() would never naturally transition it) is closed as soon as
    /// one successful probe fires.
    #[tokio::test]
    async fn probe_closes_open_circuit_on_success() {
        let mock = Router::new().route("/v1/models", get(|| async { StatusCode::OK }));
        let url = start_probe_target(mock).await;

        // Long cooldown: is_available() alone would never transition this circuit.
        let circuit = Arc::new(RwLock::new(CircuitBreaker::new(
            1,
            Duration::from_secs(999),
        )));
        circuit.write().await.record_failure();
        assert!(
            matches!(circuit.read().await.state(), CircuitState::Open { .. }),
            "circuit must start Open for this test to be meaningful"
        );

        spawn_health_probe(
            circuit.clone(),
            append_url_path(&url, "models"),
            reqwest::Client::new(),
            Duration::from_millis(25),
        );

        // Give the probe several opportunities to fire.
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert!(
            matches!(circuit.read().await.state(), CircuitState::Closed),
            "probe should close the circuit directly — not via HalfOpen"
        );
    }

    /// Consecutive probe failures open the circuit, matching the configured
    /// failure threshold.
    #[tokio::test]
    async fn probe_opens_circuit_when_backend_unreachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // nothing listening on this port

        let url = format!("http://{}/v1", addr);

        // threshold=2: two probe failures should open the circuit.
        let circuit = Arc::new(RwLock::new(CircuitBreaker::new(2, Duration::from_secs(30))));
        assert!(
            circuit.write().await.is_available(),
            "circuit must start Closed"
        );

        spawn_health_probe(
            circuit.clone(),
            url,
            reqwest::Client::new(),
            Duration::from_millis(25),
        );

        // First tick fires immediately; second at ~25ms. 150ms gives 6+ opportunities.
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert!(
            !circuit.write().await.is_available(),
            "circuit should be open after repeated probe failures"
        );
    }

    /// A healthy backend keeps a Closed circuit closed — successive successful
    /// probes must not inadvertently change state.
    #[tokio::test]
    async fn probe_keeps_healthy_circuit_closed() {
        let mock = Router::new().route("/v1/models", get(|| async { StatusCode::OK }));
        let url = start_probe_target(mock).await;

        let circuit = Arc::new(RwLock::new(CircuitBreaker::new(1, Duration::from_secs(30))));

        spawn_health_probe(
            circuit.clone(),
            append_url_path(&url, "models"),
            reqwest::Client::new(),
            Duration::from_millis(25),
        );

        tokio::time::sleep(Duration::from_millis(150)).await;

        assert!(
            matches!(circuit.read().await.state(), CircuitState::Closed),
            "repeated successful probes must leave a Closed circuit Closed"
        );
    }
}

/// Spawn a background task that probes `url` every `interval` and records the
/// result in `circuit`. Any HTTP response (even 4xx) counts as alive; only
/// connection errors count as failures.
pub fn spawn_health_probe(
    circuit: Arc<RwLock<CircuitBreaker>>,
    url: String,
    client: reqwest::Client,
    interval: Duration,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let ok = client
                .get(&url)
                .timeout(Duration::from_secs(5))
                .send()
                .await
                .is_ok();
            let mut cb = circuit.write().await;
            if ok {
                cb.record_success();
            } else {
                cb.record_failure();
            }
        }
    });
}
