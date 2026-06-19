use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use super::circuit::CircuitBreaker;

/// Shared state for one configured backend URL.
/// Cloning produces a new handle to the same circuit breaker (Arc, not a deep copy).
#[derive(Clone)]
pub struct BackendState {
    /// Base URL including the API prefix, e.g. `http://loki:8000/v1`.
    pub url: String,
    /// Circuit breaker shared between the request path and the health-probe task.
    pub circuit: Arc<RwLock<CircuitBreaker>>,
}

impl BackendState {
    pub fn new(url: String, failure_threshold: u32, cooldown: Duration) -> Self {
        Self {
            url,
            circuit: Arc::new(RwLock::new(CircuitBreaker::new(
                failure_threshold,
                cooldown,
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::circuit::CircuitState;
    use axum::{http::StatusCode, routing::get, Router};

    /// BackendState::clone must produce a new Arc handle pointing at the same
    /// RwLock, not a deep copy.  Axum clones AppState on every request, so all
    /// handler copies must share one circuit breaker — otherwise failures recorded
    /// in one handler never affect any other.
    #[tokio::test]
    async fn clone_shares_circuit_not_copies() {
        let original =
            BackendState::new("http://loki:8000/v1".into(), 1, Duration::from_secs(30));
        let cloned = original.clone();

        original.circuit.write().await.record_failure();

        let guard = cloned.circuit.read().await;
        assert!(
            matches!(guard.state(), CircuitState::Open { .. }),
            "cloned BackendState must share the Arc — circuit state not reflected in clone"
        );
    }

    async fn start_probe_target(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
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
        let mock = Router::new().route("/v1", get(|| async { StatusCode::OK }));
        let url = start_probe_target(mock).await;

        // Long cooldown: is_available() alone would never transition this circuit.
        let circuit = Arc::new(RwLock::new(CircuitBreaker::new(1, Duration::from_secs(999))));
        circuit.write().await.record_failure();
        assert!(
            matches!(circuit.read().await.state(), CircuitState::Open { .. }),
            "circuit must start Open for this test to be meaningful"
        );

        spawn_health_probe(circuit.clone(), url, reqwest::Client::new(), Duration::from_millis(25));

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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // nothing listening on this port

        let url = format!("http://{}/v1", addr);

        // threshold=2: two probe failures should open the circuit.
        let circuit = Arc::new(RwLock::new(CircuitBreaker::new(2, Duration::from_secs(30))));
        assert!(
            circuit.write().await.is_available(),
            "circuit must start Closed"
        );

        spawn_health_probe(circuit.clone(), url, reqwest::Client::new(), Duration::from_millis(25));

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
        let mock = Router::new().route("/v1", get(|| async { StatusCode::OK }));
        let url = start_probe_target(mock).await;

        let circuit = Arc::new(RwLock::new(CircuitBreaker::new(1, Duration::from_secs(30))));

        spawn_health_probe(circuit.clone(), url, reqwest::Client::new(), Duration::from_millis(25));

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
