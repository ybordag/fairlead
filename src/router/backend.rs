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
