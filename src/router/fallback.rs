use crate::router::backend::BackendState;

/// Select the best available backend index, with optional soft affinity.
///
/// If `preferred` is `Some(idx)` and that backend's circuit allows a request,
/// it is returned immediately. Otherwise the list is walked in declared order
/// (skipping the already-checked preferred index) and the first available
/// backend is returned. Returns `None` if every circuit is open.
pub async fn select_backend(backends: &[BackendState], preferred: Option<usize>) -> Option<usize> {
    // Try the preferred backend first.
    if let Some(idx) = preferred {
        if let Some(backend) = backends.get(idx) {
            if backend.circuit.write().await.is_available() {
                return Some(idx);
            }
        }
    }

    // Walk in declared priority order, skipping the already-checked preferred.
    for (i, backend) in backends.iter().enumerate() {
        if Some(i) == preferred {
            continue;
        }
        if backend.circuit.write().await.is_available() {
            return Some(i);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn healthy(url: &str) -> BackendState {
        BackendState::new(url.to_string(), 1, Duration::from_secs(60))
    }

    async fn tripped(url: &str) -> BackendState {
        let b = healthy(url);
        b.circuit.write().await.record_failure();
        b
    }

    #[tokio::test]
    async fn empty_backends_returns_none() {
        assert_eq!(select_backend(&[], None).await, None);
    }

    #[tokio::test]
    async fn all_circuits_open_returns_none() {
        let backends = vec![
            tripped("http://a:8000/v1").await,
            tripped("http://b:8000/v1").await,
        ];
        assert_eq!(select_backend(&backends, None).await, None);
    }

    #[tokio::test]
    async fn returns_first_healthy_backend() {
        let backends = vec![
            healthy("http://a:8000/v1"),
            healthy("http://b:8000/v1"),
        ];
        assert_eq!(select_backend(&backends, None).await, Some(0));
    }

    #[tokio::test]
    async fn skips_open_circuits_in_chain() {
        let backends = vec![
            tripped("http://a:8000/v1").await,
            tripped("http://b:8000/v1").await,
            healthy("http://c:8000/v1"),
        ];
        assert_eq!(select_backend(&backends, None).await, Some(2));
    }

    #[tokio::test]
    async fn preferred_used_when_available() {
        let backends = vec![
            healthy("http://a:8000/v1"),
            healthy("http://b:8000/v1"),
        ];
        assert_eq!(select_backend(&backends, Some(1)).await, Some(1));
    }

    #[tokio::test]
    async fn falls_back_when_preferred_circuit_open() {
        let backends = vec![
            healthy("http://a:8000/v1"),
            tripped("http://b:8000/v1").await,
        ];
        // Prefer index 1 (open) → falls back to index 0.
        assert_eq!(select_backend(&backends, Some(1)).await, Some(0));
    }

    #[tokio::test]
    async fn preferred_out_of_bounds_uses_chain() {
        let backends = vec![healthy("http://a:8000/v1")];
        // preferred=99 doesn't exist → chain picks index 0.
        assert_eq!(select_backend(&backends, Some(99)).await, Some(0));
    }
}
