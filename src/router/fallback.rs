use crate::router::backend::BackendState;

/// Select the best available backend index, with optional origin-node locality
/// and soft affinity.
///
/// If `origin_node` is present, the first available backend on that node is
/// returned. If no origin-local backend is available, `preferred` session
/// affinity is tried. Otherwise the list is walked in declared order, skipping
/// already-checked indexes. Returns `None` if every circuit is open.
#[cfg(test)]
async fn select_backend(
    backends: &[BackendState],
    preferred: Option<usize>,
    origin_node: Option<&str>,
) -> Option<usize> {
    select_backend_excluding(backends, preferred, origin_node, &[]).await
}

/// Select the best available backend index while skipping backends already
/// attempted for the current request.
pub async fn select_backend_excluding(
    backends: &[BackendState],
    preferred: Option<usize>,
    origin_node: Option<&str>,
    excluded: &[usize],
) -> Option<usize> {
    let mut checked = Vec::with_capacity(2);

    // Prefer same-node backends first. This is the first Bluewater locality
    // rule; resource eligibility will be layered in later.
    if let Some(origin) = origin_node {
        for (i, backend) in backends.iter().enumerate() {
            if excluded.contains(&i) || backend.node_id.as_deref() != Some(origin) {
                continue;
            }
            checked.push(i);
            if backend.circuit.write().await.is_available() {
                return Some(i);
            }
        }
    }

    // Try the preferred backend first.
    if let Some(idx) = preferred {
        if !checked.contains(&idx) && !excluded.contains(&idx) {
            if let Some(backend) = backends.get(idx) {
                checked.push(idx);
                if backend.circuit.write().await.is_available() {
                    return Some(idx);
                }
            }
        }
    }

    // Walk in declared priority order, skipping already-checked candidates.
    for (i, backend) in backends.iter().enumerate() {
        if checked.contains(&i) || excluded.contains(&i) {
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
    use crate::config::{BackendConfig, WorkloadKind};
    use std::time::Duration;

    fn healthy(url: &str) -> BackendState {
        BackendState::new(url.to_string(), 1, Duration::from_secs(60))
    }

    fn healthy_on_node(url: &str, node_id: &str) -> BackendState {
        BackendState::from_config(
            BackendConfig {
                id: format!("{node_id}-vllm"),
                url: url.to_string(),
                node_id: Some(node_id.to_string()),
                pool: "local-llm".into(),
                workloads: WorkloadKind::default_proxy_workloads(),
                health_path: None,
            },
            1,
            Duration::from_secs(60),
        )
    }

    async fn tripped(url: &str) -> BackendState {
        let b = healthy(url);
        b.circuit.write().await.record_failure();
        b
    }

    #[tokio::test]
    async fn empty_backends_returns_none() {
        assert_eq!(select_backend(&[], None, None).await, None);
    }

    #[tokio::test]
    async fn all_circuits_open_returns_none() {
        let backends = vec![
            tripped("http://a:8000/v1").await,
            tripped("http://b:8000/v1").await,
        ];
        assert_eq!(select_backend(&backends, None, None).await, None);
    }

    #[tokio::test]
    async fn returns_first_healthy_backend() {
        let backends = vec![healthy("http://a:8000/v1"), healthy("http://b:8000/v1")];
        assert_eq!(select_backend(&backends, None, None).await, Some(0));
    }

    #[tokio::test]
    async fn skips_open_circuits_in_chain() {
        let backends = vec![
            tripped("http://a:8000/v1").await,
            tripped("http://b:8000/v1").await,
            healthy("http://c:8000/v1"),
        ];
        assert_eq!(select_backend(&backends, None, None).await, Some(2));
    }

    #[tokio::test]
    async fn preferred_used_when_available() {
        let backends = vec![healthy("http://a:8000/v1"), healthy("http://b:8000/v1")];
        assert_eq!(select_backend(&backends, Some(1), None).await, Some(1));
    }

    #[tokio::test]
    async fn falls_back_when_preferred_circuit_open() {
        let backends = vec![
            healthy("http://a:8000/v1"),
            tripped("http://b:8000/v1").await,
        ];
        // Prefer index 1 (open) → falls back to index 0.
        assert_eq!(select_backend(&backends, Some(1), None).await, Some(0));
    }

    #[tokio::test]
    async fn preferred_at_index_zero_falls_back_when_open() {
        let backends = vec![
            tripped("http://a:8000/v1").await,
            healthy("http://b:8000/v1"),
        ];
        assert_eq!(select_backend(&backends, Some(0), None).await, Some(1));
    }

    #[tokio::test]
    async fn preferred_out_of_bounds_uses_chain() {
        let backends = vec![healthy("http://a:8000/v1")];
        // preferred=99 doesn't exist → chain picks index 0.
        assert_eq!(select_backend(&backends, Some(99), None).await, Some(0));
    }

    #[tokio::test]
    async fn origin_node_preferred_when_available() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];

        assert_eq!(
            select_backend(&backends, None, Some("node-b")).await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn origin_node_precedence_over_affinity() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];

        assert_eq!(
            select_backend(&backends, Some(1), Some("node-a")).await,
            Some(0)
        );
    }

    #[tokio::test]
    async fn origin_node_falls_back_when_local_circuit_open() {
        let local = healthy_on_node("http://node-a:8000/v1", "node-a");
        local.circuit.write().await.record_failure();
        let backends = vec![local, healthy_on_node("http://node-b:8000/v1", "node-b")];

        assert_eq!(
            select_backend(&backends, None, Some("node-a")).await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn unknown_origin_node_uses_affinity_then_chain() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];

        assert_eq!(
            select_backend(&backends, Some(1), Some("odin")).await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn excluded_origin_backend_uses_next_candidate() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];

        assert_eq!(
            select_backend_excluding(&backends, None, Some("node-a"), &[0]).await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn excluded_preferred_backend_uses_chain() {
        let backends = vec![
            healthy("http://a:8000/v1"),
            healthy("http://b:8000/v1"),
            healthy("http://c:8000/v1"),
        ];

        assert_eq!(
            select_backend_excluding(&backends, Some(1), None, &[1]).await,
            Some(0)
        );
    }
}
