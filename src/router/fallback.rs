use crate::router::backend::BackendState;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResourceRank {
    pub current_load: Option<f64>,
    pub available_vram_mb: u64,
}

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
#[cfg(test)]
pub async fn select_backend_excluding(
    backends: &[BackendState],
    preferred: Option<usize>,
    origin_node: Option<&str>,
    excluded: &[usize],
) -> Option<usize> {
    select_backend_excluding_resource(backends, preferred, origin_node, excluded, &[], &[]).await
}

/// Select the best available backend index while skipping both already
/// attempted backends and candidates that are ineligible under resource policy.
pub async fn select_backend_excluding_resource(
    backends: &[BackendState],
    preferred: Option<usize>,
    origin_node: Option<&str>,
    excluded: &[usize],
    resource_ineligible: &[usize],
    resource_ranks: &[Option<ResourceRank>],
) -> Option<usize> {
    let mut checked = Vec::with_capacity(2);

    // Prefer same-node backends first. If multiple origin-local candidates are
    // available, resource rank chooses the least-loaded / highest-headroom one.
    if let Some(origin) = origin_node {
        let candidates: Vec<_> = backends
            .iter()
            .enumerate()
            .filter_map(|(i, backend)| {
                (!excluded.contains(&i)
                    && !resource_ineligible.contains(&i)
                    && backend.node_id.as_deref() == Some(origin))
                .then_some(i)
            })
            .collect();
        checked.extend(candidates.iter().copied());
        if let Some(idx) = best_available_backend(backends, &candidates, resource_ranks).await {
            return Some(idx);
        }
    }

    // Try the preferred backend first.
    if let Some(idx) = preferred {
        if !checked.contains(&idx)
            && !excluded.contains(&idx)
            && !resource_ineligible.contains(&idx)
        {
            if let Some(backend) = backends.get(idx) {
                checked.push(idx);
                if backend.circuit.write().await.is_available() {
                    return Some(idx);
                }
            }
        }
    }

    // Walk remaining backends in resource-rank order, using configured order as
    // the deterministic tie-breaker.
    let candidates: Vec<_> = backends
        .iter()
        .enumerate()
        .filter_map(|(i, _)| {
            (!checked.contains(&i) && !excluded.contains(&i) && !resource_ineligible.contains(&i))
                .then_some(i)
        })
        .collect();
    best_available_backend(backends, &candidates, resource_ranks).await
}

async fn best_available_backend(
    backends: &[BackendState],
    candidates: &[usize],
    resource_ranks: &[Option<ResourceRank>],
) -> Option<usize> {
    let mut available = Vec::new();
    for idx in candidates {
        let Some(backend) = backends.get(*idx) else {
            continue;
        };
        if backend.circuit.write().await.is_available() {
            available.push(*idx);
        }
    }

    available
        .into_iter()
        .min_by(|a, b| compare_resource_rank(*a, *b, resource_ranks))
}

fn compare_resource_rank(
    a: usize,
    b: usize,
    resource_ranks: &[Option<ResourceRank>],
) -> std::cmp::Ordering {
    let a_rank = resource_ranks.get(a).and_then(|rank| *rank);
    let b_rank = resource_ranks.get(b).and_then(|rank| *rank);

    match (a_rank, b_rank) {
        (Some(a_rank), Some(b_rank)) => compare_rank_values(a_rank, b_rank).then_with(|| a.cmp(&b)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.cmp(&b),
    }
}

fn compare_rank_values(a: ResourceRank, b: ResourceRank) -> std::cmp::Ordering {
    compare_optional_load(a.current_load, b.current_load)
        .then_with(|| b.available_vram_mb.cmp(&a.available_vram_mb))
}

fn compare_optional_load(a: Option<f64>, b: Option<f64>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
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

    #[tokio::test]
    async fn resource_ineligible_origin_backend_uses_peer() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];

        assert_eq!(
            select_backend_excluding_resource(&backends, None, Some("node-a"), &[], &[0], &[])
                .await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn all_resource_ineligible_returns_none() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];

        assert_eq!(
            select_backend_excluding_resource(&backends, None, Some("node-a"), &[], &[0, 1], &[])
                .await,
            None
        );
    }

    #[tokio::test]
    async fn lower_load_backend_wins_among_remaining_candidates() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];
        let ranks = vec![
            Some(ResourceRank {
                current_load: Some(0.8),
                available_vram_mb: 60_000,
            }),
            Some(ResourceRank {
                current_load: Some(0.2),
                available_vram_mb: 20_000,
            }),
        ];

        assert_eq!(
            select_backend_excluding_resource(&backends, None, None, &[], &[], &ranks).await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn higher_headroom_breaks_equal_load_tie() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];
        let ranks = vec![
            Some(ResourceRank {
                current_load: Some(0.2),
                available_vram_mb: 20_000,
            }),
            Some(ResourceRank {
                current_load: Some(0.2),
                available_vram_mb: 60_000,
            }),
        ];

        assert_eq!(
            select_backend_excluding_resource(&backends, None, None, &[], &[], &ranks).await,
            Some(1)
        );
    }

    #[tokio::test]
    async fn origin_node_precedence_over_load_ranking() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];
        let ranks = vec![
            Some(ResourceRank {
                current_load: Some(0.8),
                available_vram_mb: 20_000,
            }),
            Some(ResourceRank {
                current_load: Some(0.1),
                available_vram_mb: 60_000,
            }),
        ];

        assert_eq!(
            select_backend_excluding_resource(&backends, None, Some("node-a"), &[], &[], &ranks)
                .await,
            Some(0)
        );
    }

    #[tokio::test]
    async fn affinity_precedence_over_load_ranking() {
        let backends = vec![
            healthy_on_node("http://node-a:8000/v1", "node-a"),
            healthy_on_node("http://node-b:8000/v1", "node-b"),
        ];
        let ranks = vec![
            Some(ResourceRank {
                current_load: Some(0.1),
                available_vram_mb: 60_000,
            }),
            Some(ResourceRank {
                current_load: Some(0.8),
                available_vram_mb: 20_000,
            }),
        ];

        assert_eq!(
            select_backend_excluding_resource(&backends, Some(1), None, &[], &[], &ranks).await,
            Some(1)
        );
    }
}
