mod config;
mod error;
mod health;
mod metrics;
mod proxy;
mod router;

use axum::{
    routing::{get, post},
    Router,
};
use std::{net::SocketAddr, time::Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;

use router::{BackendState, spawn_health_probe};

/// Shared state cloned into every handler by Axum's `State` extractor.
/// Cloning is shallow: `BackendState` clones its `Arc`, so all handler
/// copies share the same circuit-breaker state.
#[derive(Clone)]
pub struct AppState {
    /// Reusable HTTP client for forwarding requests upstream.
    pub client: reqwest::Client,
    /// Ordered list of configured backends with their circuit breakers.
    /// Phase 2: always picks `backends[0]`.
    /// Phase 4: circuit-aware fallback chain.
    pub backends: Vec<BackendState>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = config::Config::from_env()?;

    init_tracing(&cfg);

    let client = reqwest::Client::new();

    let backends: Vec<BackendState> = cfg
        .backends
        .iter()
        .map(|url| {
            BackendState::new(
                url.clone(),
                cfg.circuit_failure_threshold,
                Duration::from_secs(cfg.circuit_cooldown_secs),
            )
        })
        .collect();

    // Spawn a background health-probe task for each backend.
    for b in &backends {
        spawn_health_probe(
            b.circuit.clone(),
            b.url.clone(),
            client.clone(),
            Duration::from_secs(cfg.health_probe_interval_secs),
        );
    }

    let state = AppState { client, backends };
    let app = build_router(state);

    let addr: SocketAddr = format!("0.0.0.0:{}", cfg.port).parse()?;
    info!(port = cfg.port, "fairlead starting");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

pub(crate) fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health::health))
        .route("/metrics", get(metrics::metrics))
        .route("/v1/chat/completions", post(proxy::chat_completions))
        .route("/v1/embeddings", post(proxy::embeddings))
        .with_state(state)
}

fn init_tracing(cfg: &config::Config) {
    let filter = EnvFilter::new(&cfg.log_level);
    if cfg.log_format_json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn server_binds_and_serves_health() {
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![],
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, build_router(state)).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/health"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let json: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(json["status"], "ok");
    }
}
