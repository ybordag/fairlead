mod config;
mod error;
mod health;
mod proxy;

use axum::{routing::{get, post}, Router};
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Shared state injected into every handler via Axum's `State` extractor.
/// All fields must be `Clone` — Axum clones state once per request.
#[derive(Clone)]
pub struct AppState {
    /// Reusable HTTP client for forwarding requests to backends.
    pub client: reqwest::Client,
    /// Ordered list of backend base URLs (e.g. "http://loki:8000/v1").
    /// Phase 2: first entry is always used. Phase 3+ adds circuit-breaker selection.
    pub backends: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = config::Config::from_env()?;

    init_tracing(&cfg);

    let state = AppState {
        client: reqwest::Client::new(),
        backends: cfg.backends.clone(),
    };

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
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .init();
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
