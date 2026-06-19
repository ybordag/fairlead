mod config;
mod error;
mod health;

use axum::{routing::get, Router};
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = config::Config::from_env()?;

    init_tracing(&cfg);

    let app = build_router();

    let addr: SocketAddr = format!("0.0.0.0:{}", cfg.port).parse()?;
    info!(port = cfg.port, "fairlead starting");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn build_router() -> Router {
    Router::new().route("/health", get(health::health))
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

    /// Bind a real TCP listener on a random port and confirm the server accepts
    /// connections and returns a valid health response. This is the only test
    /// that exercises the full TCP path rather than the in-process oneshot harness.
    #[tokio::test]
    async fn server_binds_and_serves_health() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, build_router()).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/health"))
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(json["status"], "ok");
    }
}
