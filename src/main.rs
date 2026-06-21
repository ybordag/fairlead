mod callbacks;
mod config;
mod error;
mod health;
mod jobs;
mod metrics;
mod models;
mod priority;
mod proxy;
mod resources;
mod router;
mod scheduler;
mod storage;
mod workers;

use axum::{
    routing::{get, post},
    Router,
};
use std::{net::SocketAddr, time::Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;

use metrics::RoutingMetrics;
use priority::PriorityLimiter;
use resources::{ResourceRegistry, ResourceRoutingPolicy};
use router::{spawn_health_probe, BackendState, SessionAffinity};

/// Shared state cloned into every handler by Axum's `State` extractor.
/// Cloning is shallow — `BackendState` and `SessionAffinity` both wrap
/// `Arc`, so all handler copies share the same circuit breakers and
/// affinity map.
#[derive(Clone)]
pub struct AppState {
    /// Reusable HTTP client for forwarding requests upstream.
    pub client: reqwest::Client,
    /// Ordered list of configured backends with their circuit breakers.
    pub backends: Vec<BackendState>,
    /// Thread-ID → backend-index affinity map.
    pub affinity: SessionAffinity,
    /// In-process routing metrics rendered by `/metrics`.
    pub metrics: RoutingMetrics,
    /// Callback delivery retry and timeout settings.
    pub callback_policy: callbacks::CallbackPolicy,
    /// Cooperative resource reports from model servers and compute workers.
    pub resources: ResourceRegistry,
    /// Policy for using resource reports during backend selection.
    pub resource_policy: ResourceRoutingPolicy,
    /// Per-priority synchronous admission limits.
    pub priority_limiter: PriorityLimiter,
    /// Async job and lease state, optionally backed by SQLite.
    pub jobs: jobs::JobRegistry,
    /// In-memory worker registry for async worker-pull jobs.
    pub workers: workers::WorkerRegistry,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = config::Config::from_env()?;

    init_tracing(&cfg);
    let jobs = match &cfg.job_store {
        config::JobStoreConfig::Memory => jobs::JobRegistry::default(),
        config::JobStoreConfig::Sqlite { path } => {
            jobs::JobRegistry::with_store(storage::SqliteJobStore::open(path)?)?
        }
    };

    let client = reqwest::Client::new();

    let backends: Vec<BackendState> = cfg
        .backends
        .iter()
        .map(|backend| {
            BackendState::from_config(
                backend.clone(),
                cfg.circuit_failure_threshold,
                Duration::from_secs(cfg.circuit_cooldown_secs),
            )
        })
        .collect();

    // Spawn a background health-probe task for each backend.
    for b in &backends {
        spawn_health_probe(
            b.circuit.clone(),
            b.health_url.clone(),
            client.clone(),
            Duration::from_secs(cfg.health_probe_interval_secs),
        );
    }

    let state = AppState {
        client,
        backends,
        affinity: SessionAffinity::default(),
        metrics: RoutingMetrics::default(),
        callback_policy: callbacks::CallbackPolicy {
            max_attempts: cfg.callback_max_attempts,
            timeout: Duration::from_secs(cfg.callback_timeout_secs),
            retry_delay: Duration::from_millis(cfg.callback_retry_delay_ms),
        },
        resources: ResourceRegistry::new(Duration::from_secs(cfg.resource_report_ttl_secs)),
        resource_policy: ResourceRoutingPolicy {
            enabled: cfg.resource_aware_routing,
            chat_completions_required_vram_mb: cfg.chat_completions_required_vram_mb,
            embeddings_required_vram_mb: cfg.embeddings_required_vram_mb,
        },
        priority_limiter: PriorityLimiter::new(
            cfg.priority_realtime_limit,
            cfg.priority_batch_limit,
            cfg.priority_background_limit,
        ),
        jobs,
        workers: workers::WorkerRegistry::default(),
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
        .route("/metrics", get(metrics::metrics))
        .route("/v1/resources", get(resources::list_resources))
        .route("/v1/resources/report", post(resources::report_resources))
        .route("/v1/models", get(models::list_models))
        .route("/v1/jobs", get(jobs::list_jobs).post(jobs::submit_job))
        .route("/v1/jobs/:id", get(jobs::get_job).delete(jobs::cancel_job))
        .route(
            "/v1/scheduler/preview",
            get(scheduler::preview_next_assignment_handler),
        )
        .route("/v1/workers", get(workers::list_workers))
        .route("/v1/workers/register", post(workers::register_worker))
        .route(
            "/v1/workers/:id/claim",
            post(scheduler::claim_worker_job_handler),
        )
        .route(
            "/v1/workers/:worker_id/jobs/:job_id/renew",
            post(scheduler::renew_worker_job_lease_handler),
        )
        .route(
            "/v1/workers/:worker_id/jobs/:job_id/complete",
            post(scheduler::complete_worker_job_handler),
        )
        .route(
            "/v1/workers/:worker_id/jobs/:job_id/fail",
            post(scheduler::fail_worker_job_handler),
        )
        .route("/v1/workers/:id/heartbeat", post(workers::heartbeat_worker))
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
            affinity: SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            callback_policy: callbacks::CallbackPolicy::default(),
            resources: ResourceRegistry::default(),
            resource_policy: ResourceRoutingPolicy::default(),
            priority_limiter: PriorityLimiter::default(),
            jobs: jobs::JobRegistry::default(),
            workers: workers::WorkerRegistry::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, build_router(state)).await.unwrap();
        });

        let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
        assert_eq!(resp.status(), 200);

        let json: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(json["status"], "ok");
    }
}
