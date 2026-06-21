use axum::{extract::State, Json};
use serde::Serialize;

use crate::{config::WorkloadKind, AppState};

#[derive(Debug, Serialize)]
pub struct ModelListResponse {
    pub object: &'static str,
    pub data: Vec<ModelInfo>,
}

#[derive(Debug, Serialize)]
pub struct ModelInfo {
    pub id: String,
    pub object: &'static str,
    pub created: u64,
    pub owned_by: &'static str,
    pub backend_id: String,
    pub backend_url: String,
    pub health_url: String,
    pub node_id: Option<String>,
    pub pool: String,
    pub workloads: Vec<&'static str>,
}

pub async fn list_models(State(state): State<AppState>) -> Json<ModelListResponse> {
    Json(ModelListResponse {
        object: "list",
        data: state.backends.iter().map(model_info).collect(),
    })
}

fn model_info(backend: &crate::router::BackendState) -> ModelInfo {
    ModelInfo {
        id: backend.id.clone(),
        object: "model",
        created: 0,
        owned_by: "fairlead",
        backend_id: backend.id.clone(),
        backend_url: backend.url.clone(),
        health_url: backend.health_url.clone(),
        node_id: backend.node_id.clone(),
        pool: backend.pool.clone(),
        workloads: backend.workloads.iter().map(WorkloadKind::as_str).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        build_router,
        config::BackendConfig,
        metrics::RoutingMetrics,
        priority::PriorityLimiter,
        resources::{ResourceRegistry, ResourceRoutingPolicy},
        router::{BackendState, SessionAffinity},
        AppState,
    };
    use axum::{
        body::Body,
        http::{Method, Request},
    };
    use serde_json::json;
    use std::time::Duration;
    use tower::ServiceExt;

    fn test_state(backends: Vec<BackendState>) -> AppState {
        AppState {
            client: reqwest::Client::new(),
            backends,
            affinity: SessionAffinity::default(),
            metrics: RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: ResourceRegistry::default(),
            resource_policy: ResourceRoutingPolicy::default(),
            priority_limiter: PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        }
    }

    fn backend_config(
        id: &str,
        node_id: Option<&str>,
        pool: &str,
        workloads: Vec<WorkloadKind>,
    ) -> BackendConfig {
        BackendConfig {
            id: id.into(),
            url: format!("http://{id}:8000/v1"),
            node_id: node_id.map(str::to_owned),
            pool: pool.into(),
            workloads,
            health_path: None,
        }
    }

    #[tokio::test]
    async fn list_models_returns_configured_backend_metadata() {
        let backend = BackendState::from_config(
            BackendConfig {
                id: "node-a-vllm".into(),
                url: "http://node-a:8000/v1".into(),
                node_id: Some("node-a".into()),
                pool: "local-llm".into(),
                workloads: vec![WorkloadKind::ChatCompletions, WorkloadKind::Embeddings],
                health_path: Some("/health".into()),
            },
            3,
            Duration::from_secs(30),
        );
        let app = build_router(test_state(vec![backend]));

        let response = app
            .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), 200);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value["object"], "list");
        assert_eq!(value["data"][0]["id"], "node-a-vllm");
        assert_eq!(value["data"][0]["object"], "model");
        assert_eq!(value["data"][0]["owned_by"], "fairlead");
        assert_eq!(value["data"][0]["backend_id"], "node-a-vllm");
        assert_eq!(value["data"][0]["backend_url"], "http://node-a:8000/v1");
        assert_eq!(value["data"][0]["health_url"], "http://node-a:8000/health");
        assert_eq!(value["data"][0]["node_id"], "node-a");
        assert_eq!(value["data"][0]["pool"], "local-llm");
        assert_eq!(
            value["data"][0]["workloads"],
            json!(["chat_completions", "embeddings"])
        );
    }

    #[tokio::test]
    async fn list_models_returns_empty_list_when_no_backends_are_configured() {
        let app = build_router(test_state(vec![]));

        let response = app
            .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), 200);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value["object"], "list");
        assert_eq!(value["data"], json!([]));
    }

    #[tokio::test]
    async fn list_models_preserves_backend_order_and_workload_metadata() {
        let first = BackendState::from_config(
            backend_config(
                "chat-backend",
                Some("node-a"),
                "local-llm",
                vec![WorkloadKind::ChatCompletions],
            ),
            3,
            Duration::from_secs(30),
        );
        let second = BackendState::from_config(
            backend_config(
                "embedding-backend",
                None,
                "embedding",
                vec![WorkloadKind::Embeddings],
            ),
            3,
            Duration::from_secs(30),
        );
        let app = build_router(test_state(vec![first, second]));

        let response = app
            .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), 200);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value["data"][0]["id"], "chat-backend");
        assert_eq!(value["data"][0]["node_id"], "node-a");
        assert_eq!(value["data"][0]["pool"], "local-llm");
        assert_eq!(value["data"][0]["workloads"], json!(["chat_completions"]));

        assert_eq!(value["data"][1]["id"], "embedding-backend");
        assert_eq!(value["data"][1]["node_id"], serde_json::Value::Null);
        assert_eq!(value["data"][1]["pool"], "embedding");
        assert_eq!(value["data"][1]["workloads"], json!(["embeddings"]));
    }

    #[tokio::test]
    async fn post_to_models_returns_405() {
        let app = build_router(test_state(vec![]));

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/models")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), 405);
    }
}
