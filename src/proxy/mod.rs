pub mod types;

use axum::{
    body::Body,
    extract::State,
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use std::time::Instant;
use tracing::{info, warn};

use crate::{
    metrics::{FallbackLabels, RequestLabels, RetryLabels},
    router::{select_backend_excluding, BackendState},
    AppState,
};

pub async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(
        &state,
        "chat_completions",
        "chat/completions",
        &headers,
        body,
    )
    .await
}

pub async fn embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(&state, "embeddings", "embeddings", &headers, body).await
}

async fn forward(
    state: &AppState,
    workload: &str,
    path: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    if state.backends.is_empty() {
        record_request(
            state,
            workload,
            None,
            None,
            StatusCode::SERVICE_UNAVAILABLE,
            "no_backends",
            started,
        );
        return (StatusCode::SERVICE_UNAVAILABLE, "no backends configured").into_response();
    }

    let request_id = headers
        .get("x-request-id")
        .or_else(|| headers.get("x-fairlead-request-id"))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Extract optional thread ID for session affinity.
    let thread_id = headers
        .get("x-fairlead-thread-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    // Extract optional origin node for locality-aware routing.
    let origin_node = headers
        .get("x-fairlead-origin-node")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    // Resolve preferred backend index (if any) then run the fallback chain.
    let preferred = match thread_id {
        Some(ref tid) => state.affinity.preferred(tid).await,
        None => None,
    };

    let mut attempted = Vec::new();
    let mut next_backend = None;

    loop {
        let idx = match next_backend.take() {
            Some(idx) => idx,
            None => {
                let Some(idx) = select_backend_excluding(
                    &state.backends,
                    preferred,
                    origin_node.as_deref(),
                    &attempted,
                )
                .await
                else {
                    if attempted.is_empty() {
                        record_request(
                            state,
                            workload,
                            None,
                            origin_node.as_deref(),
                            StatusCode::SERVICE_UNAVAILABLE,
                            "unavailable",
                            started,
                        );
                        info!(
                            request_id,
                            workload,
                            origin_node = origin_node.as_deref().unwrap_or(""),
                            affinity_key = thread_id.as_deref().unwrap_or(""),
                            selected_backend = "",
                            retry_count = attempted.len(),
                            fallback_reason = "",
                            status = StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                            outcome = "unavailable",
                            "request completed"
                        );
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "all backends unavailable (circuits open)",
                        )
                            .into_response();
                    }
                    record_request(
                        state,
                        workload,
                        None,
                        origin_node.as_deref(),
                        StatusCode::BAD_GATEWAY,
                        "upstream_error",
                        started,
                    );
                    info!(
                        request_id,
                        workload,
                        origin_node = origin_node.as_deref().unwrap_or(""),
                        affinity_key = thread_id.as_deref().unwrap_or(""),
                        selected_backend = "",
                        retry_count = attempted.len(),
                        fallback_reason = "",
                        status = StatusCode::BAD_GATEWAY.as_u16(),
                        outcome = "upstream_error",
                        "request completed"
                    );
                    return StatusCode::BAD_GATEWAY.into_response();
                };
                idx
            }
        };

        let backend = &state.backends[idx];
        let fallback_reason =
            fallback_reason(&state.backends, idx, preferred, origin_node.as_deref());
        if let Some(reason) = fallback_reason {
            record_fallback(state, workload, backend, origin_node.as_deref(), reason);
        }
        let url = format!("{}/{}", backend.url.trim_end_matches('/'), path);

        let upstream = match state
            .client
            .post(&url)
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => {
                backend.circuit.write().await.record_failure();
                record_retry(
                    state,
                    workload,
                    backend,
                    origin_node.as_deref(),
                    "connection_error",
                );
                warn!(
                    request_id,
                    workload,
                    origin_node = origin_node.as_deref().unwrap_or(""),
                    affinity_key = thread_id.as_deref().unwrap_or(""),
                    failed_backend = backend.id,
                    retry_count = attempted.len() + 1,
                    reason = "connection_error",
                    "retrying after upstream failure"
                );
                attempted.push(idx);
                next_backend = select_backend_excluding(
                    &state.backends,
                    preferred,
                    origin_node.as_deref(),
                    &attempted,
                )
                .await;
                if next_backend.is_some() {
                    continue;
                }
                record_request(
                    state,
                    workload,
                    Some(backend),
                    origin_node.as_deref(),
                    StatusCode::BAD_GATEWAY,
                    "upstream_error",
                    started,
                );
                info!(
                    request_id,
                    workload,
                    origin_node = origin_node.as_deref().unwrap_or(""),
                    affinity_key = thread_id.as_deref().unwrap_or(""),
                    selected_backend = backend.id,
                    retry_count = attempted.len(),
                    fallback_reason = fallback_reason.unwrap_or(""),
                    status = StatusCode::BAD_GATEWAY.as_u16(),
                    outcome = "upstream_error",
                    "request completed"
                );
                return StatusCode::BAD_GATEWAY.into_response();
            }
        };

        let status = upstream.status();

        if status.is_server_error() {
            backend.circuit.write().await.record_failure();
            record_retry(
                state,
                workload,
                backend,
                origin_node.as_deref(),
                "server_error",
            );
            warn!(
                request_id,
                workload,
                origin_node = origin_node.as_deref().unwrap_or(""),
                affinity_key = thread_id.as_deref().unwrap_or(""),
                failed_backend = backend.id,
                retry_count = attempted.len() + 1,
                status = status.as_u16(),
                reason = "server_error",
                "retrying after upstream failure"
            );
            attempted.push(idx);
            next_backend = select_backend_excluding(
                &state.backends,
                preferred,
                origin_node.as_deref(),
                &attempted,
            )
            .await;
            if next_backend.is_some() {
                continue;
            }
            record_request(
                state,
                workload,
                Some(backend),
                origin_node.as_deref(),
                status,
                "upstream_5xx",
                started,
            );
            info!(
                request_id,
                workload,
                origin_node = origin_node.as_deref().unwrap_or(""),
                affinity_key = thread_id.as_deref().unwrap_or(""),
                selected_backend = backend.id,
                retry_count = attempted.len(),
                fallback_reason = fallback_reason.unwrap_or(""),
                status = status.as_u16(),
                outcome = "upstream_5xx",
                "request completed"
            );
            return upstream_response(upstream, status);
        }

        backend.circuit.write().await.record_success();
        // Update affinity so the next request from this thread prefers the
        // same backend — including after a fallback re-route.
        if let Some(ref tid) = thread_id {
            state.affinity.record(tid, idx).await;
        }

        let outcome = if attempted.is_empty() {
            "completed"
        } else {
            "retried_success"
        };
        record_request(
            state,
            workload,
            Some(backend),
            origin_node.as_deref(),
            status,
            outcome,
            started,
        );
        info!(
            request_id,
            workload,
            origin_node = origin_node.as_deref().unwrap_or(""),
            affinity_key = thread_id.as_deref().unwrap_or(""),
            selected_backend = backend.id,
            retry_count = attempted.len(),
            fallback_reason = fallback_reason.unwrap_or(""),
            status = status.as_u16(),
            outcome,
            "request completed"
        );

        return upstream_response(upstream, status);
    }
}

fn fallback_reason(
    backends: &[BackendState],
    selected_idx: usize,
    preferred: Option<usize>,
    origin_node: Option<&str>,
) -> Option<&'static str> {
    let selected = backends.get(selected_idx)?;

    if let Some(origin) = origin_node {
        let has_origin_backend = backends
            .iter()
            .any(|backend| backend.node_id.as_deref() == Some(origin));
        if has_origin_backend && selected.node_id.as_deref() != Some(origin) {
            return Some("origin_unavailable");
        }
    }

    if let Some(preferred_idx) = preferred {
        if preferred_idx != selected_idx && backends.get(preferred_idx).is_some() {
            return Some("affinity_unavailable");
        }
    }

    None
}

fn record_request(
    state: &AppState,
    workload: &str,
    backend: Option<&BackendState>,
    origin_node: Option<&str>,
    status: StatusCode,
    outcome: &str,
    started: Instant,
) {
    let labels = RequestLabels {
        workload: workload.to_string(),
        backend: backend.map(|b| b.id.clone()).unwrap_or_default(),
        node: backend.and_then(|b| b.node_id.clone()).unwrap_or_default(),
        pool: backend.map(|b| b.pool.clone()).unwrap_or_default(),
        origin_node: origin_node.unwrap_or("").to_string(),
        status: status.as_u16(),
        outcome: outcome.to_string(),
    };
    state.metrics.record_request(labels, started.elapsed());
}

fn record_fallback(
    state: &AppState,
    workload: &str,
    backend: &BackendState,
    origin_node: Option<&str>,
    reason: &str,
) {
    let labels = FallbackLabels {
        workload: workload.to_string(),
        backend: backend.id.clone(),
        node: backend.node_id.clone().unwrap_or_default(),
        pool: backend.pool.clone(),
        origin_node: origin_node.unwrap_or("").to_string(),
        reason: reason.to_string(),
    };
    state.metrics.record_fallback(labels);
}

fn record_retry(
    state: &AppState,
    workload: &str,
    backend: &BackendState,
    origin_node: Option<&str>,
    reason: &str,
) {
    let labels = RetryLabels {
        workload: workload.to_string(),
        backend: backend.id.clone(),
        node: backend.node_id.clone().unwrap_or_default(),
        pool: backend.pool.clone(),
        origin_node: origin_node.unwrap_or("").to_string(),
        reason: reason.to_string(),
    };
    state.metrics.record_retry(labels);
}

fn upstream_response(upstream: reqwest::Response, status: StatusCode) -> Response {
    let content_type = upstream.headers().get(CONTENT_TYPE).cloned();
    let is_sse = content_type
        .as_ref()
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("text/event-stream"))
        .unwrap_or(false);

    let stream = upstream.bytes_stream();
    let mut builder = Response::builder().status(status);

    if let Some(ct) = content_type {
        builder = builder.header(CONTENT_TYPE, ct);
    }
    if is_sse {
        builder = builder
            .header("cache-control", "no-cache")
            .header("x-accel-buffering", "no");
    }

    builder.body(Body::from_stream(stream)).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BackendConfig, WorkloadKind};
    use crate::{build_router, router::BackendState, router::SessionAffinity};
    use axum::{http::StatusCode, routing::post, Router};
    use serde_json::json;
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    async fn start_mock(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{}/v1", addr)
    }

    /// Start Fairlead with given backends. High circuit thresholds so existing
    /// proxy tests don't accidentally trip the circuit.
    async fn start_fairlead(backend_urls: &[&str]) -> String {
        start_fairlead_with_backends(
            backend_urls
                .iter()
                .map(|u| BackendState::new(u.to_string(), 10, Duration::from_secs(60)))
                .collect(),
        )
        .await
    }

    async fn start_fairlead_with_backends(backends: Vec<BackendState>) -> String {
        let state = AppState {
            client: reqwest::Client::new(),
            backends,
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
        };
        start_fairlead_with_state(state).await
    }

    async fn start_fairlead_with_state(state: AppState) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, build_router(state)).await.unwrap();
        });
        format!("http://{}", addr)
    }

    fn backend_on_node(url: String, node_id: &str) -> BackendState {
        BackendState::from_config(
            BackendConfig {
                id: format!("{node_id}-vllm"),
                url,
                node_id: Some(node_id.to_string()),
                pool: "local-llm".into(),
                workloads: WorkloadKind::default_proxy_workloads(),
                health_path: None,
            },
            10,
            Duration::from_secs(60),
        )
    }

    fn backend_with_id(url: String, id: &str) -> BackendState {
        BackendState::from_config(
            BackendConfig {
                id: id.to_string(),
                url,
                node_id: None,
                pool: "default".into(),
                workloads: WorkloadKind::default_proxy_workloads(),
                health_path: None,
            },
            10,
            Duration::from_secs(60),
        )
    }

    // ── existing proxy coverage ──────────────────────────────────────────────

    #[tokio::test]
    async fn no_backends_returns_503() {
        let fairlead = start_fairlead(&[]).await;
        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
    }

    #[tokio::test]
    async fn non_streaming_completion_proxied() {
        let mock_body = json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "model": "test-model",
            "choices": [{"index":0,"message":{"role":"assistant","content":"Hello!"},"finish_reason":"stop"}]
        });
        let body = mock_body.clone();
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let b = body.clone();
                async move { axum::Json(b) }
            }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"test-model","messages":[{"role":"user","content":"hi"}]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let received: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(received["choices"][0]["message"]["content"], "Hello!");
    }

    #[tokio::test]
    async fn streaming_completion_proxied() {
        let sse_body = "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(move || async move {
                Response::builder()
                    .status(200)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse_body))
                    .unwrap()
            }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[],"stream":true}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let ct = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(ct.contains("text/event-stream"), "expected SSE, got {ct}");
        assert!(resp.text().await.unwrap().contains("data: [DONE]"));
    }

    #[tokio::test]
    async fn backend_unreachable_returns_502() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let fairlead = start_fairlead(&[&format!("http://{}/v1", addr)]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 502);
    }

    #[tokio::test]
    async fn connection_failure_retries_next_backend() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let dead_backend = format!("http://{}/v1", addr);

        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async {
                axum::Json(json!({
                    "id": "chatcmpl-retry",
                    "choices": [{"message": {"role": "assistant", "content": "retried"}}]
                }))
            }),
        );
        let healthy_backend = start_mock(mock).await;
        let fairlead = start_fairlead_with_backends(vec![
            backend_with_id(dead_backend, "dead"),
            backend_with_id(healthy_backend, "healthy"),
        ])
        .await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let received: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(received["choices"][0]["message"]["content"], "retried");

        let metrics = client
            .get(format!("{}/metrics", fairlead))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(metrics.contains(
            "fairlead_retries_total{workload=\"chat_completions\",backend=\"dead\",node=\"\",pool=\"default\",origin_node=\"\",reason=\"connection_error\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_requests_total{workload=\"chat_completions\",backend=\"healthy\",node=\"\",pool=\"default\",origin_node=\"\",status=\"200\",outcome=\"retried_success\"} 1"
        ));
    }

    #[tokio::test]
    async fn server_error_retries_next_backend() {
        let first_hits = Arc::new(AtomicUsize::new(0));
        let first_hits_for_route = first_hits.clone();
        let first = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let hits = first_hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }),
        );
        let first_backend = start_mock(first).await;

        let second_hits = Arc::new(AtomicUsize::new(0));
        let second_hits_for_route = second_hits.clone();
        let second = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let hits = second_hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::Json(json!({
                        "id": "chatcmpl-server-error-retry",
                        "choices": [{"message": {"role": "assistant", "content": "fallback"}}]
                    }))
                }
            }),
        );
        let second_backend = start_mock(second).await;
        let fairlead = start_fairlead_with_backends(vec![
            backend_with_id(first_backend, "primary"),
            backend_with_id(second_backend, "secondary"),
        ])
        .await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let received: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(received["choices"][0]["message"]["content"], "fallback");
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.load(Ordering::SeqCst), 1);

        let metrics = reqwest::Client::new()
            .get(format!("{}/metrics", fairlead))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(metrics.contains(
            "fairlead_retries_total{workload=\"chat_completions\",backend=\"primary\",node=\"\",pool=\"default\",origin_node=\"\",reason=\"server_error\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_requests_total{workload=\"chat_completions\",backend=\"secondary\",node=\"\",pool=\"default\",origin_node=\"\",status=\"200\",outcome=\"retried_success\"} 1"
        ));
    }

    #[tokio::test]
    async fn client_error_does_not_retry_next_backend() {
        let first = Router::new().route(
            "/v1/chat/completions",
            post(|| async { StatusCode::TOO_MANY_REQUESTS }),
        );
        let first_backend = start_mock(first).await;

        let second_hit = Arc::new(AtomicBool::new(false));
        let second_hit_for_route = second_hit.clone();
        let second = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let hit = second_hit_for_route.clone();
                async move {
                    hit.store(true, Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        );
        let second_backend = start_mock(second).await;
        let fairlead = start_fairlead(&[&first_backend, &second_backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 429);
        assert!(!second_hit.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn embeddings_proxied() {
        let mock_body = json!({
            "object":"list",
            "data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}],
            "model":"text-embedding-ada-002"
        });
        let body = mock_body.clone();
        let mock = Router::new().route(
            "/v1/embeddings",
            post(move || {
                let b = body.clone();
                async move { axum::Json(b) }
            }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/embeddings", fairlead))
            .json(&json!({"model":"text-embedding-ada-002","input":"hello"}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let received: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(received["data"][0]["embedding"][0], 0.1);
    }

    #[tokio::test]
    async fn embeddings_uses_fallback_chain_when_first_backend_open() {
        let first = Router::new().route(
            "/v1/embeddings",
            post(|| async {
                axum::Json(json!({
                    "object":"list",
                    "data":[{"object":"embedding","index":0,"embedding":[9.9]}],
                    "model":"first"
                }))
            }),
        );
        let second = Router::new().route(
            "/v1/embeddings",
            post(|| async {
                axum::Json(json!({
                    "object":"list",
                    "data":[{"object":"embedding","index":0,"embedding":[0.2]}],
                    "model":"second"
                }))
            }),
        );
        let first_url = start_mock(first).await;
        let second_url = start_mock(second).await;

        let first_backend = BackendState::new(first_url, 1, Duration::from_secs(60));
        first_backend.circuit.write().await.record_failure();
        let second_backend = BackendState::new(second_url, 10, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![first_backend, second_backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/embeddings", fairlead))
            .json(&json!({"model":"m","input":"hello"}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let received: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(received["model"], "second");
        assert_eq!(received["data"][0]["embedding"][0], 0.2);
    }

    #[tokio::test]
    async fn routing_metrics_record_workload_backend_and_origin() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"choices": [{"message": {"content": "ok"}}]})) }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead_with_backends(vec![backend_on_node(backend, "node-a")]).await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", fairlead))
            .header("x-fairlead-origin-node", "node-a")
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let metrics = client
            .get(format!("{}/metrics", fairlead))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();

        assert!(metrics.contains(
            "fairlead_requests_total{workload=\"chat_completions\",backend=\"node-a-vllm\",node=\"node-a\",pool=\"local-llm\",origin_node=\"node-a\",status=\"200\",outcome=\"completed\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_request_latency_seconds_count{workload=\"chat_completions\",backend=\"node-a-vllm\",node=\"node-a\",pool=\"local-llm\",origin_node=\"node-a\",status=\"200\",outcome=\"completed\"} 1"
        ));
    }

    #[tokio::test]
    async fn backend_error_status_forwarded() {
        for status_code in [400u16, 429, 500] {
            let mock = Router::new().route(
                "/v1/chat/completions",
                post(move || async move { StatusCode::from_u16(status_code).unwrap() }),
            );
            let backend = start_mock(mock).await;
            let fairlead = start_fairlead(&[&backend]).await;

            let resp = reqwest::Client::new()
                .post(format!("{}/v1/chat/completions", fairlead))
                .json(&json!({"model":"m","messages":[]}))
                .send()
                .await
                .unwrap();

            assert_eq!(resp.status().as_u16(), status_code);
        }
    }

    #[tokio::test]
    async fn request_body_forwarded_verbatim() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|body: Bytes| async move {
                (StatusCode::OK, [("content-type", "application/json")], body)
            }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let payload = json!({"model":"test","messages":[{"role":"user","content":"verbatim"}],"temperature":0.7,"max_tokens":256});
        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&payload)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let echoed: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(echoed["temperature"], 0.7);
        assert_eq!(echoed["max_tokens"], 256);
    }

    #[tokio::test]
    async fn embeddings_no_backends_returns_503() {
        let fairlead = start_fairlead(&[]).await;
        let resp = reqwest::Client::new()
            .post(format!("{}/v1/embeddings", fairlead))
            .json(&json!({"model":"m","input":"hello"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
    }

    // ── circuit breaker integration ──────────────────────────────────────────

    #[tokio::test]
    async fn circuit_opens_on_repeated_5xx() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let backend_url = start_mock(mock).await;
        let backend = BackendState::new(backend_url, 2, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            500
        );
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            500
        );
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            503
        );
    }

    #[tokio::test]
    async fn circuit_stays_closed_on_4xx() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { StatusCode::BAD_REQUEST }),
        );
        let backend_url = start_mock(mock).await;
        let backend = BackendState::new(backend_url, 1, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});
        for _ in 0..3 {
            assert_eq!(
                client.post(&url).json(&body).send().await.unwrap().status(),
                400
            );
        }
    }

    #[tokio::test]
    async fn connection_failure_trips_circuit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let backend = BackendState::new(format!("http://{}/v1", addr), 2, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            502
        );
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            502
        );
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            503
        );
    }

    #[tokio::test]
    async fn half_open_failure_reopens_circuit() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let backend_url = start_mock(mock).await;
        let backend = BackendState::new(backend_url, 1, Duration::from_millis(50));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            500
        );
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            503
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            500,
            "half-open probe reached backend"
        );
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            503,
            "circuit re-opened"
        );
    }

    #[tokio::test]
    async fn circuit_recovers_after_cooldown() {
        let should_fail = Arc::new(AtomicBool::new(true));
        let sf = should_fail.clone();
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let flag = sf.clone();
                async move {
                    if flag.load(Ordering::SeqCst) {
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    } else {
                        axum::Json(json!({"recovered":true})).into_response()
                    }
                }
            }),
        );
        let backend_url = start_mock(mock).await;
        let backend = BackendState::new(backend_url, 1, Duration::from_millis(50));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            500
        );
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            503
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        should_fail.store(false, Ordering::SeqCst);
        assert_eq!(
            client.post(&url).json(&body).send().await.unwrap().status(),
            200
        );
        let r4 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r4.status(), 200);
        assert_eq!(
            r4.json::<serde_json::Value>().await.unwrap()["recovered"],
            true
        );
    }

    // ── fallback chain integration ───────────────────────────────────────────

    /// When the first backend's circuit is open, requests fall back to the second.
    #[tokio::test]
    async fn fallback_to_second_when_first_circuit_open() {
        let first = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"first"})) }),
        );
        let second = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"second"})) }),
        );
        let first_url = start_mock(first).await;
        let second_url = start_mock(second).await;

        let first_backend = BackendState::new(first_url, 1, Duration::from_secs(60));
        first_backend.circuit.write().await.record_failure(); // open it
        let second_backend = BackendState::new(second_url, 10, Duration::from_secs(60));

        let fairlead = start_fairlead_with_backends(vec![first_backend, second_backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.json::<serde_json::Value>().await.unwrap()["source"],
            "second"
        );
    }

    /// When all circuits are open, return 503 without touching any backend.
    #[tokio::test]
    async fn all_backends_open_returns_503() {
        let first_backend =
            BackendState::new("http://a:8000/v1".into(), 1, Duration::from_secs(60));
        let second_backend =
            BackendState::new("http://b:8000/v1".into(), 1, Duration::from_secs(60));
        first_backend.circuit.write().await.record_failure();
        second_backend.circuit.write().await.record_failure();

        let fairlead = start_fairlead_with_backends(vec![first_backend, second_backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 503);
    }

    // ── origin-node locality integration ────────────────────────────────────

    #[tokio::test]
    async fn origin_node_routes_to_same_node_backend() {
        let node_a = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"node-a"})) }),
        );
        let node_b = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"node-b"})) }),
        );
        let node_a_url = start_mock(node_a).await;
        let node_b_url = start_mock(node_b).await;

        let fairlead = start_fairlead_with_backends(vec![
            backend_on_node(node_a_url, "node-a"),
            backend_on_node(node_b_url, "node-b"),
        ])
        .await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .header("x-fairlead-origin-node", "node-b")
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.json::<serde_json::Value>().await.unwrap()["source"],
            "node-b"
        );
    }

    #[tokio::test]
    async fn origin_node_falls_back_when_same_node_backend_open() {
        let node_a = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"node-a"})) }),
        );
        let node_b = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"node-b"})) }),
        );
        let node_a_url = start_mock(node_a).await;
        let node_b_url = start_mock(node_b).await;

        let node_a_backend = backend_on_node(node_a_url, "node-a");
        for _ in 0..10 {
            node_a_backend.circuit.write().await.record_failure();
        }
        let fairlead = start_fairlead_with_backends(vec![
            node_a_backend,
            backend_on_node(node_b_url, "node-b"),
        ])
        .await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", fairlead))
            .header("x-fairlead-origin-node", "node-a")
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.json::<serde_json::Value>().await.unwrap()["source"],
            "node-b"
        );

        let metrics = client
            .get(format!("{}/metrics", fairlead))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(metrics.contains(
            "fairlead_fallbacks_total{workload=\"chat_completions\",backend=\"node-b-vllm\",node=\"node-b\",pool=\"local-llm\",origin_node=\"node-a\",reason=\"origin_unavailable\"} 1"
        ));
    }

    #[tokio::test]
    async fn origin_node_precedes_existing_affinity() {
        let node_a = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"node-a"})) }),
        );
        let node_b = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"node-b"})) }),
        );
        let node_a_url = start_mock(node_a).await;
        let node_b_url = start_mock(node_b).await;

        let fairlead = start_fairlead_with_backends(vec![
            backend_on_node(node_a_url, "node-a"),
            backend_on_node(node_b_url, "node-b"),
        ])
        .await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        let first = client
            .post(&url)
            .header("x-fairlead-origin-node", "node-b")
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            first.json::<serde_json::Value>().await.unwrap()["source"],
            "node-b"
        );

        let second = client
            .post(&url)
            .header("x-fairlead-origin-node", "node-a")
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            second.json::<serde_json::Value>().await.unwrap()["source"],
            "node-a",
            "same-node locality should take precedence over prior affinity"
        );
    }

    // ── session affinity integration ─────────────────────────────────────────

    /// A thread is routed to the same backend on subsequent requests.
    #[tokio::test]
    async fn affinity_routes_thread_to_same_backend() {
        let first = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"first"})) }),
        );
        let second = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"second"})) }),
        );
        let first_url = start_mock(first).await;
        let second_url = start_mock(second).await;

        // Open backend 0 so the first request lands on backend 1, recording affinity.
        let first_backend = BackendState::new(first_url, 1, Duration::from_secs(60));
        let first_handle = first_backend.clone();
        first_handle.circuit.write().await.record_failure();
        let second_backend = BackendState::new(second_url, 10, Duration::from_secs(60));

        let fairlead = start_fairlead_with_backends(vec![first_backend, second_backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        // Request 1 with thread-1 → routes to backend 1 (0 is open), affinity recorded.
        let r1 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            r1.json::<serde_json::Value>().await.unwrap()["source"],
            "second"
        );

        // Restore backend 0 — now BOTH are available.
        first_handle.circuit.write().await.record_success();

        // Request 2 with thread-1 → affinity map says backend 1 → still "second".
        let r2 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            r2.json::<serde_json::Value>().await.unwrap()["source"],
            "second",
            "affinity should keep thread on backend 1"
        );

        // Request with thread-2 (no affinity) → goes to backend 0 (first available).
        let r3 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-2")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            r3.json::<serde_json::Value>().await.unwrap()["source"],
            "first"
        );
    }

    #[tokio::test]
    async fn affinity_preserved_across_streaming_requests() {
        let first_sse = "data: {\"source\":\"first\"}\n\ndata: [DONE]\n\n";
        let second_sse = "data: {\"source\":\"second\"}\n\ndata: [DONE]\n\n";
        let first = Router::new().route(
            "/v1/chat/completions",
            post(move || async move {
                Response::builder()
                    .status(200)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(first_sse))
                    .unwrap()
            }),
        );
        let second = Router::new().route(
            "/v1/chat/completions",
            post(move || async move {
                Response::builder()
                    .status(200)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(second_sse))
                    .unwrap()
            }),
        );
        let first_url = start_mock(first).await;
        let second_url = start_mock(second).await;

        let first_backend = BackendState::new(first_url, 1, Duration::from_secs(60));
        let first_handle = first_backend.clone();
        first_handle.circuit.write().await.record_failure();
        let second_backend = BackendState::new(second_url, 10, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![first_backend, second_backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[],"stream":true});

        let r1 = client
            .post(&url)
            .header("x-fairlead-thread-id", "stream-thread")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(r1.text().await.unwrap().contains("\"source\":\"second\""));

        first_handle.circuit.write().await.record_success();

        let r2 = client
            .post(&url)
            .header("x-fairlead-thread-id", "stream-thread")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            r2.text().await.unwrap().contains("\"source\":\"second\""),
            "streaming request should respect recorded affinity"
        );
    }

    #[tokio::test]
    async fn no_thread_id_does_not_pollute_affinity_map() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"only"})) }),
        );
        let backend = start_mock(mock).await;
        let affinity = SessionAffinity::default();
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![BackendState::new(backend, 10, Duration::from_secs(60))],
            affinity: affinity.clone(),
            metrics: crate::metrics::RoutingMetrics::default(),
        };
        let fairlead = start_fairlead_with_state(state).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        assert_eq!(affinity.len().await, 0);
    }

    /// Affinity is only updated on success, never on failure. When a preferred
    /// backend starts returning 5xx, the same request retries the next eligible
    /// backend and affinity follows the successful retry target.
    #[tokio::test]
    async fn affinity_follows_same_request_retry_after_5xx_degradation() {
        let a_failing = Arc::new(AtomicBool::new(false));
        let af = a_failing.clone();
        let a_hits = Arc::new(AtomicUsize::new(0));
        let a_hits_for_route = a_hits.clone();

        let backend_a = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let flag = af.clone();
                let hits = a_hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    if flag.load(Ordering::SeqCst) {
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    } else {
                        axum::Json(json!({"source": "a"})).into_response()
                    }
                }
            }),
        );
        let backend_b = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source": "b"})) }),
        );

        let url_a = start_mock(backend_a).await;
        let url_b = start_mock(backend_b).await;

        // threshold=2: two 5xx responses open the circuit on A.
        let state_a = BackendState::new(url_a, 2, Duration::from_secs(60));
        let state_b = BackendState::new(url_b, 10, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![state_a, state_b]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model": "m", "messages": []});

        // Step 1: healthy request establishes affinity thread-1 → A (index 0).
        let r1 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r1.json::<serde_json::Value>().await.unwrap()["source"], "a");

        // Step 2: A starts failing.
        a_failing.store(true, Ordering::SeqCst);

        // Step 3: A returns 5xx, Fairlead records the failure, retries B in the
        // same request, and updates affinity to B after that retry succeeds.
        let r2 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r2.status(), 200);
        assert_eq!(r2.json::<serde_json::Value>().await.unwrap()["source"], "b");
        assert_eq!(a_hits.load(Ordering::SeqCst), 2);

        // Step 4: Affinity now points to B. The next request does not hit A.
        let r3 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            r3.json::<serde_json::Value>().await.unwrap()["source"],
            "b",
            "affinity updated to B after fallback"
        );
        assert_eq!(a_hits.load(Ordering::SeqCst), 2);
    }

    /// Soft affinity: when the preferred backend's circuit opens, the request
    /// falls back to another backend and the affinity map is updated.
    #[tokio::test]
    async fn affinity_falls_back_and_updates_when_preferred_opens() {
        let first = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"first"})) }),
        );
        let second = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"second"})) }),
        );
        let first_url = start_mock(first).await;
        let second_url = start_mock(second).await;

        let first_backend = BackendState::new(first_url, 1, Duration::from_secs(60));
        let first_handle = first_backend.clone();
        let second_backend = BackendState::new(second_url, 1, Duration::from_secs(60));
        let second_handle = second_backend.clone();

        // Open backend 0 → thread-1's first request lands on backend 1.
        first_handle.circuit.write().await.record_failure();

        let fairlead = start_fairlead_with_backends(vec![first_backend, second_backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        // Establish affinity: thread-1 → backend 1 ("second").
        let r1 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            r1.json::<serde_json::Value>().await.unwrap()["source"],
            "second"
        );

        // Now open backend 1's circuit and restore backend 0.
        first_handle.circuit.write().await.record_success();
        second_handle.circuit.write().await.record_failure();

        // thread-1 preferred backend 1 (open) → falls back to backend 0.
        let r2 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            r2.json::<serde_json::Value>().await.unwrap()["source"],
            "first",
            "should fall back to backend 0"
        );

        // Affinity map should now point thread-1 → backend 0.
        // Restore backend 1 — thread-1 should still prefer backend 0 (the updated affinity).
        second_handle.circuit.write().await.record_success();
        let r3 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            r3.json::<serde_json::Value>().await.unwrap()["source"],
            "first",
            "affinity should have updated to backend 0"
        );
    }
}
