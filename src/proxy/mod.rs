pub mod types;

use axum::{
    body::Body,
    extract::State,
    http::{
        header::{AUTHORIZATION, CONTENT_TYPE},
        HeaderMap, StatusCode,
    },
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use std::time::Instant;
use tracing::{info, warn};

use crate::{
    config::{Priority, WorkloadKind, WorkloadRoute},
    metrics::{FallbackLabels, RequestLabels, RetryLabels},
    priority::PriorityPermit,
    router::{select_backend_excluding_resource, BackendState, ResourceRank},
    AppState,
};

const DEFAULT_UPSTREAM_CONTENT_TYPE: &str = "application/json";
const FORWARDED_UPSTREAM_HEADERS: &[&str] = &[
    "openai-organization",
    "openai-project",
    "anthropic-version",
    "anthropic-beta",
    "x-goog-api-key",
];

pub async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(
        &state,
        WorkloadKind::ChatCompletions.route(),
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
    forward(&state, WorkloadKind::Embeddings.route(), &headers, body).await
}

async fn forward(
    state: &AppState,
    route: WorkloadRoute,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let workload_kind = route.kind;
    let workload = workload_kind.as_str();
    let started = Instant::now();
    let priority = match parse_priority(headers) {
        Ok(priority) => priority,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
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
    let affinity_key = thread_id
        .as_deref()
        .map(|tid| affinity_key(workload_kind, tid));
    let affinity_key_str = affinity_key.as_deref().unwrap_or("");

    // Extract optional origin node for locality-aware routing.
    let origin_node = headers
        .get("x-fairlead-origin-node")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    if state.backends.is_empty() {
        record_request(
            state,
            workload,
            priority,
            None,
            origin_node.as_deref(),
            StatusCode::SERVICE_UNAVAILABLE,
            "no_backends",
            started,
        );
        return (StatusCode::SERVICE_UNAVAILABLE, "no backends configured").into_response();
    }

    let Some(priority_permit) = state.priority_limiter.try_acquire(priority) else {
        record_request(
            state,
            workload,
            priority,
            None,
            origin_node.as_deref(),
            StatusCode::TOO_MANY_REQUESTS,
            "priority_limited",
            started,
        );
        info!(
            request_id,
            workload,
            priority = priority.as_str(),
            origin_node = origin_node.as_deref().unwrap_or(""),
            affinity_key = affinity_key_str,
            selected_backend = "",
            retry_count = 0,
            fallback_reason = "",
            status = StatusCode::TOO_MANY_REQUESTS.as_u16(),
            outcome = "priority_limited",
            "request completed"
        );
        return (StatusCode::TOO_MANY_REQUESTS, "priority limit reached").into_response();
    };

    // Resolve preferred backend index (if any) then run the fallback chain.
    let preferred = match affinity_key {
        Some(ref key) => state.affinity.preferred(key).await,
        None => None,
    };

    let mut attempted = Vec::new();
    let mut next_backend = None;
    let route_ineligible = route_ineligible_backends(&state.backends, &route);
    let resource_state = resource_selection_state(state, &workload_kind).await;
    let selection_ineligible = selection_ineligible(&route_ineligible, &resource_state.ineligible);

    loop {
        let idx = match next_backend.take() {
            Some(idx) => idx,
            None => {
                let Some(idx) = select_backend_excluding_resource(
                    &state.backends,
                    preferred,
                    origin_node.as_deref(),
                    &attempted,
                    &selection_ineligible,
                    &resource_state.ranks,
                )
                .await
                else {
                    if attempted.is_empty() {
                        record_request(
                            state,
                            workload,
                            priority,
                            None,
                            origin_node.as_deref(),
                            StatusCode::SERVICE_UNAVAILABLE,
                            unavailable_outcome(
                                &route_ineligible,
                                &resource_state.ineligible,
                                state.backends.len(),
                            ),
                            started,
                        );
                        info!(
                            request_id,
                            workload,
                            priority = priority.as_str(),
                            origin_node = origin_node.as_deref().unwrap_or(""),
                            affinity_key = affinity_key_str,
                            selected_backend = "",
                            retry_count = attempted.len(),
                            fallback_reason = unavailable_fallback_reason(
                                &route_ineligible,
                                &resource_state.ineligible,
                                state.backends.len(),
                            ),
                            status = StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                            outcome = unavailable_outcome(
                                &route_ineligible,
                                &resource_state.ineligible,
                                state.backends.len(),
                            ),
                            "request completed"
                        );
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            unavailable_message(
                                &route_ineligible,
                                &resource_state.ineligible,
                                state.backends.len(),
                            ),
                        )
                            .into_response();
                    }
                    record_request(
                        state,
                        workload,
                        priority,
                        None,
                        origin_node.as_deref(),
                        StatusCode::BAD_GATEWAY,
                        "upstream_error",
                        started,
                    );
                    info!(
                        request_id,
                        workload,
                        priority = priority.as_str(),
                        origin_node = origin_node.as_deref().unwrap_or(""),
                        affinity_key = affinity_key_str,
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
        let fallback_reason = fallback_reason(
            &state.backends,
            idx,
            preferred,
            origin_node.as_deref(),
            &route_ineligible,
            &resource_state.ineligible,
        );
        if let Some(reason) = fallback_reason {
            record_fallback(
                state,
                workload,
                priority,
                backend,
                origin_node.as_deref(),
                reason,
            );
        }
        let url = format!(
            "{}/{}",
            backend.url.trim_end_matches('/'),
            route.upstream_path
        );

        let upstream = match state
            .client
            .post(&url)
            .with_upstream_headers(headers)
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
                    priority,
                    backend,
                    origin_node.as_deref(),
                    "connection_error",
                );
                warn!(
                    request_id,
                    workload,
                    priority = priority.as_str(),
                    origin_node = origin_node.as_deref().unwrap_or(""),
                    affinity_key = affinity_key_str,
                    failed_backend = backend.id,
                    retry_count = attempted.len() + 1,
                    reason = "connection_error",
                    "retrying after upstream failure"
                );
                attempted.push(idx);
                next_backend = select_backend_excluding_resource(
                    &state.backends,
                    preferred,
                    origin_node.as_deref(),
                    &attempted,
                    &selection_ineligible,
                    &resource_state.ranks,
                )
                .await;
                if next_backend.is_some() {
                    continue;
                }
                record_request(
                    state,
                    workload,
                    priority,
                    Some(backend),
                    origin_node.as_deref(),
                    StatusCode::BAD_GATEWAY,
                    "upstream_error",
                    started,
                );
                info!(
                    request_id,
                    workload,
                    priority = priority.as_str(),
                    origin_node = origin_node.as_deref().unwrap_or(""),
                    affinity_key = affinity_key_str,
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

        if route.retry_server_errors && status.is_server_error() {
            backend.circuit.write().await.record_failure();
            record_retry(
                state,
                workload,
                priority,
                backend,
                origin_node.as_deref(),
                "server_error",
            );
            warn!(
                request_id,
                workload,
                priority = priority.as_str(),
                origin_node = origin_node.as_deref().unwrap_or(""),
                affinity_key = affinity_key_str,
                failed_backend = backend.id,
                retry_count = attempted.len() + 1,
                status = status.as_u16(),
                reason = "server_error",
                "retrying after upstream failure"
            );
            attempted.push(idx);
            next_backend = select_backend_excluding_resource(
                &state.backends,
                preferred,
                origin_node.as_deref(),
                &attempted,
                &selection_ineligible,
                &resource_state.ranks,
            )
            .await;
            if next_backend.is_some() {
                continue;
            }
            record_request(
                state,
                workload,
                priority,
                Some(backend),
                origin_node.as_deref(),
                status,
                "upstream_5xx",
                started,
            );
            info!(
                request_id,
                workload,
                priority = priority.as_str(),
                origin_node = origin_node.as_deref().unwrap_or(""),
                affinity_key = affinity_key_str,
                selected_backend = backend.id,
                retry_count = attempted.len(),
                fallback_reason = fallback_reason.unwrap_or(""),
                status = status.as_u16(),
                outcome = "upstream_5xx",
                "request completed"
            );
            return upstream_response(upstream, status, priority_permit);
        }

        backend.circuit.write().await.record_success();
        // Update workload-scoped affinity so the next request from this thread
        // and workload prefers the same backend, including after fallback.
        if let Some(ref key) = affinity_key {
            state.affinity.record(key, idx).await;
        }

        let outcome = if attempted.is_empty() {
            "completed"
        } else {
            "retried_success"
        };
        record_request(
            state,
            workload,
            priority,
            Some(backend),
            origin_node.as_deref(),
            status,
            outcome,
            started,
        );
        info!(
            request_id,
            workload,
            priority = priority.as_str(),
            origin_node = origin_node.as_deref().unwrap_or(""),
            affinity_key = affinity_key_str,
            selected_backend = backend.id,
            retry_count = attempted.len(),
            fallback_reason = fallback_reason.unwrap_or(""),
            status = status.as_u16(),
            outcome,
            "request completed"
        );

        return upstream_response(upstream, status, priority_permit);
    }
}

fn affinity_key(workload: WorkloadKind, thread_id: &str) -> String {
    format!("{}:{thread_id}", workload.as_str())
}

trait UpstreamHeaderPolicy {
    fn with_upstream_headers(self, headers: &HeaderMap) -> Self;
}

impl UpstreamHeaderPolicy for reqwest::RequestBuilder {
    fn with_upstream_headers(mut self, headers: &HeaderMap) -> Self {
        match headers.get(CONTENT_TYPE) {
            Some(content_type) => {
                self = self.header(CONTENT_TYPE, content_type.clone());
            }
            None => {
                self = self.header(CONTENT_TYPE, DEFAULT_UPSTREAM_CONTENT_TYPE);
            }
        }

        if let Some(authorization) = headers.get(AUTHORIZATION) {
            self = self.header(AUTHORIZATION, authorization.clone());
        }

        for name in FORWARDED_UPSTREAM_HEADERS {
            if let Some(value) = headers.get(*name) {
                self = self.header(*name, value.clone());
            }
        }

        self
    }
}

fn fallback_reason(
    backends: &[BackendState],
    selected_idx: usize,
    preferred: Option<usize>,
    origin_node: Option<&str>,
    route_ineligible: &[usize],
    resource_ineligible: &[usize],
) -> Option<&'static str> {
    let selected = backends.get(selected_idx)?;

    if let Some(origin) = origin_node {
        let origin_indexes: Vec<_> = backends
            .iter()
            .enumerate()
            .filter_map(|(idx, backend)| {
                (backend.node_id.as_deref() == Some(origin)).then_some(idx)
            })
            .collect();
        let has_origin_backend = !origin_indexes.is_empty();
        if has_origin_backend && selected.node_id.as_deref() != Some(origin) {
            if origin_indexes
                .iter()
                .any(|idx| route_ineligible.contains(idx))
            {
                return Some("workload_unavailable");
            }
            if origin_indexes
                .iter()
                .any(|idx| resource_ineligible.contains(idx))
            {
                return Some("resource_unavailable");
            }
            return Some("origin_unavailable");
        }
    }

    if let Some(preferred_idx) = preferred {
        if preferred_idx != selected_idx && backends.get(preferred_idx).is_some() {
            if route_ineligible.contains(&preferred_idx) {
                return Some("workload_unavailable");
            }
            if resource_ineligible.contains(&preferred_idx) {
                return Some("resource_unavailable");
            }
            return Some("affinity_unavailable");
        }
    }

    if route_ineligible
        .iter()
        .any(|idx| *idx < selected_idx && backends.get(*idx).is_some())
    {
        return Some("workload_unavailable");
    }

    if resource_ineligible
        .iter()
        .any(|idx| *idx < selected_idx && backends.get(*idx).is_some())
    {
        return Some("resource_unavailable");
    }

    None
}

fn route_ineligible_backends(backends: &[BackendState], route: &WorkloadRoute) -> Vec<usize> {
    backends
        .iter()
        .enumerate()
        .filter_map(|(idx, backend)| (!route_allows_backend(backend, route)).then_some(idx))
        .collect()
}

fn route_allows_backend(backend: &BackendState, route: &WorkloadRoute) -> bool {
    route.backend_pool.allows(&backend.pool) && backend.workloads.contains(&route.kind)
}

fn selection_ineligible(route_ineligible: &[usize], resource_ineligible: &[usize]) -> Vec<usize> {
    let mut ineligible = route_ineligible.to_vec();
    for idx in resource_ineligible {
        if !ineligible.contains(idx) {
            ineligible.push(*idx);
        }
    }
    ineligible
}

fn parse_priority(headers: &HeaderMap) -> Result<Priority, &'static str> {
    let Some(value) = headers.get("x-fairlead-priority") else {
        return Ok(Priority::default());
    };

    let value = value
        .to_str()
        .map_err(|_| "invalid X-Fairlead-Priority header")?;

    Priority::parse(value)
        .ok_or("invalid X-Fairlead-Priority: expected realtime, batch, or background")
}

#[derive(Default)]
struct ResourceSelectionState {
    ineligible: Vec<usize>,
    ranks: Vec<Option<ResourceRank>>,
}

async fn resource_selection_state(
    state: &AppState,
    workload: &WorkloadKind,
) -> ResourceSelectionState {
    let mut selection = ResourceSelectionState {
        ineligible: Vec::new(),
        ranks: vec![None; state.backends.len()],
    };

    if !state.resource_policy.enabled {
        return selection;
    }

    let required_vram_mb = state.resource_policy.required_vram_mb(workload);

    for (idx, backend) in state.backends.iter().enumerate() {
        let Some(node_id) = backend.node_id.as_deref() else {
            selection.ineligible.push(idx);
            continue;
        };

        let Some(report) = state
            .resources
            .fresh_backend_report(node_id, &backend.id)
            .await
        else {
            selection.ineligible.push(idx);
            continue;
        };

        if report.available_vram_mb < required_vram_mb {
            selection.ineligible.push(idx);
            continue;
        }

        selection.ranks[idx] = Some(ResourceRank {
            current_load: report.current_load,
            available_vram_mb: report.available_vram_mb,
        });
    }

    selection
}

fn unavailable_outcome(
    route_ineligible: &[usize],
    resource_ineligible: &[usize],
    backend_count: usize,
) -> &'static str {
    if backend_count > 0 && route_ineligible.len() == backend_count {
        "unsupported_workload"
    } else if !resource_ineligible.is_empty() {
        "resource_unavailable"
    } else {
        "unavailable"
    }
}

fn unavailable_fallback_reason(
    route_ineligible: &[usize],
    resource_ineligible: &[usize],
    backend_count: usize,
) -> &'static str {
    if backend_count > 0 && route_ineligible.len() == backend_count {
        "workload_unavailable"
    } else if !resource_ineligible.is_empty() {
        "resource_unavailable"
    } else {
        ""
    }
}

fn unavailable_message(
    route_ineligible: &[usize],
    resource_ineligible: &[usize],
    backend_count: usize,
) -> &'static str {
    if backend_count > 0 && route_ineligible.len() == backend_count {
        "no backends configured for workload"
    } else if !resource_ineligible.is_empty() {
        "all backends unavailable (circuits open or insufficient resources)"
    } else {
        "all backends unavailable (circuits open)"
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "request metrics map directly to Prometheus label dimensions"
)]
fn record_request(
    state: &AppState,
    workload: &str,
    priority: Priority,
    backend: Option<&BackendState>,
    origin_node: Option<&str>,
    status: StatusCode,
    outcome: &str,
    started: Instant,
) {
    let labels = RequestLabels {
        workload: workload.to_string(),
        priority: priority.as_str().to_string(),
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
    priority: Priority,
    backend: &BackendState,
    origin_node: Option<&str>,
    reason: &str,
) {
    let labels = FallbackLabels {
        workload: workload.to_string(),
        priority: priority.as_str().to_string(),
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
    priority: Priority,
    backend: &BackendState,
    origin_node: Option<&str>,
    reason: &str,
) {
    let labels = RetryLabels {
        workload: workload.to_string(),
        priority: priority.as_str().to_string(),
        backend: backend.id.clone(),
        node: backend.node_id.clone().unwrap_or_default(),
        pool: backend.pool.clone(),
        origin_node: origin_node.unwrap_or("").to_string(),
        reason: reason.to_string(),
    };
    state.metrics.record_retry(labels);
}

fn upstream_response(
    upstream: reqwest::Response,
    status: StatusCode,
    priority_permit: PriorityPermit,
) -> Response {
    let content_type = upstream.headers().get(CONTENT_TYPE).cloned();
    let is_sse = content_type
        .as_ref()
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("text/event-stream"))
        .unwrap_or(false);

    let stream = upstream.bytes_stream().map(move |chunk| {
        let _permit = &priority_permit;
        chunk
    });
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
    use axum::{
        http::{Request, StatusCode},
        routing::post,
        Router,
    };
    use serde_json::json;
    use std::{
        io,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex, OnceLock,
        },
        time::Duration,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;
    use tower::ServiceExt;
    use tracing::Level;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone)]
    struct CapturedLogs {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    struct CapturedLogWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        fn new() -> Self {
            Self {
                bytes: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn lines(&self) -> Vec<serde_json::Value> {
            let bytes = self.bytes.lock().unwrap().clone();
            String::from_utf8(bytes)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }
    }

    impl io::Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedLogWriter {
                bytes: self.bytes.clone(),
            }
        }
    }

    static CAPTURED_LOGS: OnceLock<CapturedLogs> = OnceLock::new();

    fn captured_logs() -> CapturedLogs {
        CAPTURED_LOGS
            .get_or_init(|| {
                let captured = CapturedLogs::new();
                let subscriber = tracing_subscriber::fmt()
                    .json()
                    .with_writer(captured.clone())
                    .with_max_level(Level::INFO)
                    .finish();
                tracing::subscriber::set_global_default(subscriber).unwrap();
                tracing::callsite::rebuild_interest_cache();
                captured
            })
            .clone()
    }

    async fn start_mock(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{}/v1", addr)
    }

    async fn start_mid_stream_failure_backend() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            let _ = stream.read(&mut buf).await.unwrap();

            let chunk = b"data: {\"partial\":true}\n\n";
            let headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n{:X}\r\n",
                chunk.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(chunk).await.unwrap();
            stream.write_all(b"\r\n").await.unwrap();
            stream.shutdown().await.unwrap();
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
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: crate::resources::ResourceRegistry::default(),
            resource_policy: crate::resources::ResourceRoutingPolicy::default(),
            priority_limiter: crate::priority::PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
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
        backend_with_workloads(url, id, "default", WorkloadKind::default_proxy_workloads())
    }

    fn backend_with_workloads(
        url: String,
        id: &str,
        pool: &str,
        workloads: Vec<WorkloadKind>,
    ) -> BackendState {
        BackendState::from_config(
            BackendConfig {
                id: id.to_string(),
                url,
                node_id: None,
                pool: pool.to_string(),
                workloads,
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
    async fn upstream_header_policy_forwards_only_allowed_headers() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|headers: HeaderMap| async move {
                axum::Json(json!({
                    "content_type": headers
                        .get(CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok()),
                    "authorization": headers
                        .get(AUTHORIZATION)
                        .and_then(|v| v.to_str().ok()),
                    "openai_organization": headers
                        .get("openai-organization")
                        .and_then(|v| v.to_str().ok()),
                    "openai_project": headers
                        .get("openai-project")
                        .and_then(|v| v.to_str().ok()),
                    "anthropic_version": headers
                        .get("anthropic-version")
                        .and_then(|v| v.to_str().ok()),
                    "anthropic_beta": headers
                        .get("anthropic-beta")
                        .and_then(|v| v.to_str().ok()),
                    "google_api_key": headers
                        .get("x-goog-api-key")
                        .and_then(|v| v.to_str().ok()),
                    "fairlead_thread": headers
                        .get("x-fairlead-thread-id")
                        .and_then(|v| v.to_str().ok()),
                    "fairlead_origin": headers
                        .get("x-fairlead-origin-node")
                        .and_then(|v| v.to_str().ok()),
                    "request_id": headers
                        .get("x-request-id")
                        .and_then(|v| v.to_str().ok()),
                }))
            }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .header("content-type", "application/json; charset=utf-8")
            .header("authorization", "Bearer upstream-token")
            .header("openai-organization", "org-test")
            .header("openai-project", "project-test")
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "tools-2024-04-04")
            .header("x-goog-api-key", "google-key")
            .header("x-fairlead-thread-id", "thread-1")
            .header("x-fairlead-origin-node", "node-a")
            .header("x-fairlead-priority", "batch")
            .header("x-request-id", "request-1")
            .body(r#"{"model":"m","messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let received: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(received["content_type"], "application/json; charset=utf-8");
        assert_eq!(received["authorization"], "Bearer upstream-token");
        assert_eq!(received["openai_organization"], "org-test");
        assert_eq!(received["openai_project"], "project-test");
        assert_eq!(received["anthropic_version"], "2023-06-01");
        assert_eq!(received["anthropic_beta"], "tools-2024-04-04");
        assert_eq!(received["google_api_key"], "google-key");
        assert_eq!(received["fairlead_thread"], serde_json::Value::Null);
        assert_eq!(received["fairlead_origin"], serde_json::Value::Null);
        assert_eq!(received["request_id"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn upstream_header_policy_defaults_missing_content_type_to_json() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|headers: HeaderMap| async move {
                axum::Json(json!({
                    "content_type": headers
                        .get(CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok()),
                }))
            }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .body(r#"{"model":"m","messages":[]}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let received: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(received["content_type"], DEFAULT_UPSTREAM_CONTENT_TYPE);
    }

    #[tokio::test]
    async fn missing_priority_defaults_to_realtime_metric_label() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"default-priority"})) }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", fairlead))
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
        assert!(metrics.contains("priority=\"realtime\""));
    }

    #[tokio::test]
    async fn explicit_batch_priority_is_recorded_in_metrics() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"batch-priority"})) }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/v1/chat/completions", fairlead))
            .header("x-fairlead-priority", "batch")
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
        assert!(metrics.contains("priority=\"batch\""));
    }

    #[tokio::test]
    async fn invalid_priority_returns_400() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"unused"})) }),
        );
        let backend = start_mock(mock).await;
        let fairlead = start_fairlead(&[&backend]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .header("x-fairlead-priority", "urgent")
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 400);
        assert!(resp.text().await.unwrap().contains("X-Fairlead-Priority"));
    }

    #[tokio::test]
    async fn priority_limit_returns_429_when_bucket_is_full() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mock = Router::new().route(
            "/v1/chat/completions",
            post({
                let entered = entered.clone();
                let release = release.clone();
                move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        axum::Json(json!({"source":"slow"}))
                    }
                }
            }),
        );
        let backend = start_mock(mock).await;
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![BackendState::new(backend, 10, Duration::from_secs(60))],
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: crate::resources::ResourceRegistry::default(),
            resource_policy: crate::resources::ResourceRoutingPolicy::default(),
            priority_limiter: crate::priority::PriorityLimiter::new(1, 1, 1),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        };
        let fairlead = start_fairlead_with_state(state).await;

        let client = reqwest::Client::new();
        let first_client = client.clone();
        let first_url = format!("{}/v1/chat/completions", fairlead);
        let first = tokio::spawn(async move {
            first_client
                .post(first_url)
                .json(&json!({"model":"m","messages":[]}))
                .send()
                .await
                .unwrap()
        });

        entered.notified().await;

        let rejected = client
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), 429);
        assert_eq!(rejected.text().await.unwrap(), "priority limit reached");

        let metrics = client
            .get(format!("{}/metrics", fairlead))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(metrics.contains(
            "fairlead_requests_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"\",node=\"\",pool=\"\",origin_node=\"\",status=\"429\",outcome=\"priority_limited\"} 1"
        ));

        release.notify_one();
        assert_eq!(first.await.unwrap().status(), 200);
    }

    #[tokio::test]
    async fn priority_limit_is_held_until_response_body_completes() {
        let release = Arc::new(Notify::new());
        let mock = Router::new().route(
            "/v1/chat/completions",
            post({
                let release = release.clone();
                move || {
                    let release = release.clone();
                    async move {
                        let body = futures_util::stream::once(async move {
                            release.notified().await;
                            Ok::<_, std::convert::Infallible>(Bytes::from_static(
                                br#"{"source":"stream"}"#,
                            ))
                        });
                        Response::builder()
                            .status(200)
                            .body(Body::from_stream(body))
                            .unwrap()
                    }
                }
            }),
        );
        let backend = start_mock(mock).await;
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![BackendState::new(backend, 10, Duration::from_secs(60))],
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: crate::resources::ResourceRegistry::default(),
            resource_policy: crate::resources::ResourceRoutingPolicy::default(),
            priority_limiter: crate::priority::PriorityLimiter::new(1, 1, 1),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        };
        let fairlead = start_fairlead_with_state(state).await;

        let client = reqwest::Client::new();
        let first = client
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(first.status(), 200);

        let rejected = client
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), 429);

        release.notify_one();
        assert_eq!(first.text().await.unwrap(), r#"{"source":"stream"}"#);
    }

    #[tokio::test]
    async fn priority_limit_buckets_are_independent_through_proxy() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered_count = Arc::new(AtomicUsize::new(0));
        let mock = Router::new().route(
            "/v1/chat/completions",
            post({
                let entered = entered.clone();
                let release = release.clone();
                let entered_count = entered_count.clone();
                move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    let entered_count = entered_count.clone();
                    async move {
                        entered_count.fetch_add(1, Ordering::SeqCst);
                        entered.notify_one();
                        release.notified().await;
                        axum::Json(json!({"source":"slow"}))
                    }
                }
            }),
        );
        let backend = start_mock(mock).await;
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![BackendState::new(backend, 10, Duration::from_secs(60))],
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: crate::resources::ResourceRegistry::default(),
            resource_policy: crate::resources::ResourceRoutingPolicy::default(),
            priority_limiter: crate::priority::PriorityLimiter::new(1, 1, 1),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        };
        let fairlead = start_fairlead_with_state(state).await;

        let client = reqwest::Client::new();
        let batch_client = client.clone();
        let batch_url = format!("{}/v1/chat/completions", fairlead);
        let batch = tokio::spawn(async move {
            batch_client
                .post(batch_url)
                .header("x-fairlead-priority", "batch")
                .json(&json!({"model":"m","messages":[]}))
                .send()
                .await
                .unwrap()
        });

        entered.notified().await;

        let realtime_client = client.clone();
        let realtime_url = format!("{}/v1/chat/completions", fairlead);
        let realtime = tokio::spawn(async move {
            realtime_client
                .post(realtime_url)
                .json(&json!({"model":"m","messages":[]}))
                .send()
                .await
                .unwrap()
        });

        entered.notified().await;
        assert_eq!(entered_count.load(Ordering::SeqCst), 2);

        release.notify_waiters();
        assert_eq!(batch.await.unwrap().status(), 200);
        assert_eq!(realtime.await.unwrap().status(), 200);
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
    async fn mid_stream_failure_is_not_retried() {
        let failing_backend = start_mid_stream_failure_backend().await;
        let fallback_hit = Arc::new(AtomicBool::new(false));
        let fallback_hit_for_route = fallback_hit.clone();
        let fallback = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let hit = fallback_hit_for_route.clone();
                async move {
                    hit.store(true, Ordering::SeqCst);
                    Response::builder()
                        .status(200)
                        .header("content-type", "text/event-stream")
                        .body(Body::from("data: fallback\n\ndata: [DONE]\n\n"))
                        .unwrap()
                }
            }),
        );
        let fallback_backend = start_mock(fallback).await;
        let fairlead = start_fairlead_with_backends(vec![
            backend_with_id(failing_backend, "failing-stream"),
            backend_with_id(fallback_backend, "fallback"),
        ])
        .await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[],"stream":true}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let body = resp.text().await;
        assert!(
            body.is_err(),
            "client should see the upstream body error after streaming starts"
        );
        assert!(
            !fallback_hit.load(Ordering::SeqCst),
            "Fairlead must not retry after response bytes have started streaming"
        );
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
            "fairlead_retries_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"dead\",node=\"\",pool=\"default\",origin_node=\"\",reason=\"connection_error\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_requests_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"healthy\",node=\"\",pool=\"default\",origin_node=\"\",status=\"200\",outcome=\"retried_success\"} 1"
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
            "fairlead_retries_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"primary\",node=\"\",pool=\"default\",origin_node=\"\",reason=\"server_error\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_requests_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"secondary\",node=\"\",pool=\"default\",origin_node=\"\",status=\"200\",outcome=\"retried_success\"} 1"
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
    async fn chat_completions_skips_backend_without_chat_workload() {
        let skipped_hits = Arc::new(AtomicUsize::new(0));
        let skipped_hits_for_route = skipped_hits.clone();
        let skipped = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let hits = skipped_hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::Json(json!({"source":"wrong-workload"}))
                }
            }),
        );
        let selected = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"chat"})) }),
        );
        let skipped_url = start_mock(skipped).await;
        let selected_url = start_mock(selected).await;
        let fairlead = start_fairlead_with_backends(vec![
            backend_with_workloads(
                skipped_url,
                "embedding-only",
                "local-llm",
                vec![WorkloadKind::Embeddings],
            ),
            backend_with_workloads(
                selected_url,
                "chat",
                "local-llm",
                vec![WorkloadKind::ChatCompletions],
            ),
        ])
        .await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.json::<serde_json::Value>().await.unwrap()["source"],
            "chat"
        );
        assert_eq!(skipped_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn embeddings_skips_backend_without_embeddings_workload() {
        let skipped_hits = Arc::new(AtomicUsize::new(0));
        let skipped_hits_for_route = skipped_hits.clone();
        let skipped = Router::new().route(
            "/v1/embeddings",
            post(move || {
                let hits = skipped_hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::Json(json!({
                        "object":"list",
                        "data":[{"object":"embedding","index":0,"embedding":[9.9]}],
                        "model":"wrong-workload"
                    }))
                }
            }),
        );
        let selected = Router::new().route(
            "/v1/embeddings",
            post(|| async {
                axum::Json(json!({
                    "object":"list",
                    "data":[{"object":"embedding","index":0,"embedding":[0.2]}],
                    "model":"embedding"
                }))
            }),
        );
        let skipped_url = start_mock(skipped).await;
        let selected_url = start_mock(selected).await;
        let fairlead = start_fairlead_with_backends(vec![
            backend_with_workloads(
                skipped_url,
                "chat-only",
                "local-llm",
                vec![WorkloadKind::ChatCompletions],
            ),
            backend_with_workloads(
                selected_url,
                "embedding",
                "local-llm",
                vec![WorkloadKind::Embeddings],
            ),
        ])
        .await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/embeddings", fairlead))
            .json(&json!({"model":"m","input":"hello"}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.json::<serde_json::Value>().await.unwrap()["model"],
            "embedding"
        );
        assert_eq!(skipped_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unsupported_workload_returns_503_without_trying_backend() {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_route = hits.clone();
        let backend = Router::new().route(
            "/v1/embeddings",
            post(move || {
                let hits = hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::Json(json!({
                        "object":"list",
                        "data":[{"object":"embedding","index":0,"embedding":[9.9]}],
                        "model":"wrong-workload"
                    }))
                }
            }),
        );
        let backend_url = start_mock(backend).await;
        let fairlead = start_fairlead_with_backends(vec![backend_with_workloads(
            backend_url,
            "chat-only",
            "local-llm",
            vec![WorkloadKind::ChatCompletions],
        )])
        .await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/embeddings", fairlead))
            .json(&json!({"model":"m","input":"hello"}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.text().await.unwrap(),
            "no backends configured for workload"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0);
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
            "fairlead_requests_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"node-a-vllm\",node=\"node-a\",pool=\"local-llm\",origin_node=\"node-a\",status=\"200\",outcome=\"completed\"} 1"
        ));
        assert!(metrics.contains(
            "fairlead_request_latency_seconds_count{workload=\"chat_completions\",priority=\"realtime\",backend=\"node-a-vllm\",node=\"node-a\",pool=\"local-llm\",origin_node=\"node-a\",status=\"200\",outcome=\"completed\"} 1"
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
            "fairlead_fallbacks_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"node-b-vllm\",node=\"node-b\",pool=\"local-llm\",origin_node=\"node-a\",reason=\"origin_unavailable\"} 1"
        ));
    }

    #[tokio::test]
    async fn origin_node_falls_back_when_same_node_backend_cannot_serve_workload() {
        let node_a_hits = Arc::new(AtomicUsize::new(0));
        let node_a_hits_for_route = node_a_hits.clone();
        let node_a = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let hits = node_a_hits_for_route.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    axum::Json(json!({"source":"wrong-workload"}))
                }
            }),
        );
        let node_b = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"node-b"})) }),
        );
        let node_a_url = start_mock(node_a).await;
        let node_b_url = start_mock(node_b).await;

        let fairlead = start_fairlead_with_backends(vec![
            BackendState::from_config(
                BackendConfig {
                    id: "node-a-embed".into(),
                    url: node_a_url,
                    node_id: Some("node-a".into()),
                    pool: "local-llm".into(),
                    workloads: vec![WorkloadKind::Embeddings],
                    health_path: None,
                },
                10,
                Duration::from_secs(60),
            ),
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
        assert_eq!(node_a_hits.load(Ordering::SeqCst), 0);

        let metrics = client
            .get(format!("{}/metrics", fairlead))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(metrics.contains(
            "fairlead_fallbacks_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"node-b-vllm\",node=\"node-b\",pool=\"local-llm\",origin_node=\"node-a\",reason=\"workload_unavailable\"} 1"
        ));
    }

    #[tokio::test]
    async fn resource_aware_routing_skips_local_backend_without_headroom() {
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
        let resources = crate::resources::ResourceRegistry::default();
        resources
            .report(crate::resources::ResourceReportRequest {
                node_id: "node-a".into(),
                backend_id: Some("node-a-vllm".into()),
                total_vram_mb: 64_000,
                reserved_vram_mb: 63_500,
                current_load: Some(0.95),
            })
            .await
            .unwrap();
        resources
            .report(crate::resources::ResourceReportRequest {
                node_id: "node-b".into(),
                backend_id: Some("node-b-vllm".into()),
                total_vram_mb: 64_000,
                reserved_vram_mb: 16_000,
                current_load: Some(0.25),
            })
            .await
            .unwrap();
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![
                backend_on_node(node_a_url, "node-a"),
                backend_on_node(node_b_url, "node-b"),
            ],
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources,
            resource_policy: crate::resources::ResourceRoutingPolicy {
                enabled: true,
                chat_completions_required_vram_mb: 1024,
                embeddings_required_vram_mb: 512,
            },
            priority_limiter: crate::priority::PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        };
        let fairlead = start_fairlead_with_state(state).await;

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
            "fairlead_fallbacks_total{workload=\"chat_completions\",priority=\"realtime\",backend=\"node-b-vllm\",node=\"node-b\",pool=\"local-llm\",origin_node=\"node-a\",reason=\"resource_unavailable\"} 1"
        ));
    }

    #[tokio::test]
    async fn resource_aware_routing_prefers_lower_load_when_no_locality_or_affinity() {
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
        let resources = crate::resources::ResourceRegistry::default();
        resources
            .report(crate::resources::ResourceReportRequest {
                node_id: "node-a".into(),
                backend_id: Some("node-a-vllm".into()),
                total_vram_mb: 64_000,
                reserved_vram_mb: 16_000,
                current_load: Some(0.85),
            })
            .await
            .unwrap();
        resources
            .report(crate::resources::ResourceReportRequest {
                node_id: "node-b".into(),
                backend_id: Some("node-b-vllm".into()),
                total_vram_mb: 64_000,
                reserved_vram_mb: 20_000,
                current_load: Some(0.20),
            })
            .await
            .unwrap();
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![
                backend_on_node(node_a_url, "node-a"),
                backend_on_node(node_b_url, "node-b"),
            ],
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources,
            resource_policy: crate::resources::ResourceRoutingPolicy {
                enabled: true,
                chat_completions_required_vram_mb: 1024,
                embeddings_required_vram_mb: 512,
            },
            priority_limiter: crate::priority::PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        };
        let fairlead = start_fairlead_with_state(state).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
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
    async fn resource_aware_routing_ignores_stale_reports() {
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
        let resources = crate::resources::ResourceRegistry::new(Duration::from_millis(250));
        resources
            .report(crate::resources::ResourceReportRequest {
                node_id: "node-a".into(),
                backend_id: Some("node-a-vllm".into()),
                total_vram_mb: 64_000,
                reserved_vram_mb: 16_000,
                current_load: Some(0.25),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        resources
            .report(crate::resources::ResourceReportRequest {
                node_id: "node-b".into(),
                backend_id: Some("node-b-vllm".into()),
                total_vram_mb: 64_000,
                reserved_vram_mb: 16_000,
                current_load: Some(0.25),
            })
            .await
            .unwrap();
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![
                backend_on_node(node_a_url, "node-a"),
                backend_on_node(node_b_url, "node-b"),
            ],
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources,
            resource_policy: crate::resources::ResourceRoutingPolicy {
                enabled: true,
                chat_completions_required_vram_mb: 1024,
                embeddings_required_vram_mb: 512,
            },
            priority_limiter: crate::priority::PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        };
        let fairlead = start_fairlead_with_state(state).await;

        let resp = reqwest::Client::new()
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
    }

    #[tokio::test]
    async fn resource_aware_routing_returns_503_when_all_backends_lack_headroom() {
        let resources = crate::resources::ResourceRegistry::default();
        for node_id in ["node-a", "node-b"] {
            resources
                .report(crate::resources::ResourceReportRequest {
                    node_id: node_id.into(),
                    backend_id: Some(format!("{node_id}-vllm")),
                    total_vram_mb: 64_000,
                    reserved_vram_mb: 63_500,
                    current_load: Some(0.95),
                })
                .await
                .unwrap();
        }
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![
                backend_on_node("http://node-a:8000/v1".into(), "node-a"),
                backend_on_node("http://node-b:8000/v1".into(), "node-b"),
            ],
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources,
            resource_policy: crate::resources::ResourceRoutingPolicy {
                enabled: true,
                chat_completions_required_vram_mb: 1024,
                embeddings_required_vram_mb: 512,
            },
            priority_limiter: crate::priority::PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        };
        let fairlead = start_fairlead_with_state(state).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .header("x-fairlead-origin-node", "node-a")
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 503);
        assert!(resp
            .text()
            .await
            .unwrap()
            .contains("insufficient resources"));
    }

    #[tokio::test]
    async fn structured_tracing_fields_are_emitted_for_origin_fallback() {
        let node_b = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"node-b"})) }),
        );
        let node_b_url = start_mock(node_b).await;

        let node_a_backend = backend_on_node("http://node-a:8000/v1".into(), "node-a");
        for _ in 0..10 {
            node_a_backend.circuit.write().await.record_failure();
        }
        let state = AppState {
            client: reqwest::Client::new(),
            backends: vec![node_a_backend, backend_on_node(node_b_url, "node-b")],
            affinity: SessionAffinity::default(),
            metrics: crate::metrics::RoutingMetrics::default(),
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: crate::resources::ResourceRegistry::default(),
            resource_policy: crate::resources::ResourceRoutingPolicy::default(),
            priority_limiter: crate::priority::PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
        };
        let app = build_router(state);

        let captured = captured_logs();

        let resp = app
            .oneshot(
                Request::post("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .header("x-request-id", "trace-test-1")
                    .header("x-fairlead-origin-node", "node-a")
                    .header("x-fairlead-thread-id", "thread-trace")
                    .body(Body::from(r#"{"model":"m","messages":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let request_completed = captured
            .lines()
            .into_iter()
            .find(|event| {
                event["fields"]["message"] == "request completed"
                    && event["fields"]["request_id"] == "trace-test-1"
            })
            .expect("expected request completed trace event");
        let fields = &request_completed["fields"];

        assert_eq!(fields["request_id"], "trace-test-1");
        assert_eq!(fields["workload"], "chat_completions");
        assert_eq!(fields["origin_node"], "node-a");
        assert_eq!(fields["affinity_key"], "chat_completions:thread-trace");
        assert_eq!(fields["selected_backend"], "node-b-vllm");
        assert_eq!(fields["retry_count"], 0);
        assert_eq!(fields["fallback_reason"], "origin_unavailable");
        assert_eq!(fields["status"], 200);
        assert_eq!(fields["outcome"], "completed");
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
            callback_policy: crate::callbacks::CallbackPolicy::default(),
            callback_dispatcher: crate::callbacks::CallbackDispatcher::default(),
            resources: crate::resources::ResourceRegistry::default(),
            resource_policy: crate::resources::ResourceRoutingPolicy::default(),
            priority_limiter: crate::priority::PriorityLimiter::default(),
            jobs: crate::jobs::JobRegistry::default(),
            workers: crate::workers::WorkerRegistry::default(),
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

    #[test]
    fn affinity_key_is_scoped_by_workload() {
        assert_eq!(
            affinity_key(WorkloadKind::ChatCompletions, "thread-1"),
            "chat_completions:thread-1"
        );
        assert_eq!(
            affinity_key(WorkloadKind::Embeddings, "thread-1"),
            "embeddings:thread-1"
        );
    }

    #[tokio::test]
    async fn affinity_is_scoped_per_workload_for_same_thread_id() {
        let chat_a = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"chat-a"})) }),
        );
        let embed_a = Router::new().route(
            "/v1/embeddings",
            post(|| async {
                axum::Json(json!({
                    "object":"list",
                    "data":[{"object":"embedding","index":0,"embedding":[0.1]}],
                    "model":"embed-a"
                }))
            }),
        );
        let chat_b = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source":"chat-b"})) }),
        );
        let embed_b = Router::new().route(
            "/v1/embeddings",
            post(|| async {
                axum::Json(json!({
                    "object":"list",
                    "data":[{"object":"embedding","index":0,"embedding":[0.2]}],
                    "model":"embed-b"
                }))
            }),
        );

        let chat_a_url = start_mock(chat_a).await;
        let embed_a_url = start_mock(embed_a).await;
        let chat_b_url = start_mock(chat_b).await;
        let embed_b_url = start_mock(embed_b).await;

        let chat_a_backend = BackendState::from_config(
            BackendConfig {
                id: "chat-a".into(),
                url: chat_a_url,
                node_id: None,
                pool: "local-llm".into(),
                workloads: vec![WorkloadKind::ChatCompletions],
                health_path: None,
            },
            1,
            Duration::from_secs(60),
        );
        let chat_a_handle = chat_a_backend.clone();
        chat_a_handle.circuit.write().await.record_failure();

        let embed_a_backend = BackendState::from_config(
            BackendConfig {
                id: "embed-a".into(),
                url: embed_a_url,
                node_id: None,
                pool: "embedding".into(),
                workloads: vec![WorkloadKind::Embeddings],
                health_path: None,
            },
            10,
            Duration::from_secs(60),
        );
        let chat_b_backend = BackendState::from_config(
            BackendConfig {
                id: "chat-b".into(),
                url: chat_b_url,
                node_id: None,
                pool: "local-llm".into(),
                workloads: vec![WorkloadKind::ChatCompletions],
                health_path: None,
            },
            10,
            Duration::from_secs(60),
        );
        let embed_b_backend = BackendState::from_config(
            BackendConfig {
                id: "embed-b".into(),
                url: embed_b_url,
                node_id: None,
                pool: "embedding".into(),
                workloads: vec![WorkloadKind::Embeddings],
                health_path: None,
            },
            10,
            Duration::from_secs(60),
        );

        let fairlead = start_fairlead_with_backends(vec![
            chat_a_backend,
            embed_a_backend,
            chat_b_backend,
            embed_b_backend,
        ])
        .await;
        let client = reqwest::Client::new();
        let chat_url = format!("{}/v1/chat/completions", fairlead);
        let embed_url = format!("{}/v1/embeddings", fairlead);

        let chat_first = client
            .post(&chat_url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            chat_first.json::<serde_json::Value>().await.unwrap()["source"],
            "chat-b"
        );

        let embed_first = client
            .post(&embed_url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&json!({"model":"m","input":"hello"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            embed_first.json::<serde_json::Value>().await.unwrap()["model"],
            "embed-a"
        );

        chat_a_handle.circuit.write().await.record_success();

        let chat_second = client
            .post(&chat_url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            chat_second.json::<serde_json::Value>().await.unwrap()["source"],
            "chat-b",
            "chat should keep its own workload-scoped affinity"
        );

        let embed_second = client
            .post(&embed_url)
            .header("x-fairlead-thread-id", "thread-1")
            .json(&json!({"model":"m","input":"hello"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            embed_second.json::<serde_json::Value>().await.unwrap()["model"],
            "embed-a",
            "embeddings should not inherit chat affinity"
        );
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
