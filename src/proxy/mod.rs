pub mod types;

use axum::{
    body::Body,
    extract::State,
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use crate::{router::select_backend, AppState};

pub async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(&state, "chat/completions", &headers, body).await
}

pub async fn embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward(&state, "embeddings", &headers, body).await
}

async fn forward(state: &AppState, path: &str, headers: &HeaderMap, body: Bytes) -> Response {
    if state.backends.is_empty() {
        return (StatusCode::SERVICE_UNAVAILABLE, "no backends configured").into_response();
    }

    // Extract optional thread ID for session affinity.
    let thread_id = headers
        .get("x-fairlead-thread-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    // Resolve preferred backend index (if any) then run the fallback chain.
    let preferred = match thread_id {
        Some(ref tid) => state.affinity.preferred(tid).await,
        None => None,
    };

    let Some(idx) = select_backend(&state.backends, preferred).await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "all backends unavailable (circuits open)",
        )
            .into_response();
    };

    let backend = &state.backends[idx];
    let url = format!("{}/{}", backend.url.trim_end_matches('/'), path);

    let upstream = match state
        .client
        .post(&url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => {
            backend.circuit.write().await.record_failure();
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    let status = upstream.status();

    if status.is_server_error() {
        backend.circuit.write().await.record_failure();
    } else {
        backend.circuit.write().await.record_success();
        // Update affinity so the next request from this thread prefers the
        // same backend — including after a fallback re-route.
        if let Some(ref tid) = thread_id {
            state.affinity.record(tid, idx).await;
        }
    }

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
    use crate::{build_router, router::BackendState, router::SessionAffinity};
    use axum::{http::StatusCode, routing::post, Router};
    use serde_json::json;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    async fn start_mock(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
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
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, build_router(state)).await.unwrap();
        });
        format!("http://{}", addr)
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

        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 500);
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 500);
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 503);
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
            assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 400);
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

        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 502);
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 502);
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 503);
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

        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 500);
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 503);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 500, "half-open probe reached backend");
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 503, "circuit re-opened");
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

        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 500);
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 503);
        tokio::time::sleep(Duration::from_millis(100)).await;
        should_fail.store(false, Ordering::SeqCst);
        assert_eq!(client.post(&url).json(&body).send().await.unwrap().status(), 200);
        let r4 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r4.status(), 200);
        assert_eq!(r4.json::<serde_json::Value>().await.unwrap()["recovered"], true);
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
        assert_eq!(resp.json::<serde_json::Value>().await.unwrap()["source"], "second");
    }

    /// When all circuits are open, return 503 without touching any backend.
    #[tokio::test]
    async fn all_backends_open_returns_503() {
        let first_backend = BackendState::new("http://a:8000/v1".into(), 1, Duration::from_secs(60));
        let second_backend = BackendState::new("http://b:8000/v1".into(), 1, Duration::from_secs(60));
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

        let fairlead =
            start_fairlead_with_backends(vec![first_backend, second_backend]).await;

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
        assert_eq!(r1.json::<serde_json::Value>().await.unwrap()["source"], "second");

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
        assert_eq!(r2.json::<serde_json::Value>().await.unwrap()["source"], "second", "affinity should keep thread on backend 1");

        // Request with thread-2 (no affinity) → goes to backend 0 (first available).
        let r3 = client
            .post(&url)
            .header("x-fairlead-thread-id", "thread-2")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r3.json::<serde_json::Value>().await.unwrap()["source"], "first");
    }

    /// Affinity is only updated on success, never on failure.  When a preferred
    /// backend starts returning 5xx the thread keeps hitting it (accumulating
    /// failures) until the circuit opens, at which point the fallback chain
    /// takes over and the affinity map is updated to the new backend.
    #[tokio::test]
    async fn affinity_follows_circuit_after_5xx_degradation() {
        let a_failing = Arc::new(AtomicBool::new(false));
        let af = a_failing.clone();

        let backend_a = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let flag = af.clone();
                async move {
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
        let r1 = client.post(&url).header("x-fairlead-thread-id", "thread-1").json(&body).send().await.unwrap();
        assert_eq!(r1.json::<serde_json::Value>().await.unwrap()["source"], "a");

        // Step 2: A starts failing.
        a_failing.store(true, Ordering::SeqCst);

        // Steps 3–4: two 5xx responses — circuit not open yet (1/2 then 2/2).
        // Affinity is NOT updated because the requests did not succeed.
        let r2 = client.post(&url).header("x-fairlead-thread-id", "thread-1").json(&body).send().await.unwrap();
        assert_eq!(r2.status(), 500, "A degrading, circuit at 1/2");

        let r3 = client.post(&url).header("x-fairlead-thread-id", "thread-1").json(&body).send().await.unwrap();
        assert_eq!(r3.status(), 500, "A degrading, circuit now opens at 2/2");

        // Step 5: A's circuit is open. select_backend skips A, routes to B,
        // succeeds, and updates affinity to thread-1 → B (index 1).
        let r4 = client.post(&url).header("x-fairlead-thread-id", "thread-1").json(&body).send().await.unwrap();
        assert_eq!(r4.status(), 200);
        assert_eq!(r4.json::<serde_json::Value>().await.unwrap()["source"], "b");

        // Step 6: Affinity now points to B — thread stays on B even if A recovers.
        let r5 = client.post(&url).header("x-fairlead-thread-id", "thread-1").json(&body).send().await.unwrap();
        assert_eq!(r5.json::<serde_json::Value>().await.unwrap()["source"], "b", "affinity updated to B after fallback");
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

        let fairlead =
            start_fairlead_with_backends(vec![first_backend, second_backend]).await;

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
        assert_eq!(r1.json::<serde_json::Value>().await.unwrap()["source"], "second");

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
        assert_eq!(r2.json::<serde_json::Value>().await.unwrap()["source"], "first", "should fall back to backend 0");

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
        assert_eq!(r3.json::<serde_json::Value>().await.unwrap()["source"], "first", "affinity should have updated to backend 0");
    }
}
