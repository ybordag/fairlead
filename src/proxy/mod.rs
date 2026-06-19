pub mod types;

use axum::{
    body::Body,
    extract::State,
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use crate::AppState;

pub async fn chat_completions(State(state): State<AppState>, body: Bytes) -> Response {
    forward(&state, "chat/completions", body).await
}

pub async fn embeddings(State(state): State<AppState>, body: Bytes) -> Response {
    forward(&state, "embeddings", body).await
}

async fn forward(state: &AppState, path: &str, body: Bytes) -> Response {
    let Some(backend) = state.backends.first() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "no backends configured").into_response();
    };

    // Reject immediately if the circuit is open (don't touch the backend at all).
    if !backend.circuit.write().await.is_available() {
        return (StatusCode::SERVICE_UNAVAILABLE, "circuit open").into_response();
    }

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

    // 5xx → backend is broken; 2xx/3xx/4xx → backend is alive.
    if status.is_server_error() {
        backend.circuit.write().await.record_failure();
    } else {
        backend.circuit.write().await.record_success();
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
    use crate::{build_router, router::BackendState};
    use axum::{http::StatusCode, routing::post, Router};
    use serde_json::json;
    use std::{
        sync::{atomic::{AtomicBool, Ordering}, Arc},
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

    /// Start Fairlead with the given raw backend URLs, using high circuit thresholds
    /// so existing proxy tests don't accidentally trip the circuit.
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
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "Hello!"},
                "finish_reason": "stop"
            }]
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
        assert!(
            ct.contains("text/event-stream"),
            "expected SSE content-type, got {ct}"
        );
        let text = resp.text().await.unwrap();
        assert!(text.contains("data: [DONE]"));
    }

    #[tokio::test]
    async fn backend_unreachable_returns_502() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let backend = format!("http://{}/v1", addr);
        let fairlead = start_fairlead(&[&backend]).await;

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
            "object": "list",
            "data": [{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}],
            "model": "text-embedding-ada-002"
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

            assert_eq!(
                resp.status().as_u16(),
                status_code,
                "expected backend status {status_code} to be forwarded unchanged"
            );
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

        let payload = json!({
            "model": "test-model",
            "messages": [{"role": "user", "content": "verbatim check"}],
            "temperature": 0.7,
            "max_tokens": 256
        });

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&payload)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let echoed: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(echoed["model"], "test-model");
        assert_eq!(echoed["messages"][0]["content"], "verbatim check");
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

    #[tokio::test]
    async fn uses_first_backend_when_multiple_configured() {
        let first = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source": "first"})) }),
        );
        let second = Router::new().route(
            "/v1/chat/completions",
            post(|| async { axum::Json(json!({"source": "second"})) }),
        );
        let first_url = start_mock(first).await;
        let second_url = start_mock(second).await;
        let fairlead = start_fairlead(&[&first_url, &second_url]).await;

        let resp = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", fairlead))
            .json(&json!({"model":"m","messages":[]}))
            .send()
            .await
            .unwrap();

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["source"], "first");
    }

    // ── circuit breaker integration ──────────────────────────────────────────

    /// After `failure_threshold` consecutive 5xx responses the circuit opens
    /// and subsequent requests get 503 without touching the backend.
    #[tokio::test]
    async fn circuit_opens_on_repeated_5xx() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let backend_url = start_mock(mock).await;

        // threshold=2 so two 5xx responses trip the circuit.
        let backend = BackendState::new(backend_url, 2, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        // Request 1 & 2: forwarded, both 500, circuit trips on the second.
        let r1 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r1.status(), 500);
        let r2 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r2.status(), 500);

        // Request 3: circuit is open → 503 immediately, backend never called.
        let r3 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r3.status(), 503);
    }

    /// 4xx responses from the backend must NOT trip the circuit — the backend
    /// is alive and functioning; the request was simply invalid.
    #[tokio::test]
    async fn circuit_stays_closed_on_4xx() {
        let mock = Router::new().route(
            "/v1/chat/completions",
            post(|| async { StatusCode::BAD_REQUEST }),
        );
        let backend_url = start_mock(mock).await;

        // threshold=1 — one failure would trip immediately.
        let backend = BackendState::new(backend_url, 1, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        // Three 400s in a row — circuit should stay closed.
        for _ in 0..3 {
            let r = client.post(&url).json(&body).send().await.unwrap();
            assert_eq!(r.status(), 400, "4xx must be forwarded, circuit must stay closed");
        }
    }

    /// Connection errors (502 path) must also trip the circuit — not just 5xx responses.
    #[tokio::test]
    async fn connection_failure_trips_circuit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // nothing listening on this port

        let backend_url = format!("http://{}/v1", addr);
        // threshold=2: two connection failures should open the circuit.
        let backend = BackendState::new(backend_url, 2, Duration::from_secs(60));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        let r1 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r1.status(), 502, "first connection failure → 502");

        let r2 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r2.status(), 502, "second connection failure → 502");

        // Circuit is now open — should return 503 without attempting the backend.
        let r3 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r3.status(), 503, "circuit should be open after two connection failures");
    }

    /// If the half-open probe also fails the circuit must re-open, not stay
    /// half-open or close.
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

        // Trip the circuit.
        let r1 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r1.status(), 500);

        let r2 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r2.status(), 503, "circuit should be open");

        // Wait for cooldown → half-open.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Half-open probe reaches backend, still gets 500 → circuit re-opens.
        let r3 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r3.status(), 500, "half-open probe should reach backend");

        let r4 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r4.status(), 503, "circuit should be open again after half-open failure");
    }

    /// After the cooldown the circuit enters Half-open, one successful request
    /// closes it, and subsequent requests flow normally again.
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
                        axum::Json(json!({"recovered": true})).into_response()
                    }
                }
            }),
        );
        let backend_url = start_mock(mock).await;

        // threshold=1, cooldown=50ms for a fast test.
        let backend = BackendState::new(backend_url, 1, Duration::from_millis(50));
        let fairlead = start_fairlead_with_backends(vec![backend]).await;

        let client = reqwest::Client::new();
        let url = format!("{}/v1/chat/completions", fairlead);
        let body = json!({"model":"m","messages":[]});

        // Trip the circuit.
        let r1 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r1.status(), 500, "first request should reach backend and return 500");

        // Circuit is open — blocked immediately.
        let r2 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r2.status(), 503, "circuit should be open");

        // Wait for cooldown, then flip backend to healthy.
        tokio::time::sleep(Duration::from_millis(100)).await;
        should_fail.store(false, Ordering::SeqCst);

        // First request after cooldown: circuit enters Half-open, succeeds, closes.
        let r3 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r3.status(), 200, "half-open probe should succeed and close the circuit");

        // Circuit is closed again — normal operation.
        let r4 = client.post(&url).json(&body).send().await.unwrap();
        assert_eq!(r4.status(), 200);
        let payload: serde_json::Value = r4.json().await.unwrap();
        assert_eq!(payload["recovered"], true);
    }
}
