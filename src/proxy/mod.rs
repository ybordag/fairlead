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

    let url = format!("{}/{}", backend.trim_end_matches('/'), path);

    let upstream = match state
        .client
        .post(&url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };

    let status = upstream.status();
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
    use crate::build_router;
    use axum::{http::StatusCode, routing::post, Router};
    use serde_json::json;

    /// Start a mock backend Axum app. Returns its base URL including the `/v1` prefix
    /// so callers can append paths like `/v1/chat/completions` directly.
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

    /// Start a Fairlead instance configured with the given backend URLs.
    /// Pass an empty slice to test the no-backends code path.
    async fn start_fairlead(backends: &[&str]) -> String {
        let state = AppState {
            client: reqwest::Client::new(),
            backends: backends.iter().map(|s| s.to_string()).collect(),
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

    // ── existing coverage ────────────────────────────────────────────────────

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
        // Grab a free port then release it — nothing will be listening there.
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

    // ── gap coverage ─────────────────────────────────────────────────────────

    /// Backend error codes (400, 429, 500) must pass through unchanged.
    /// Fairlead is a transparent proxy — it must not swallow or remap upstream errors.
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

    /// Extra fields beyond the typed structs (temperature, max_tokens, etc.) must
    /// reach the backend verbatim. Fairlead forwards raw bytes, not re-serialized structs.
    #[tokio::test]
    async fn request_body_forwarded_verbatim() {
        // Echo handler: reflect whatever bytes arrived back to the caller.
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

    /// The embeddings endpoint must also return 503 when no backends are configured.
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

    /// When multiple backends are configured, requests go to the first one.
    /// Phase 4 will add circuit-breaker-aware selection; for now first-wins is correct.
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
}
