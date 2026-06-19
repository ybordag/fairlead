use axum::{extract::State, response::Response};

use crate::{router::CircuitState, AppState};

pub async fn metrics(State(state): State<AppState>) -> Response<String> {
    let mut body = String::from(
        "# HELP fairlead_circuit_state Circuit state per backend (0=closed 1=half_open 2=open)\n\
         # TYPE fairlead_circuit_state gauge\n",
    );

    for backend in &state.backends {
        let value: u8 = {
            let guard = backend.circuit.read().await;
            match guard.state() {
                CircuitState::Closed => 0,
                CircuitState::HalfOpen => 1,
                CircuitState::Open { .. } => 2,
            }
        };
        // Escape any quotes in the URL so the Prometheus label is valid.
        let label = backend.url.replace('"', "\\\"");
        body.push_str(&format!(
            "fairlead_circuit_state{{backend=\"{label}\"}} {value}\n"
        ));
    }

    Response::builder()
        .status(200)
        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
        .body(body)
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::BackendState;
    use axum::{body::Body, routing::get, Router};
    use http_body_util::BodyExt;
    use std::time::Duration;
    use tower::ServiceExt;

    fn router_with_backends(backends: Vec<BackendState>) -> Router {
        let state = AppState {
            client: reqwest::Client::new(),
            backends,
            affinity: crate::router::SessionAffinity::default(),
        };
        Router::new()
            .route("/metrics", get(metrics))
            .with_state(state)
    }

    async fn body_text(resp: axum::response::Response) -> String {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn metrics_reports_closed_for_healthy_backend() {
        let backends = vec![BackendState::new(
            "http://loki:8000/v1".into(),
            3,
            Duration::from_secs(30),
        )];
        let app = router_with_backends(backends);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let text = body_text(resp).await;
        assert!(text.contains("fairlead_circuit_state{backend=\"http://loki:8000/v1\"} 0"));
    }

    #[tokio::test]
    async fn metrics_reports_open_after_failures() {
        let backend = BackendState::new("http://thor:8000/v1".into(), 1, Duration::from_secs(30));
        // Trip the circuit manually.
        backend.circuit.write().await.record_failure();

        let app = router_with_backends(vec![backend]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(text.contains("fairlead_circuit_state{backend=\"http://thor:8000/v1\"} 2"));
    }

    #[tokio::test]
    async fn metrics_reports_half_open() {
        let backend =
            BackendState::new("http://loki:8000/v1".into(), 1, Duration::from_millis(10));
        backend.circuit.write().await.record_failure();
        tokio::time::sleep(Duration::from_millis(20)).await;
        // is_available() transitions Open → HalfOpen once the cooldown has elapsed.
        backend.circuit.write().await.is_available();

        let app = router_with_backends(vec![backend]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(
            text.contains("fairlead_circuit_state{backend=\"http://loki:8000/v1\"} 1"),
            "HalfOpen should report value 1, got:\n{text}"
        );
    }

    #[tokio::test]
    async fn metrics_reports_multiple_backends() {
        let healthy =
            BackendState::new("http://loki:8000/v1".into(), 3, Duration::from_secs(30));
        let broken =
            BackendState::new("http://thor:8000/v1".into(), 1, Duration::from_secs(30));
        broken.circuit.write().await.record_failure();

        let app = router_with_backends(vec![healthy, broken]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let text = body_text(resp).await;
        assert!(text.contains("fairlead_circuit_state{backend=\"http://loki:8000/v1\"} 0"));
        assert!(text.contains("fairlead_circuit_state{backend=\"http://thor:8000/v1\"} 2"));
    }

    #[tokio::test]
    async fn metrics_empty_when_no_backends() {
        let app = router_with_backends(vec![]);
        let resp = app
            .oneshot(
                axum::http::Request::get("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let text = body_text(resp).await;
        assert!(text.contains("# HELP fairlead_circuit_state"));
        assert!(!text.contains("fairlead_circuit_state{"));
    }
}
