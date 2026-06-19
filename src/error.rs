use axum::{http::StatusCode, response::{IntoResponse, Response}};
use thiserror::Error;

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "variants wired into routing handlers from Phase 3 onward")
)]
#[derive(Debug, Error)]
pub enum FairleadError {
    #[error("no backend available (all circuits open or insufficient VRAM)")]
    NoBackend,

    #[error("job type '{0}' has no registered worker")]
    NoWorker(String),

    #[error("backend returned an error: {0}")]
    Backend(String),

    #[error("configuration error: {0}")]
    Config(String),
}

/// Convert domain errors into HTTP responses so handlers can use `?`.
impl IntoResponse for FairleadError {
    fn into_response(self) -> Response {
        let status = match &self {
            FairleadError::NoBackend        => StatusCode::SERVICE_UNAVAILABLE,
            FairleadError::NoWorker(_)      => StatusCode::BAD_REQUEST,
            FairleadError::Backend(_)       => StatusCode::BAD_GATEWAY,
            FairleadError::Config(_)        => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_backend_maps_to_503() {
        let response = FairleadError::NoBackend.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn no_worker_maps_to_400() {
        let response = FairleadError::NoWorker("vision_analysis".into()).into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn backend_error_maps_to_502() {
        let response = FairleadError::Backend("connection refused".into()).into_response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn config_error_maps_to_500() {
        let response = FairleadError::Config("missing key".into()).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn error_message_appears_in_body() {
        use http_body_util::BodyExt;
        // Verify the error text is forwarded so callers can read the reason.
        let response = FairleadError::NoWorker("embed_batch".into()).into_response();
        let bytes = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(response.into_body().collect())
            .unwrap()
            .to_bytes();
        let body = std::str::from_utf8(&bytes).unwrap();
        assert!(body.contains("embed_batch"));
    }
}
