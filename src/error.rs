use axum::{http::StatusCode, response::{IntoResponse, Response}};
use thiserror::Error;

#[allow(dead_code)] // variants used from Phase 3 onward when routing is live
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
