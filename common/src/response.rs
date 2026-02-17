use axum::{http::StatusCode, Json};
use serde::{Serialize, Deserialize};

pub struct ErrorResponse {
    code: StatusCode,
    error: String,
}

impl ErrorResponse {
    pub fn new<S: Into<String>>(code: StatusCode, message: S) -> Self {
        Self {
            code,
            error: message.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ErrorMessage {
    pub error: String,
}

impl axum::response::IntoResponse for ErrorResponse {
    fn into_response(self) -> axum::response::Response {
        (self.code, Json(ErrorMessage { error: self.error })).into_response()
    }
}

impl From<anyhow::Error> for ErrorResponse {
    fn from(value: anyhow::Error) -> Self {
        Self {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            error: value.to_string(),
        }
    }
}
