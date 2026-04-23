use mongodb::Collection;
use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct AppState {
    pub requests: Collection<RequestDocument>,
    pub tasks: Collection<TaskDocument>,
    pub rabbit_addr: String,
    pub alphabet: String,
    pub fallback_workers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestDocument {
    #[serde(rename = "_id")]
    pub id: String,
    pub hash: String,
    pub max_length: i32,
    pub status: String,
    pub progress: i32,
    pub data: Vec<String>,
    pub total_tasks: i32,
    pub completed_tasks: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDocument {
    #[serde(rename = "_id")]
    pub id: String,
    pub request_id: String,
    pub hash: String,
    pub max_length: i32,
    pub alphabet: String,
    pub start_index: i64,
    pub end_index: i64,
    #[serde(default)]
    pub total_candidates: i64,
    #[serde(default)]
    pub processed_candidates: i64,
    pub status: String,
    pub matches: Vec<String>,
    pub last_error: Option<String>,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[derive(Debug)]
pub struct ApiError {
    status: axum::http::StatusCode,
    message: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = axum::Json(ErrorResponse {
            error: self.message,
        });

        (self.status, body).into_response()
    }
}
