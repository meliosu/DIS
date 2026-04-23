use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RequestStatus {
    InProgress,
    Ready,
    Error,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrackHashRequest {
    pub hash: String,
    pub max_length: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrackHashResponse {
    pub request_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrackStatusRequest {
    pub request_id: Uuid,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrackStatusResponse {
    pub status: RequestStatus,
    pub progress: u8,
    pub data: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrackTaskMessage {
    pub request_id: Uuid,
    pub task_id: Uuid,
    pub hash: String,
    pub max_length: usize,
    pub alphabet: String,
    pub start_index: u64,
    pub end_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum WorkerTaskUpdateMessage {
    Progress {
        request_id: Uuid,
        task_id: Uuid,
        processed: u64,
        total: u64,
    },
    Finished {
        request_id: Uuid,
        task_id: Uuid,
        processed: u64,
        total: u64,
        matches: Vec<String>,
        error: Option<String>,
    },
}
