use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskRequest {
    pub hash: String,
    pub request_id: uuid::Uuid,
    pub alphabet: String,
    pub max_length: usize,
    pub start: usize,
    pub end: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterRequest {
    pub worker_address: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTaskQuery {
    pub request_id: uuid::Uuid,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTaskRequest {
    pub segment_start: usize,
    pub segment_end: usize,
    pub data: Vec<String>,
}

