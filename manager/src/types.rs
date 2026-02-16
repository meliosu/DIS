use serde::{Serialize, Deserialize};

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum CrackStatus {
    Ready,
    InProgress,
    Error,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CrackRequest {
    hash: String,
    max_length: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CrackResponse {
    request_id: uuid::Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatusRequest {
    request_id: uuid::Uuid,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatusResponse {
    status: CrackStatus,
    progress: usize,
    data: Option<Vec<String>>,
}
