use serde::{Serialize, Deserialize};

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum CrackStatus {
    Ready,
    InProgress,
    Error,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CrackRequest {
    pub hash: String,
    pub max_length: usize,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CrackResponse {
    pub request_id: uuid::Uuid,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatusRequest {
    pub request_id: uuid::Uuid,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StatusResponse {
    pub status: CrackStatus,
    pub progress: usize,
    pub data: Option<Vec<String>>,
}
