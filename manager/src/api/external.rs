use axum::routing::{get, post};
use axum::extract::{Json, Query, State};
use common::response::ErrorResponse;
use axum::http::StatusCode;
use common::types::CreateTaskRequest;
use uuid::Uuid;

use crate::constants::{ALPHABET, TIMEOUT};
use crate::state::{Crack, CrackWorker};
use crate::types::{CrackRequest, CrackResponse, CrackStatus, StatusRequest, StatusResponse};

pub fn router(state: crate::state::State) -> axum::Router {
    axum::Router::new()
        .route("/api/hash/crack", post(crack_hash))
        .route("/api/hash/status", get(get_crack_status))
        .with_state(state)
}

async fn crack_hash(State(state): State<crate::state::State>, Json(r): Json<CrackRequest>) -> Result<Json<CrackResponse>, ErrorResponse> {
    if r.max_length == 0 {
        return Err(ErrorResponse::new(StatusCode::BAD_REQUEST, "maxLength should be greater than 0"));
    }

    let alphabet_size = ALPHABET.len();
    let total_count = alphabet_size * (alphabet_size.pow(r.max_length as u32) - 1) / (alphabet_size - 1);

    let mut workers = state.workers.lock().await;

    for worker in std::mem::take(&mut *workers) {
        match worker.client.healthcheck().await {
            Ok(_) => {
                workers.push(worker);
            }

            Err(e) => {
                log::error!("worker {} failed healthcheck: {}", worker.address, e);
            }
        }
    }

    if workers.is_empty() {
        return Err(ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, "there are no workers available"));
    }

    let request_id = Uuid::new_v4();
    let mut crack_workers = Vec::new();

    for (i, worker) in workers.iter().enumerate() {
        let remainder = total_count % workers.len();
        let base = total_count / workers.len();
        let extra = if i < remainder { 1 } else { 0 };

        let start = i * base + i.min(remainder);
        let end = start + base + extra;

        if start == end {
            continue;
        }

        let crack_worker = CrackWorker {
            last_update: tokio::time::Instant::now(),
            worker: worker.clone(),
            start,
            end,
            curr: start,
        };

        crack_workers.push(crack_worker);

        let create_task_request = CreateTaskRequest {
            hash: r.hash.clone(),
            request_id,
            alphabet: ALPHABET.to_string(),
            max_length: r.max_length,
            start,
            end,
        };

        log::info!("request: {create_task_request:?}");

        worker.client.create_task(&create_task_request).await?;
    }

    let crack = Crack {
        alphabet: ALPHABET.to_string(),
        max_length: r.max_length,
        total_count,
        data: Vec::new(),
        workers: crack_workers,
    };

    let mut requests = state.requests.lock().await;
    requests.insert(request_id.clone(), crack);
    drop(requests);

    drop(workers);

    Ok(Json(CrackResponse { request_id }))
}

async fn get_crack_status(State(state): State<crate::state::State>, Query(r): Query<StatusRequest>) -> Result<Json<StatusResponse>, ErrorResponse> {
    let requests = state.requests.lock().await;

    let crack = requests.get(&r.request_id).ok_or(ErrorResponse::new(StatusCode::NOT_FOUND, format!("request with id {} doesn't exist", r.request_id)))?;

    let now = tokio::time::Instant::now();
    let mut done = 0;
    let mut is_timeout = false;

    for worker in &crack.workers {
        done += worker.curr - worker.start;

        if worker.curr < worker.end && now - worker.last_update >= TIMEOUT {
            is_timeout = true;
        }
    }

    let status = if done == crack.total_count {
        CrackStatus::Ready
    } else if is_timeout {
        CrackStatus::Error
    } else {
        CrackStatus::InProgress
    };

    let progress = done * 100 / crack.total_count;

    let data = if crack.data.is_empty() {
        None
    } else {
        Some(crack.data.clone())
    };

    let response = StatusResponse {
        status,
        progress,
        data,
    };

    drop(requests);

    Ok(Json(response))
}
