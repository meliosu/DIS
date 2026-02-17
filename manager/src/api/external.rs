use axum::extract::{Json, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use common::response::ErrorResponse;
use common::types::CreateTaskRequest;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::CONFIG;
use crate::state::{Crack, CrackWorker};
use crate::types::{CrackRequest, CrackResponse, CrackStatus, StatusRequest, StatusResponse};

pub fn router(state: crate::state::State) -> axum::Router {
    axum::Router::new()
        .route("/api/hash/crack", post(crack_hash))
        .route("/api/hash/status", get(get_crack_status))
        .with_state(state)
}

async fn crack_hash(
    State(state): State<crate::state::State>,
    Json(r): Json<CrackRequest>,
) -> Result<Json<CrackResponse>, ErrorResponse> {
    if r.max_length == 0 {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "maxLength should be greater than 0",
        ));
    }

    if r.hash.len() != 32 || !r.hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "`hash` is not a valid md5 hash",
        ));
    }

    let alphabet_size = CONFIG.alphabet.len();
    let total_count =
        alphabet_size * (alphabet_size.pow(r.max_length as u32) - 1) / (alphabet_size - 1);

    let workers = {
        let workers = state.workers.lock().await;
        workers.clone()
    };

    let mut healthy_workers = Vec::new();

    for worker in workers {
        match worker.client.healthcheck().await {
            Ok(_) => {
                healthy_workers.push(worker);
            }

            Err(_) => {
                log::error!("worker {} failed healthcheck", worker.address);
            }
        }
    }

    if healthy_workers.is_empty() {
        return Err(ErrorResponse::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "there are no workers available",
        ));
    }

    {
        let mut workers = state.workers.lock().await;
        *workers = healthy_workers.clone();
    }

    let request_id = Uuid::new_v4();

    log::info!(
        "Request {}: hash {}, max. length {}, search space {}",
        request_id,
        r.hash,
        r.max_length,
        total_count
    );

    let mut crack_workers = Vec::new();

    for (i, worker) in healthy_workers.iter().enumerate() {
        let remainder = total_count % healthy_workers.len();
        let base = total_count / healthy_workers.len();
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
            alphabet: CONFIG.alphabet.clone(),
            max_length: r.max_length,
            start,
            end,
        };

        log::info!(
            "Request {}: giving search range {}-{} to worker {}",
            request_id,
            start,
            end,
            worker.address
        );

        worker.client.create_task(&create_task_request).await?;
    }

    let crack = Crack {
        hash: r.hash.clone(),
        alphabet: CONFIG.alphabet.clone(),
        max_length: r.max_length,
        total_count,
        data: Vec::new(),
        workers: crack_workers,
    };

    let mut requests = state.requests.lock().await;
    requests.insert(request_id.clone(), Arc::new(Mutex::new(crack)));

    Ok(Json(CrackResponse { request_id }))
}

async fn get_crack_status(
    State(state): State<crate::state::State>,
    Query(r): Query<StatusRequest>,
) -> Result<Json<StatusResponse>, ErrorResponse> {
    let crack = {
        let requests = state.requests.lock().await;

        requests
            .get(&r.request_id)
            .cloned()
            .ok_or(ErrorResponse::new(
                StatusCode::NOT_FOUND,
                format!("request with id {} doesn't exist", r.request_id),
            ))?
    };

    let crack = crack.lock().await;

    let left: usize = crack.workers.iter().map(|w| w.end - w.curr).sum();
    let done = crack.total_count - left;

    let has_alive_workers = crack.workers.iter().any(|w| w.curr < w.end);

    let status = if done == crack.total_count {
        CrackStatus::Ready
    } else if !has_alive_workers {
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

    Ok(Json(response))
}
