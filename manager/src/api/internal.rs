use axum::extract::{Json, Query, State};
use axum::routing::{patch, post};

use common::constants::{MANAGER_REGISTER_PATH, MANAGER_UPDATE_TASK_PATH};
use common::response::ErrorResponse;
use common::types::{RegisterRequest, UpdateTaskQuery, UpdateTaskRequest};
use reqwest::StatusCode;

use crate::state::Worker;

pub fn router(state: crate::state::State) -> axum::Router {
    axum::Router::new()
        .route(MANAGER_REGISTER_PATH, post(register_worker))
        .route(MANAGER_UPDATE_TASK_PATH, patch(update_task))
        .with_state(state)
}

async fn register_worker(
    State(state): State<crate::state::State>,
    Json(r): Json<RegisterRequest>,
) -> Result<(), ErrorResponse> {
    let addr = format!("http://{}", r.worker_address);
    let client = crate::worker::Client::new(addr)?;

    let worker = Worker {
        address: r.worker_address.clone(),
        client,
    };

    let mut workers = state.workers.lock().await;
    workers.push(worker);

    log::info!("worker {} registered", r.worker_address);

    Ok(())
}

async fn update_task(
    State(state): State<crate::state::State>,
    Query(q): Query<UpdateTaskQuery>,
    Json(r): Json<UpdateTaskRequest>,
) -> Result<(), ErrorResponse> {
    let crack = {
        let requests = state.requests.lock().await;

        requests
            .get(&q.request_id)
            .cloned()
            .ok_or(ErrorResponse::new(
                StatusCode::NOT_FOUND,
                format!("request with id {} doesn't exist", q.request_id),
            ))?
    };

    let mut crack = crack.lock().await;

    let worker = crack
        .workers
        .iter_mut()
        .find(|w| w.curr == r.segment_start)
        .ok_or(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            format!("invalid task update"),
        ))?;

    worker.last_update = tokio::time::Instant::now();
    worker.curr = r.segment_end;
    crack.data.extend(r.data);

    let left: usize = crack.workers.iter().map(|w| w.end - w.curr).sum();

    if left == 0 {
        if !crack.data.is_empty() {
            let words = crack.data.join(", ");
            log::info!("Request {}: finished, found words: {}", q.request_id, words);
        } else {
            log::info!("Request {}: didn't find any words", q.request_id);
        }
    }

    Ok(())
}
