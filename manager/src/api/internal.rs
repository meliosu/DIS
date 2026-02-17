use axum::routing::{post, patch};
use axum::extract::{Json, Query, State};

use common::response::ErrorResponse;
use common::types::{RegisterRequest, UpdateTaskRequest, UpdateTaskQuery};
use common::constants::{MANAGER_REGISTER_PATH, MANAGER_UPDATE_TASK_PATH};
use reqwest::StatusCode;

use crate::state::Worker;

pub fn router(state: crate::state::State) -> axum::Router {
    axum::Router::new()
        .route(MANAGER_REGISTER_PATH, post(register_worker))
        .route(MANAGER_UPDATE_TASK_PATH, patch(update_task))
        .with_state(state)
}

async fn register_worker(State(state): State<crate::state::State>, Json(r): Json<RegisterRequest>) -> Result<(), ErrorResponse> {
    let addr = format!("http://{}", r.worker_address);
    let client = crate::worker::Client::new(addr)?;
    let worker = Worker {
        address: r.worker_address,
        client,
    };

    let mut workers = state.workers.lock().await;
    workers.push(worker);
    drop(workers);

    Ok(())
}

async fn update_task(State(state): State<crate::state::State>, Query(q): Query<UpdateTaskQuery>, Json(r): Json<UpdateTaskRequest>) -> Result<(), ErrorResponse> {
    let mut requests = state.requests.lock().await;

    let crack = requests.get_mut(&q.request_id)
        .ok_or(ErrorResponse::new(StatusCode::NOT_FOUND, format!("request with id {} doesn't exist", q.request_id)))?;

    let worker = crack.workers
        .iter_mut()
        .find(|w| w.curr == r.segment_start)
        .ok_or(ErrorResponse::new(StatusCode::BAD_REQUEST, format!("invalid task update")))?;

    worker.last_update = tokio::time::Instant::now();
    worker.curr = r.segment_end;
    crack.data.extend(r.data);

    drop(requests);

    Ok(())
}
