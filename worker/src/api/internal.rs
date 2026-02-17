use axum::routing::{get, post};
use axum::extract::{Json, State};

use common::response::ErrorResponse;
use common::types::{UpdateTaskQuery, UpdateTaskRequest};
use common::{constants::{WORKER_CRACK_TASK_PATH, WORKER_HEALTHCHECK_PATH}, types::CreateTaskRequest};

use crate::constants::UPDATE_COUNT;
use crate::permutations::permutations;

pub fn router(state: crate::state::State) -> axum::Router {
    axum::Router::new()
        .route(WORKER_CRACK_TASK_PATH, post(create_crack_task))
        .route(WORKER_HEALTHCHECK_PATH, get(healthcheck))
        .with_state(state)
}

async fn create_crack_task(State(state): State<crate::state::State>, Json(r): Json<CreateTaskRequest>) -> Result<(), ErrorResponse> {
    tokio::task::spawn(async move {
        let alphabet = r.alphabet.chars().collect::<Vec<_>>();

        let mut data = Vec::new();

        let mut start = r.start;

        for (i, perm) in permutations(&alphabet, r.max_length).skip(r.start).take(r.end - r.start).enumerate() {
            let word = perm.iter().collect::<String>();
            let hash = md5::compute(&word);
            let hash = hex::encode(&*hash);

            if hash == r.hash {
                data.push(word);
            }

            let update_frequency = ((r.end - r.start) / UPDATE_COUNT).max(1);

            if (i != 0 && i % update_frequency == 0) || i == r.end - r.start - 1 {
                let query = UpdateTaskQuery {
                    request_id: r.request_id.clone(),
                };

                let request = UpdateTaskRequest {
                    segment_start: start,
                    segment_end: r.start + i + 1,
                    data: std::mem::take(&mut data),
                };

                _ = state.client.update(&query, &request).await;

                start = r.start + i + 1;
            }

            if i == r.end - r.start - 1 {
                log::info!("end: {start}");
            }
        }
    });

    Ok(())
}

async fn healthcheck() {

}
