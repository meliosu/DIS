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

        for update in 0..UPDATE_COUNT {
            let mut data = Vec::new();

            let step_size = (r.end - r.start) / UPDATE_COUNT;
            let start = r.start + update * step_size;
            let end = r.start + (update + 1) * step_size;

            for perm in permutations(&alphabet, r.max_length).skip(start).take(end - start) {
                let word: String = perm.iter().collect();
                let hash = md5::compute(&word.as_bytes());
                let hash_hex = hex::encode(&*hash);

                if hash_hex == r.hash {
                    data.push(word);
                }
            }

            let query = UpdateTaskQuery {
                request_id: r.request_id.clone(),
            };

            let request = UpdateTaskRequest {
                segment_start: start,
                segment_end: end,
                data,
            };

            match state.client.update(&query, &request).await {
                Err(_) => break,
                _ => {}
            }
        }
    });

    Ok(())
}

async fn healthcheck() {

}
