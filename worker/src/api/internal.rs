use axum::extract::{Json, State};
use axum::routing::{get, post};

use common::response::ErrorResponse;
use common::types::{UpdateTaskQuery, UpdateTaskRequest};
use common::{
    constants::{WORKER_CRACK_TASK_PATH, WORKER_HEALTHCHECK_PATH},
    types::CreateTaskRequest,
};

use crate::constants::{INTERVAL, INTERVAL_COUNT, UPDATE_COUNT};
use crate::permutations::permutations;

pub fn router(state: crate::state::State) -> axum::Router {
    axum::Router::new()
        .route(WORKER_CRACK_TASK_PATH, post(create_crack_task))
        .route(WORKER_HEALTHCHECK_PATH, get(healthcheck))
        .with_state(state)
}

async fn create_crack_task(
    State(state): State<crate::state::State>,
    Json(r): Json<CreateTaskRequest>,
) -> Result<(), ErrorResponse> {
    tokio::task::spawn(async move {
        let alphabet = r.alphabet.chars().collect::<Vec<_>>();

        let mut data = Vec::new();

        let mut start = r.start;
        let mut last = tokio::time::Instant::now();

        for (i, perm) in permutations(&alphabet, r.max_length)
            .skip(r.start)
            .take(r.end - r.start)
            .enumerate()
        {
            let word = perm.iter().collect::<String>();
            let hash = md5::compute(&word);
            let hash = hex::encode(&*hash);

            if hash == r.hash {
                data.push(word);
            }

            let update_frequency = ((r.end - r.start) / UPDATE_COUNT).max(1);

            let should_send_update =
                (i != 0 && i % INTERVAL_COUNT == 0 && last.elapsed() > INTERVAL)
                    || (i != 0 && i % update_frequency == 0)
                    || i == r.end - r.start - 1;

            if should_send_update {
                let query = UpdateTaskQuery {
                    request_id: r.request_id.clone(),
                };

                if !data.is_empty() {
                    let words = data.join(", ");
                    log::info!(
                        "Request {}: found words in range {}-{}: {}",
                        r.request_id,
                        r.start,
                        r.end,
                        words
                    );
                }

                let request = UpdateTaskRequest {
                    segment_start: start,
                    segment_end: r.start + i + 1,
                    data: std::mem::take(&mut data),
                };

                _ = state.client.update(&query, &request).await;

                start = r.start + i + 1;
                last = tokio::time::Instant::now();
            }

            tokio::task::consume_budget().await;
        }
    });

    Ok(())
}

async fn healthcheck() {}
