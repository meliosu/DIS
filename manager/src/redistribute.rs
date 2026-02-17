use common::types::CreateTaskRequest;

use crate::config::CONFIG;
use crate::state::{CrackWorker, State};

pub async fn redistribute(state: &State) {
    let mut requests = state.requests.lock().await;

    for (id, crack) in requests.iter_mut() {
        let done: usize = crack.workers.iter().map(|w| w.curr - w.start).sum();

        if done == crack.total_count {
            continue;
        }

        let now = tokio::time::Instant::now();

        let mut ranges = Vec::new();
        let mut timed_out = Vec::new();

        for worker in std::mem::take(&mut crack.workers) {
            if worker.curr < worker.end && now - worker.last_update >= CONFIG.timeout {
                log::warn!("Request {}: worker {} timed out", id, worker.worker.address);
                ranges.push((worker.curr, worker.end));
                timed_out.push(worker.worker.clone());
            } else {
                crack.workers.push(worker)
            }
        }

        if ranges.is_empty() {
            continue;
        }

        let mut workers = state.workers.lock().await;
        workers.retain(|w1| !timed_out.iter().any(|w2| w1.address == w2.address));

        for worker in std::mem::take(&mut *workers) {
            if let Err(_) = worker.client.healthcheck().await {
                continue;
            }

            workers.push(worker);
        }

        if workers.is_empty() {
            log::error!("Request {}: no healthy workers available", id);
            continue;
        }

        for (range_start, range_end) in &ranges {
            let range_size = range_end - range_start;
            let worker_count = workers.len();

            for (i, worker) in workers.iter().enumerate() {
                let remainder = range_size % worker_count;
                let base = range_size / worker_count;
                let extra = if i < remainder { 1 } else { 0 };

                let start = range_start + i * base + i.min(remainder);
                let end = start + base + extra;

                if start == end {
                    continue;
                }

                crack.workers.push(CrackWorker {
                    last_update: tokio::time::Instant::now(),
                    worker: worker.clone(),
                    start,
                    end,
                    curr: start,
                });

                let create_task_request = CreateTaskRequest {
                    hash: crack.hash.clone(),
                    request_id: id.clone(),
                    alphabet: crack.alphabet.clone(),
                    max_length: crack.max_length,
                    start,
                    end,
                };

                log::info!(
                    "Request {}: redistributing range {}-{} to worker {}",
                    id, start, end, worker.address
                );

                if let Err(e) = worker.client.create_task(&create_task_request).await {
                    log::error!(
                        "Request {}: failed to send task to worker {}: {}",
                        id, worker.address, e
                    );
                }
            }
        }
    }
}
