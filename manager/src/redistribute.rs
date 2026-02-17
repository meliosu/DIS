use common::types::CreateTaskRequest;

use crate::config::CONFIG;
use crate::state::{CrackWorker, State};

pub async fn redistribute(state: &State) {
    let requests = state.requests.lock().await.clone();

    for (id, arc_crack) in requests {
        let mut crack = arc_crack.lock().await;

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

        drop(crack);

        let workers = state.workers.lock().await;
        let mut workers_clone = workers.clone();
        drop(workers);

        workers_clone.retain(|w1| !timed_out.iter().any(|w2| w1.address == w2.address));

        let mut healthy_workers = Vec::new();

        for worker in workers_clone {
            if worker.client.healthcheck().await.is_ok() {
                healthy_workers.push(worker);
            }
        }

        {
            let mut workers = state.workers.lock().await;
            *workers = healthy_workers.clone();
        }

        if healthy_workers.is_empty() {
            log::error!("Request {}: no healthy workers available", id);
            continue;
        }

        let mut crack = arc_crack.lock().await;

        for (range_start, range_end) in &ranges {
            let range_size = range_end - range_start;
            let worker_count = healthy_workers.len();

            for (i, worker) in healthy_workers.iter().enumerate() {
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
                    id,
                    start,
                    end,
                    worker.address
                );

                if let Err(e) = worker.client.create_task(&create_task_request).await {
                    log::error!(
                        "Request {}: failed to send task to worker {}: {}",
                        id,
                        worker.address,
                        e
                    );
                }
            }
        }
    }
}
