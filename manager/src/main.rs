use std::{collections::BTreeSet, env, time::Duration};

use anyhow::{Context, anyhow};
use axum::{
    Json, Router,
    extract::{Query, State},
    routing::{get, post},
};
use futures_util::{StreamExt, TryStreamExt};
use lapin::{
    BasicProperties,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use mongodb::{
    Client,
    bson::{Bson, DateTime, doc},
    options::{ClientOptions, WriteConcern},
};
use uuid::Uuid;

use common::constants::*;
use common::rabbit;
use common::types::*;

use manager::constants::*;
use manager::helpers::{split_evenly, total_combinations};
use manager::types::*;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run().await {
        log::error!("{e:#}");
    }
}

async fn run() -> anyhow::Result<()> {
    let bind_addr =
        env::var(MANAGER_BIND_ADDR_ENV).unwrap_or_else(|_| DEFAULT_MANAGER_BIND_ADDR.to_string());

    let rabbit_addr =
        env::var(RABBITMQ_ADDR_ENV).unwrap_or_else(|_| DEFAULT_RABBITMQ_ADDR.to_string());

    let mongo_uri = env::var(MONGO_URI_ENV).unwrap_or_else(|_| DEFAULT_MONGO_URI.to_string());
    let mongo_db = env::var(MONGO_DB_ENV).unwrap_or_else(|_| DEFAULT_MONGO_DB.to_string());
    let alphabet = env::var(ALPHABET_ENV).context("ALPHABET is not set")?;
    let fallback_workers = env::var(NUM_WORKERS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_WORKERS)
        .max(1);

    if alphabet.is_empty() {
        return Err(anyhow!("ALPHABET must not be empty"));
    }

    let mut mongo_options = ClientOptions::parse(&mongo_uri)
        .await
        .with_context(|| format!("failed to parse MongoDB URI: {mongo_uri}"))?;

    mongo_options.app_name = Some(MONGODB_APP_NAME.to_string());
    mongo_options.write_concern = Some(WriteConcern::majority());
    let client = Client::with_options(mongo_options)?;
    let db = client.database(&mongo_db);

    let state = AppState {
        requests: db.collection(REQUESTS_COLLECTION),
        tasks: db.collection(TASKS_COLLECTION),
        rabbit_addr,
        alphabet,
        fallback_workers,
    };

    if let Err(error) = ensure_rabbit_topology(&state.rabbit_addr).await {
        log::warn!("RabbitMQ is currently unavailable during startup: {error:#}");
    }

    spawn_results_consumer(state.clone());
    spawn_dlq_consumer(state.clone());
    spawn_pending_republisher(state.clone());

    if let Err(error) = publish_pending_tasks(&state, None).await {
        log::warn!("Initial pending task publish failed: {error:#}");
    }

    let app = Router::new()
        .route("/api/hash/crack", post(crack_hash))
        .route("/api/hash/status", get(get_status))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind manager on {bind_addr}"))?;

    log::info!("Manager listening on {bind_addr}");
    axum::serve(listener, app)
        .await
        .context("manager HTTP server exited")
}

async fn crack_hash(
    State(state): State<AppState>,
    Json(payload): Json<CrackHashRequest>,
) -> Result<Json<CrackHashResponse>, ApiError> {
    if payload.max_length == 0 {
        return Err(ApiError::bad_request("maxLength must be greater than 0"));
    }

    if !is_valid_md5(&payload.hash) {
        return Err(ApiError::bad_request(
            "hash must be a valid 32-char MD5 hex string",
        ));
    }

    let request_id = Uuid::new_v4();
    let worker_count = available_worker_count(&state.rabbit_addr, state.fallback_workers).await;
    let max_length = i32::try_from(payload.max_length)
        .map_err(|_| ApiError::bad_request("maxLength is too large"))?;

    let total = total_combinations(state.alphabet.chars().count(), payload.max_length)
        .ok_or_else(|| ApiError::bad_request("search space is too large"))?;

    let ranges = split_evenly(total, worker_count);

    if ranges.is_empty() {
        return Err(ApiError::bad_request("no combinations can be generated"));
    }

    let now = DateTime::now();
    let request_doc = RequestDocument {
        id: request_id.to_string(),
        hash: payload.hash.clone(),
        max_length,
        status: RequestStatus::InProgress,
        progress: 0,
        data: Vec::new(),
        total_tasks: ranges.len() as i32,
        completed_tasks: 0,
        last_error: None,
        created_at: now,
        updated_at: now,
    };

    state
        .requests
        .insert_one(request_doc)
        .await
        .map_err(|error| ApiError::internal(format!("failed to create request: {error}")))?;

    log::info!(
        "Request {}: searching for hash {} with maximum length of {}, splitting {} combinations into {} tasks",
        request_id,
        payload.hash,
        max_length,
        total,
        worker_count
    );

    let mut task_docs = Vec::with_capacity(ranges.len());
    for (start_index, end_index) in ranges {
        let task_id = Uuid::new_v4();
        let total_candidates = end_index
            .checked_sub(start_index)
            .ok_or_else(|| ApiError::internal("invalid task split boundaries"))?;

        task_docs.push(TaskDocument {
            id: task_id.to_string(),
            request_id: request_id.to_string(),
            hash: payload.hash.clone(),
            max_length,
            alphabet: state.alphabet.clone(),
            start_index: i64::try_from(start_index).map_err(|_| {
                ApiError::internal("start index overflow while creating task document")
            })?,
            end_index: i64::try_from(end_index).map_err(|_| {
                ApiError::internal("end index overflow while creating task document")
            })?,
            total_candidates: i64::try_from(total_candidates)
                .map_err(|_| ApiError::internal("task range overflow while creating task"))?,
            processed_candidates: 0,
            status: TASK_PENDING_PUBLISH.to_string(),
            matches: Vec::new(),
            last_error: None,
            created_at: now,
            updated_at: now,
        });
    }

    state
        .tasks
        .insert_many(task_docs)
        .await
        .map_err(|error| ApiError::internal(format!("failed to create tasks: {error}")))?;

    if let Err(error) = publish_pending_tasks(&state, Some(request_id)).await {
        log::warn!("Unable to publish request {request_id} tasks immediately: {error:#}");
    }

    Ok(Json(CrackHashResponse { request_id }))
}

async fn get_status(
    State(state): State<AppState>,
    Query(query): Query<CrackStatusRequest>,
) -> Result<Json<CrackStatusResponse>, ApiError> {
    let request_id = query.request_id.to_string();
    let request = state
        .requests
        .find_one(doc! { "_id": request_id })
        .await
        .map_err(|error| ApiError::internal(format!("failed to read status: {error}")))?
        .ok_or_else(|| ApiError::not_found("request not found"))?;

    let data = if !request.data.is_empty() {
        Some(request.data)
    } else {
        None
    };

    Ok(Json(CrackStatusResponse {
        status: request.status,
        progress: request.progress.clamp(0, 100) as u8,
        data,
    }))
}

fn spawn_pending_republisher(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(REPUBLISH_INTERVAL_SECS));
        loop {
            interval.tick().await;
            if let Err(error) = publish_pending_tasks(&state, None).await {
                log::warn!("Pending task republish iteration failed: {error:#}");
            }
        }
    });
}

fn spawn_results_consumer(state: AppState) {
    tokio::spawn(async move {
        loop {
            match consume_results_once(&state).await {
                Ok(()) => log::warn!("Results consumer completed unexpectedly, reconnecting"),
                Err(error) => log::error!("Results consumer failed: {error:#}"),
            }

            tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
        }
    });
}

fn spawn_dlq_consumer(state: AppState) {
    tokio::spawn(async move {
        loop {
            match consume_dlq_once(&state).await {
                Ok(()) => log::warn!("DLQ consumer completed unexpectedly, reconnecting"),
                Err(error) => log::error!("DLQ consumer failed: {error:#}"),
            }

            tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
        }
    });
}

async fn publish_pending_tasks(state: &AppState, request_id: Option<Uuid>) -> anyhow::Result<()> {
    let mut filter = doc! { "status": TASK_PENDING_PUBLISH };
    if let Some(request_id) = request_id {
        filter.insert("request_id", request_id.to_string());
    }

    let mut pending_cursor = state.tasks.find(filter).await?;
    let mut pending_tasks = Vec::new();
    while let Some(task) = pending_cursor.try_next().await? {
        pending_tasks.push(task);
    }

    if pending_tasks.is_empty() {
        return Ok(());
    }

    let (_connection, channel) = rabbit::connect_channel(&state.rabbit_addr).await?;
    rabbit::declare_topology(&channel).await?;

    for task in pending_tasks {
        let message = task_document_to_message(&task)?;
        let payload = serde_json::to_vec(&message)?;
        let confirmation = channel
            .basic_publish(
                TASKS_EXCHANGE.into(),
                TASKS_ROUTING_KEY.into(),
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_delivery_mode(2),
            )
            .await?;

        confirmation
            .await
            .with_context(|| format!("publisher confirm failed for task {}", task.id))?;

        state
            .tasks
            .update_one(
                doc! { "_id": &task.id, "status": TASK_PENDING_PUBLISH },
                doc! {
                    "$set": {
                        "status": TASK_QUEUED,
                        "last_error": Bson::Null,
                        "updated_at": DateTime::now(),
                    }
                },
            )
            .await?;

        log::info!("Queued task {} for request {}", task.id, task.request_id);
    }

    Ok(())
}

async fn available_worker_count(rabbit_addr: &str, fallback_workers: usize) -> usize {
    let fallback = fallback_workers.max(1);

    let Ok((_connection, channel)) = rabbit::connect_channel(rabbit_addr).await else {
        log::warn!("Unable to read live worker count from RabbitMQ, using fallback {fallback}");
        return fallback;
    };

    if let Err(error) = rabbit::declare_topology(&channel).await {
        log::warn!(
            "Unable to ensure RabbitMQ topology while reading worker count: {error:#}. Using fallback {fallback}"
        );

        return fallback;
    }

    match channel
        .queue_declare(
            TASKS_QUEUE.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
    {
        Ok(queue) => {
            let consumers = queue.consumer_count() as usize;
            if consumers == 0 { fallback } else { consumers }
        }
        Err(error) => {
            log::warn!(
                "Unable to query queue consumer count from RabbitMQ: {error}. Using fallback {fallback}"
            );

            fallback
        }
    }
}

async fn consume_results_once(state: &AppState) -> anyhow::Result<()> {
    let (_connection, channel) = rabbit::connect_channel(&state.rabbit_addr).await?;
    rabbit::declare_topology(&channel).await?;

    let mut consumer = channel
        .basic_consume(
            RESULTS_QUEUE.into(),
            RESULTS_CONSUMER_TAG.into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    log::info!("Connected results consumer");

    while let Some(delivery_result) = consumer.next().await {
        let delivery = delivery_result?;
        match handle_worker_update_message(state, &delivery.data).await {
            Ok(()) => {
                delivery.ack(BasicAckOptions::default()).await?;
            }
            Err(error) => {
                log::error!("Failed to process result message, requeueing: {error:#}");
                delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: true,
                    })
                    .await?;
            }
        }
    }

    Err(anyhow!("results consumer stream closed"))
}

async fn consume_dlq_once(state: &AppState) -> anyhow::Result<()> {
    let (_connection, channel) = rabbit::connect_channel(&state.rabbit_addr).await?;
    rabbit::declare_topology(&channel).await?;

    let mut consumer = channel
        .basic_consume(
            DLQ_QUEUE.into(),
            DLQ_CONSUMER_TAG.into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    log::info!("Connected DLQ consumer");

    while let Some(delivery_result) = consumer.next().await {
        let delivery = delivery_result?;
        if let Err(error) = handle_dlq_message(state, &delivery.data).await {
            log::error!("Failed to process DLQ message: {error:#}");
        }

        delivery.ack(BasicAckOptions::default()).await?;
    }

    Err(anyhow!("DLQ consumer stream closed"))
}

async fn handle_worker_update_message(state: &AppState, payload: &[u8]) -> anyhow::Result<()> {
    let message: WorkerTaskUpdateMessage =
        serde_json::from_slice(payload).context("invalid worker update payload")?;

    match message {
        WorkerTaskUpdateMessage::Progress {
            request_id,
            task_id,
            processed,
            total,
        } => {
            log::info!(
                "Request {}: received progress message for task {}, {}/{} processed",
                request_id,
                task_id,
                processed,
                total
            );

            let update_result = state
                .tasks
                .update_one(
                    doc! {
                        "_id": task_id.to_string(),
                        "status": { "$in": [TASK_QUEUED, TASK_PENDING_PUBLISH] },
                    },
                    doc! {
                        "$max": { "processed_candidates": i64::try_from(processed)? },
                        "$set": {
                            "total_candidates": i64::try_from(total)?,
                            "updated_at": DateTime::now(),
                        },
                    },
                )
                .await?;

            if update_result.matched_count == 0 {
                log::warn!("Ignoring stale/duplicate progress for task {task_id}");
                return Ok(());
            }

            refresh_request_state(state, request_id, None).await?;
        }
        WorkerTaskUpdateMessage::Finished {
            request_id,
            task_id,
            processed,
            total,
            matches,
            error,
        } => {
            if let Some(ref error) = error {
                log::warn!(
                    "Request {}: received error message for task {}: {}",
                    request_id,
                    task_id,
                    error
                )
            } else {
                log::info!(
                    "Request {}: received final message for task {}, {}/{} processed",
                    request_id,
                    task_id,
                    processed,
                    total
                );
            }

            let status = if error.is_some() {
                TASK_ERROR
            } else {
                TASK_DONE
            };

            let mut set_doc = doc! {
                "status": status,
                "matches": matches,
                "total_candidates": i64::try_from(total)?,
                "processed_candidates": i64::try_from(processed)?,
                "updated_at": DateTime::now(),
            };

            match error.as_ref() {
                Some(error) => {
                    set_doc.insert("last_error", error.clone());
                }
                None => {
                    set_doc.insert("last_error", Bson::Null);
                }
            }

            let update_result = state
                .tasks
                .update_one(
                    doc! {
                        "_id": task_id.to_string(),
                        "status": { "$in": [TASK_QUEUED, TASK_PENDING_PUBLISH] },
                    },
                    doc! { "$set": set_doc },
                )
                .await?;

            if update_result.matched_count == 0 {
                log::warn!("Ignoring stale/duplicate completion for task {task_id}");
                return Ok(());
            }

            refresh_request_state(state, request_id, error.as_deref()).await?;
        }
    }

    Ok(())
}

async fn handle_dlq_message(state: &AppState, payload: &[u8]) -> anyhow::Result<()> {
    let body = String::from_utf8_lossy(payload);
    log::error!("DLQ message received: {body}");

    if let Ok(task_message) = serde_json::from_slice::<CrackTaskMessage>(payload) {
        let task_id = task_message.task_id.to_string();

        state
            .tasks
            .update_one(
                doc! { "_id": &task_id },
                doc! {
                    "$set": {
                        "status": TASK_DLQ,
                        "last_error": "Task moved to DLQ after 3 failed attempts",
                        "updated_at": DateTime::now(),
                    }
                },
            )
            .await?;

        refresh_request_state(
            state,
            task_message.request_id,
            Some("Task moved to DLQ after 3 failed attempts"),
        )
        .await?;

        return Ok(());
    }

    if let Ok(result_message) = serde_json::from_slice::<WorkerTaskUpdateMessage>(payload) {
        let (request_id, task_id) = match result_message {
            WorkerTaskUpdateMessage::Progress {
                request_id,
                task_id,
                ..
            } => (request_id, task_id),
            WorkerTaskUpdateMessage::Finished {
                request_id,
                task_id,
                ..
            } => (request_id, task_id),
        };

        let task_id = task_id.to_string();

        state
            .tasks
            .update_one(
                doc! { "_id": &task_id, "status": { "$ne": TASK_DONE } },
                doc! {
                    "$set": {
                        "status": TASK_ERROR,
                        "last_error": "Result message moved to DLQ after 3 failed attempts",
                        "updated_at": DateTime::now(),
                    }
                },
            )
            .await?;

        refresh_request_state(
            state,
            request_id,
            Some("Result moved to DLQ after 3 failed attempts"),
        )
        .await?;

        return Ok(());
    }

    Ok(())
}

async fn refresh_request_state(
    state: &AppState,
    request_id: Uuid,
    last_error: Option<&str>,
) -> anyhow::Result<()> {
    let request_key = request_id.to_string();
    let mut cursor = state
        .tasks
        .find(doc! { "request_id": &request_key })
        .await?;

    let mut total_tasks = 0_i32;
    let mut completed = 0_i32;
    let mut has_error = false;
    let mut data = BTreeSet::new();
    let mut total_candidates = 0_i64;
    let mut processed_candidates = 0_i64;

    while let Some(task) = cursor.try_next().await? {
        total_tasks += 1;
        let task_total = task.total_candidates;
        let task_processed = task.processed_candidates;

        match task.status.as_str() {
            TASK_DONE => {
                completed += 1;
                for value in task.matches {
                    data.insert(value);
                }
            }
            TASK_ERROR | TASK_DLQ => has_error = true,
            _ => {}
        }

        total_candidates = total_candidates.saturating_add(task_total);
        processed_candidates = processed_candidates.saturating_add(task_processed);
    }

    if total_tasks == 0 {
        return Ok(());
    }

    let progress = if total_candidates > 0 {
        ((processed_candidates.saturating_mul(100)) / total_candidates).clamp(0, 100) as i32
    } else {
        ((completed * 100) / total_tasks).clamp(0, 100)
    };

    let status = if has_error {
        RequestStatus::Error
    } else if completed == total_tasks {
        RequestStatus::Ready
    } else {
        RequestStatus::InProgress
    };

    let output = data.into_iter().collect::<Vec<_>>();
    let status_bson = mongodb::bson::to_bson(&status)?;

    let mut set_doc = doc! {
        "status": status_bson,
        "progress": progress,
        "completed_tasks": completed,
        "data": output,
        "updated_at": DateTime::now(),
    };

    if let Some(error) = last_error {
        set_doc.insert("last_error", error);
    } else if status != RequestStatus::Error {
        set_doc.insert("last_error", Bson::Null);
    }

    state
        .requests
        .update_one(doc! { "_id": request_key }, doc! { "$set": set_doc })
        .await?;

    Ok(())
}

fn task_document_to_message(task: &TaskDocument) -> anyhow::Result<CrackTaskMessage> {
    Ok(CrackTaskMessage {
        request_id: Uuid::parse_str(&task.request_id)
            .with_context(|| format!("invalid request ID in task {}", task.id))?,
        task_id: Uuid::parse_str(&task.id)
            .with_context(|| format!("invalid task ID in task {}", task.id))?,
        hash: task.hash.clone(),
        max_length: usize::try_from(task.max_length)
            .with_context(|| format!("invalid max_length in task {}", task.id))?,
        alphabet: task.alphabet.clone(),
        start_index: u64::try_from(task.start_index)
            .with_context(|| format!("invalid start_index in task {}", task.id))?,
        end_index: u64::try_from(task.end_index)
            .with_context(|| format!("invalid end_index in task {}", task.id))?,
    })
}

async fn ensure_rabbit_topology(rabbit_addr: &str) -> anyhow::Result<()> {
    let (_connection, channel) = rabbit::connect_channel(rabbit_addr).await?;
    rabbit::declare_topology(&channel).await
}

fn is_valid_md5(hash: &str) -> bool {
    hash.len() == 32 && hash.chars().all(|ch| ch.is_ascii_hexdigit())
}
