use std::{collections::BTreeSet, env, time::Duration};

use anyhow::{Context, anyhow};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::{
    constants::{
        ALPHABET_ENV, DEFAULT_MANAGER_BIND_ADDR, DEFAULT_MONGO_DB, DEFAULT_MONGO_URI,
        DEFAULT_RABBITMQ_ADDR, DEFAULT_WORKERS, DLQ_QUEUE, DLX_EXCHANGE, MANAGER_BIND_ADDR_ENV,
        MONGO_DB_ENV, MONGO_URI_ENV, NUM_WORKERS_ENV, RABBITMQ_ADDR_ENV, REPUBLISH_INTERVAL_SECS,
        REQUESTS_COLLECTION, REQUEUE_DELIVERY_LIMIT, RESULTS_DLQ_ROUTING_KEY, RESULTS_EXCHANGE,
        RESULTS_QUEUE, RESULTS_ROUTING_KEY, TASKS_COLLECTION, TASKS_DLQ_ROUTING_KEY,
        TASKS_EXCHANGE, TASKS_QUEUE, TASKS_ROUTING_KEY,
    },
    types::{
        CrackHashRequest, CrackHashResponse, CrackStatusRequest, CrackStatusResponse,
        CrackTaskMessage, RequestStatus, WorkerTaskUpdateMessage,
    },
};
use futures_util::{StreamExt, TryStreamExt};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
};
use mongodb::{
    Client, Collection,
    bson::{Bson, DateTime, doc},
    options::ClientOptions,
};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, time::sleep};
use uuid::Uuid;
use manager::helpers::{split_evenly, total_combinations};

const REQUEST_IN_PROGRESS: &str = "IN_PROGRESS";
const REQUEST_READY: &str = "READY";
const REQUEST_ERROR: &str = "ERROR";

const TASK_PENDING_PUBLISH: &str = "PENDING_PUBLISH";
const TASK_QUEUED: &str = "QUEUED";
const TASK_DONE: &str = "DONE";
const TASK_ERROR: &str = "ERROR";
const TASK_DLQ: &str = "DLQ";

const RESULTS_CONSUMER_TAG: &str = "manager-results-consumer";
const DLQ_CONSUMER_TAG: &str = "manager-dlq-consumer";
const RETRY_DELAY_SECS: u64 = 3;

#[derive(Clone)]
struct AppState {
    requests: Collection<RequestDocument>,
    tasks: Collection<TaskDocument>,
    rabbit_addr: String,
    alphabet: String,
    fallback_workers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestDocument {
    #[serde(rename = "_id")]
    id: String,
    hash: String,
    max_length: i32,
    status: String,
    progress: i32,
    data: Vec<String>,
    total_tasks: i32,
    completed_tasks: i32,
    last_error: Option<String>,
    created_at: DateTime,
    updated_at: DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskDocument {
    #[serde(rename = "_id")]
    id: String,
    request_id: String,
    hash: String,
    max_length: i32,
    alphabet: String,
    start_index: i64,
    end_index: i64,
    #[serde(default)]
    total_candidates: i64,
    #[serde(default)]
    processed_candidates: i64,
    status: String,
    matches: Vec<String>,
    last_error: Option<String>,
    created_at: DateTime,
    updated_at: DateTime,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(ErrorResponse {
            error: self.message,
        });
        (self.status, body).into_response()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run().await {
        log::error!("{e:?}");
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
    mongo_options.app_name = Some("hash-cracker-manager".to_string());
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

    let listener = TcpListener::bind(&bind_addr)
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
        status: REQUEST_IN_PROGRESS.to_string(),
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

    let status = parse_request_status(&request.status)
        .ok_or_else(|| ApiError::internal("stored request has invalid status"))?;
    let data = if matches!(status, RequestStatus::Ready) {
        Some(request.data)
    } else {
        None
    };

    Ok(Json(CrackStatusResponse {
        status,
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
            sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
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
            sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
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

    let (_connection, channel) = connect_rabbit_channel(&state.rabbit_addr).await?;
    declare_rabbit_topology(&channel).await?;

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

    let Ok((_connection, channel)) = connect_rabbit_channel(rabbit_addr).await else {
        log::warn!("Unable to read live worker count from RabbitMQ, using fallback {fallback}");
        return fallback;
    };

    if let Err(error) = declare_rabbit_topology(&channel).await {
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
    let (_connection, channel) = connect_rabbit_channel(&state.rabbit_addr).await?;
    declare_rabbit_topology(&channel).await?;
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
    let (_connection, channel) = connect_rabbit_channel(&state.rabbit_addr).await?;
    declare_rabbit_topology(&channel).await?;
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
            let (processed, total) = normalize_progress_counters(processed, total)?;
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
            let (processed, total) = normalize_progress_counters(processed, total)?;
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
        let fallback_total = task.end_index.saturating_sub(task.start_index).max(0);
        let task_total = if task.total_candidates > 0 {
            task.total_candidates
        } else {
            fallback_total
        };
        let mut task_processed = task.processed_candidates.clamp(0, task_total);

        match task.status.as_str() {
            TASK_DONE => {
                completed += 1;
                task_processed = task_total;
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
        REQUEST_ERROR
    } else if completed == total_tasks {
        REQUEST_READY
    } else {
        REQUEST_IN_PROGRESS
    };

    let output = if status == REQUEST_READY {
        data.into_iter().collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let mut set_doc = doc! {
        "status": status,
        "progress": progress,
        "completed_tasks": completed,
        "data": output,
        "updated_at": DateTime::now(),
    };

    if let Some(error) = last_error {
        set_doc.insert("last_error", error);
    } else if status != REQUEST_ERROR {
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
    let (_connection, channel) = connect_rabbit_channel(rabbit_addr).await?;
    declare_rabbit_topology(&channel).await
}

async fn connect_rabbit_channel(rabbit_addr: &str) -> anyhow::Result<(Connection, Channel)> {
    let connection = Connection::connect(rabbit_addr, ConnectionProperties::default())
        .await
        .with_context(|| format!("failed to connect RabbitMQ at {rabbit_addr}"))?;
    let channel = connection.create_channel().await?;
    Ok((connection, channel))
}

async fn declare_rabbit_topology(channel: &Channel) -> anyhow::Result<()> {
    channel
        .exchange_declare(
            TASKS_EXCHANGE.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            RESULTS_EXCHANGE.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .exchange_declare(
            DLX_EXCHANGE.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            TASKS_QUEUE.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            queue_args(TASKS_DLQ_ROUTING_KEY),
        )
        .await?;
    channel
        .queue_bind(
            TASKS_QUEUE.into(),
            TASKS_EXCHANGE.into(),
            TASKS_ROUTING_KEY.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            RESULTS_QUEUE.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            queue_args(RESULTS_DLQ_ROUTING_KEY),
        )
        .await?;
    channel
        .queue_bind(
            RESULTS_QUEUE.into(),
            RESULTS_EXCHANGE.into(),
            RESULTS_ROUTING_KEY.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            DLQ_QUEUE.into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            DLQ_QUEUE.into(),
            DLX_EXCHANGE.into(),
            TASKS_DLQ_ROUTING_KEY.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            DLQ_QUEUE.into(),
            DLX_EXCHANGE.into(),
            RESULTS_DLQ_ROUTING_KEY.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    Ok(())
}

fn queue_args(dlq_routing_key: &str) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(
        "x-queue-type".into(),
        AMQPValue::LongString("quorum".into()),
    );
    args.insert(
        "x-delivery-limit".into(),
        AMQPValue::LongInt(REQUEUE_DELIVERY_LIMIT),
    );
    args.insert(
        "x-dead-letter-exchange".into(),
        AMQPValue::LongString(DLX_EXCHANGE.into()),
    );
    args.insert(
        "x-dead-letter-routing-key".into(),
        AMQPValue::LongString(dlq_routing_key.into()),
    );
    args
}

fn normalize_progress_counters(processed: u64, total: u64) -> anyhow::Result<(u64, u64)> {
    if total == 0 {
        return Err(anyhow!("worker sent progress update with zero total"));
    }

    Ok((processed.min(total), total))
}

fn parse_request_status(status: &str) -> Option<RequestStatus> {
    match status {
        REQUEST_IN_PROGRESS => Some(RequestStatus::InProgress),
        REQUEST_READY => Some(RequestStatus::Ready),
        REQUEST_ERROR => Some(RequestStatus::Error),
        _ => None,
    }
}

fn is_valid_md5(hash: &str) -> bool {
    hash.len() == 32 && hash.chars().all(|ch| ch.is_ascii_hexdigit())
}
