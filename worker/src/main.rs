use std::{
    env, panic,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use common::rabbit;
use common::{
    constants::{
        ALPHABET_ENV, DEFAULT_PROGRESS_REPORT_INTERVAL_MS, DEFAULT_RABBITMQ_ADDR,
        DEFAULT_WORKER_MAX_CONCURRENCY, PROGRESS_REPORT_INTERVAL_MS_ENV, RABBITMQ_ADDR_ENV,
        RESULTS_EXCHANGE, RESULTS_ROUTING_KEY, TASKS_QUEUE, WORKER_MAX_CONCURRENCY_ENV,
    },
    types::{CrackTaskMessage, WorkerTaskUpdateMessage},
};
use futures_util::StreamExt;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    message::Delivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions,
    },
    types::FieldTable,
};
use tokio::task::JoinSet;
use worker::permutations::permutations;

const TASKS_CONSUMER_TAG: &str = "hash-worker-task-consumer";
const RETRY_DELAY_SECS: u64 = 3;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run().await {
        log::error!("{e:#}");
    }
}

async fn run() -> anyhow::Result<()> {
    let rabbit_addr =
        env::var(RABBITMQ_ADDR_ENV).unwrap_or_else(|_| DEFAULT_RABBITMQ_ADDR.to_string());
    let configured_alphabet = env::var(ALPHABET_ENV).context("ALPHABET is not set")?;
    let worker_max_concurrency = env::var(WORKER_MAX_CONCURRENCY_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_WORKER_MAX_CONCURRENCY);
    let progress_interval_ms = env::var(PROGRESS_REPORT_INTERVAL_MS_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_PROGRESS_REPORT_INTERVAL_MS);
    if configured_alphabet.is_empty() {
        return Err(anyhow!("ALPHABET must not be empty"));
    }
    if progress_interval_ms == 0 {
        return Err(anyhow!(
            "{PROGRESS_REPORT_INTERVAL_MS_ENV} must be greater than 0"
        ));
    }
    if worker_max_concurrency == 0 {
        return Err(anyhow!(
            "{WORKER_MAX_CONCURRENCY_ENV} must be greater than 0"
        ));
    }
    let progress_interval = Duration::from_millis(progress_interval_ms);

    loop {
        match consume_tasks_once(
            &rabbit_addr,
            &configured_alphabet,
            progress_interval,
            worker_max_concurrency,
        )
        .await
        {
            Ok(()) => log::warn!("Task consumer completed unexpectedly, reconnecting"),
            Err(error) => log::error!("Task consumer failed: {error:#}"),
        }

        tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
    }
}

async fn consume_tasks_once(
    rabbit_addr: &str,
    configured_alphabet: &str,
    progress_interval: Duration,
    worker_max_concurrency: usize,
) -> anyhow::Result<()> {
    let (_connection, channel) = connect_rabbit_channel(rabbit_addr).await?;
    rabbit::declare_topology(&channel).await?;
    let qos = worker_max_concurrency.min(u16::MAX as usize) as u16;
    channel.basic_qos(qos, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume(
            TASKS_QUEUE.into(),
            TASKS_CONSUMER_TAG.into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    log::info!("Worker is consuming tasks with max concurrency {worker_max_concurrency}");
    let mut in_flight = JoinSet::<anyhow::Result<()>>::new();

    loop {
        tokio::select! {
            delivery_result = consumer.next(), if in_flight.len() < worker_max_concurrency => {
                let Some(delivery_result) = delivery_result else {
                    break;
                };

                let delivery = delivery_result?;
                let task_channel = channel.clone();
                let alphabet = configured_alphabet.to_owned();
                in_flight.spawn(async move {
                    process_delivery(task_channel, delivery, alphabet, progress_interval).await
                });
            }
            join_result = in_flight.join_next(), if !in_flight.is_empty() => {
                let join_result = join_result.expect("join_next returned None with non-empty set");
                handle_in_flight_result(join_result)?;
            }
        }
    }

    while let Some(join_result) = in_flight.join_next().await {
        handle_in_flight_result(join_result)?;
    }

    Err(anyhow!("task consumer stream closed"))
}

async fn process_delivery(
    channel: Channel,
    delivery: Delivery,
    configured_alphabet: String,
    progress_interval: Duration,
) -> anyhow::Result<()> {
    match handle_task_delivery(
        &channel,
        &delivery.data,
        &configured_alphabet,
        progress_interval,
    )
    .await
    {
        Ok(()) => {
            delivery.ack(BasicAckOptions::default()).await?;
        }
        Err(error) => {
            log::error!("Task handling failed, requeueing: {error:#}");
            delivery
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue: true,
                })
                .await?;
        }
    }

    Ok(())
}

fn handle_in_flight_result(
    join_result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> anyhow::Result<()> {
    match join_result {
        Ok(result) => result,
        Err(join_error) if join_error.is_panic() => {
            panic::resume_unwind(join_error.into_panic());
        }
        Err(join_error) => Err(anyhow!("worker task join failed: {join_error}")),
    }
}

async fn handle_task_delivery(
    channel: &Channel,
    payload: &[u8],
    configured_alphabet: &str,
    progress_interval: Duration,
) -> anyhow::Result<()> {
    let message: CrackTaskMessage =
        serde_json::from_slice(payload).context("invalid task payload")?;

    if message.alphabet != configured_alphabet {
        log::warn!(
            "Task {} alphabet differs from worker env ALPHABET; using task alphabet",
            message.task_id
        );
    }

    let total = message
        .end_index
        .checked_sub(message.start_index)
        .ok_or_else(|| anyhow!("invalid task range boundaries"))?;
    if total == 0 {
        return Err(anyhow!("task range is empty"));
    }
    let matches = crack_task(channel, &message, progress_interval).await?;
    let result = WorkerTaskUpdateMessage::Finished {
        request_id: message.request_id,
        task_id: message.task_id,
        processed: total,
        total,
        matches,
        error: None,
    };

    publish_worker_update(channel, &result).await?;
    Ok(())
}

async fn crack_task(
    channel: &Channel,
    task: &CrackTaskMessage,
    progress_interval: Duration,
) -> anyhow::Result<Vec<String>> {
    if task.end_index < task.start_index {
        return Err(anyhow!(
            "invalid task range: {}..{}",
            task.start_index,
            task.end_index
        ));
    }

    if task.max_length == 0 {
        return Err(anyhow!("task maxLength must be greater than 0"));
    }

    let alphabet: Vec<char> = task.alphabet.chars().collect();
    if alphabet.is_empty() {
        return Err(anyhow!("task alphabet is empty"));
    }

    let start_index =
        usize::try_from(task.start_index).context("task start index does not fit usize")?;
    let total = task
        .end_index
        .checked_sub(task.start_index)
        .ok_or_else(|| anyhow!("invalid task range boundaries"))?;
    if total == 0 {
        return Ok(Vec::new());
    }

    let target_hash = task.hash.to_ascii_lowercase();
    let mut matches = Vec::new();
    let mut generator = permutations(&alphabet, task.max_length);
    let mut first = generator.nth(start_index);
    let mut processed = 0_u64;
    let mut last_progress_report = Instant::now();

    for idx in task.start_index..task.end_index {
        let candidate_vec = if idx == task.start_index {
            first
                .take()
                .ok_or_else(|| anyhow!("start index is out of permutation bounds"))?
        } else {
            generator
                .next()
                .ok_or_else(|| anyhow!("task range exceeded permutation bounds"))?
        };

        let candidate: String = candidate_vec.into_iter().collect();
        if candidate == "bom" {
            panic!(
                "Critical stop-word 'bom' encountered while processing task {}",
                task.task_id
            );
        }

        let digest = format!("{:x}", md5::compute(candidate.as_bytes()));
        if digest == target_hash {
            matches.push(candidate);
        }

        processed = processed.saturating_add(1);
        if processed < total && last_progress_report.elapsed() >= progress_interval {
            let update = WorkerTaskUpdateMessage::Progress {
                request_id: task.request_id,
                task_id: task.task_id,
                processed,
                total,
            };
            publish_worker_update(channel, &update).await?;
            last_progress_report = Instant::now();
        }
    }

    Ok(matches)
}

async fn publish_worker_update(
    channel: &Channel,
    update: &WorkerTaskUpdateMessage,
) -> anyhow::Result<()> {
    let payload = serde_json::to_vec(update)?;
    let confirmation = channel
        .basic_publish(
            RESULTS_EXCHANGE.into(),
            RESULTS_ROUTING_KEY.into(),
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default().with_delivery_mode(2),
        )
        .await?;
    confirmation
        .await
        .context("failed to confirm worker update publish")?;
    Ok(())
}

async fn connect_rabbit_channel(rabbit_addr: &str) -> anyhow::Result<(Connection, Channel)> {
    let connection = Connection::connect(rabbit_addr, ConnectionProperties::default())
        .await
        .with_context(|| format!("failed to connect RabbitMQ at {rabbit_addr}"))?;
    let channel = connection.create_channel().await?;
    Ok((connection, channel))
}
