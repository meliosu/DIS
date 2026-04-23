use anyhow::Context;
use lapin::{
    Channel, ExchangeKind, Connection, ConnectionProperties,
    options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
};

use crate::constants::*;

pub async fn connect_channel(rabbit_addr: &str) -> anyhow::Result<(Connection, Channel)> {
    let connection = Connection::connect(rabbit_addr, ConnectionProperties::default())
        .await
        .with_context(|| format!("failed to connect RabbitMQ at {rabbit_addr}"))?;

    let channel = connection.create_channel().await?;
    Ok((connection, channel))
}

pub async fn declare_topology(channel: &Channel) -> anyhow::Result<()> {
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
