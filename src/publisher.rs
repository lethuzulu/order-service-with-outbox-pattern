use crate::{error::OutboxError, types::OutboxMessage};
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties,
    options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions},
};

#[derive(Debug)]
pub struct Publisher {
    channel: Channel,
    exchange: String,
}

impl Publisher {
    pub async fn connect(amqp_url: &str, exchange: &str) -> Result<Self, OutboxError> {
        let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        channel
            .exchange_declare(
                exchange,
                lapin::ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                Default::default(),
            )
            .await?;

        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;

        tracing::info!(exchange, "connected to RabbitMQ");

        Ok(Self {
            channel,
            exchange: exchange.to_string(),
        })
    }

    pub async fn publish(&self, msg: &OutboxMessage) -> Result<(), OutboxError> {
        let payload = serde_json::to_vec(&msg.payload)?;
        let routing_key = msg.event_type.as_str();

        let confirm = self
            .channel
            .basic_publish(
                &self.exchange,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default()
                    .with_message_id(msg.id.to_string().into())
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2), // 2 = persistent
            )
            .await? // first await: message in broker TCP buffer
            .await?; // second await waits fr the broker confirm

        match confirm {
            lapin::publisher_confirm::Confirmation::Ack(_) => Ok(()),
            lapin::publisher_confirm::Confirmation::Nack(_) => Err(OutboxError::BrokerNack(msg.id)),
            lapin::publisher_confirm::Confirmation::NotRequested => {
                Err(OutboxError::ConfirmsNotEnabled)
            }
        }
    }
}
