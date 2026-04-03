use std::sync::Arc;

use crate::{db::Db, error::OutboxError, publisher::Publisher};

use tokio::{
    sync::watch,
    time::{Duration, interval},
};

#[derive(Debug, Clone)]
pub struct PollerConfig {
    pub batch_size: i64,
    pub poll_interval: Duration,
    pub lock_secs: i64,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            poll_interval: Duration::from_secs(2),
            lock_secs: 30,
        }
    }
}

pub struct Poller {
    db: Db,
    config: PollerConfig,
}

impl Poller {
    pub fn new(db: Db, config: PollerConfig) -> Self {
        Self { db, config }
    }

    pub async fn run(
        &self,
        publisher: Arc<Publisher>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), OutboxError> {
        let mut ticker = interval(self.config.poll_interval);

        tracing::info!(
            batch_size = self.config.batch_size,
            lock_secs = self.config.lock_secs,
            interval_ms = self.config.poll_interval.as_millis(),
            "poller started"
        );

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("poller received shutdown, draining current batch");
                        return Ok(());
                    }
                }
                _ = ticker.tick() => {
                    let messages = match self.db.poll(
                        self.config.batch_size,
                        self.config.lock_secs,
                    ).await {
                        Ok(m)  => m,
                        Err(e) => {
                            tracing::error!(error = %e, "poll failed, will retry next tick");
                            continue;
                        }
                    };

                    if messages.is_empty() {
                        continue;
                    }

                    tracing::debug!(count = messages.len(), "claimed outbox messages");

                    for msg in &messages {
                        match publisher.publish(msg).await {
                            Ok(()) => {
                                tracing::info!(
                                    id         = %msg.id,
                                    event_type = %msg.event_type,
                                    aggregate  = msg.aggregate_id,
                                    "message published"
                                );
                                if let Err(e) = self.db.mark_published(msg.id).await {
                                    tracing::error!(
                                        id    = %msg.id,
                                        error = %e,
                                        "failed to mark published — locked_until will expire"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    id         = %msg.id,
                                    event_type = %msg.event_type,
                                    error      = %e,
                                    "publish failed"
                                );
                                if let Err(db_err) = self.db.mark_failed(msg.id, &e.to_string()).await {
                                    tracing::error!(
                                        id    = %msg.id,
                                        error = %db_err,
                                        "failed to mark failed"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
