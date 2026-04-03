use crate::db::Db;

use tokio::time::Duration;

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
}
