use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    error::OutboxError,
    types::{
        CustomerId, EventType, MessageStatus, Money, Order, OrderId, OrderStatus, OutboxMessage,
    },
};

#[derive(Debug, Clone)]
pub struct Db {
    pub pool: PgPool,
}

impl Db {
    pub async fn new(database_url: &str) -> Result<Self, OutboxError> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }
}

impl Db {
    pub async fn with_transaction<'a, F, Fut, T>(&self, f: F) -> Result<T, OutboxError>
    where
        F: FnOnce(Transaction<'a, Postgres>) -> Fut,
        Fut: Future<Output = Result<(T, Transaction<'a, Postgres>), OutboxError>>,
    {
        let tx = self.pool.begin().await?;
        let (result, tx) = f(tx).await?;
        tx.commit().await?;
        Ok(result)
    }
}

impl Db {
    pub async fn create_order(
        &self,
        customer_id: CustomerId,
        amount: Money,
    ) -> Result<OrderId, OutboxError> {
        self.with_transaction(|mut tx| async {
            let order_id = Db::insert_order(&mut tx, customer_id, amount).await?;

            Db::insert_outbox_message(
                &mut tx,
                &EventType::new("order.created")?,
                json!({
                    "order_id":    order_id.as_uuid(),
                    "customer_id": customer_id.as_uuid(),
                    "amount":      amount.cents(),
                }),
                &order_id.to_string(),
            )
            .await?;

            Ok((order_id, tx))
        })
        .await
    }

    pub async fn insert_order(
        tx: &mut Transaction<'_, Postgres>,
        customer_id: CustomerId,
        amount: Money,
    ) -> Result<OrderId, OutboxError> {
        let id = sqlx::query_scalar!(
            r#"
            INSERT INTO orders (customer_id, amount)
            VALUES ($1, $2)
            RETURNING id
            "#,
            customer_id.as_uuid(),
            amount.cents(),
        )
        .fetch_one(&mut **tx)
        .await?;

        Ok(OrderId::from_uuid(id))
    }

    pub async fn get_order(&self, id: OrderId) -> Result<Option<Order>, OutboxError> {
        let row = sqlx::query!(
            r#"
            SELECT id, customer_id, amount, status, created_at
            FROM   orders
            WHERE  id = $1
            "#,
            id.as_uuid(),
        )
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| {
            Ok(Order {
                id: OrderId::from_uuid(r.id),
                customer_id: CustomerId::from_uuid(r.customer_id),
                amount: Money::from_cents(r.amount)?,
                status: OrderStatus::try_from(r.status.as_str())?,
                created_at: r.created_at,
            })
        })
        .transpose()
    }
}

impl Db {
    pub async fn insert_outbox_message(
        tx: &mut Transaction<'_, Postgres>,
        event_type: &EventType,
        payload: serde_json::Value,
        aggregate_id: &str,
    ) -> Result<Uuid, OutboxError> {
        let id = sqlx::query_scalar!(
            r#"
            INSERT INTO outbox_messages (event_type, payload, aggregate_id)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
            event_type.as_str(),
            payload,
            aggregate_id,
        )
        .fetch_one(&mut **tx)
        .await?;

        Ok(id)
    }
}

impl Db {
    pub async fn poll(
        &self,
        batch_size: i64,
        lock_secs: i64,
    ) -> Result<Vec<OutboxMessage>, OutboxError> {
        let rows = sqlx::query!(
            r#"
            UPDATE outbox_messages
            SET    status       = 'processing',
                   attempts     = attempts + 1,
                   locked_until = now() + ($1 || ' seconds')::interval
            WHERE  id IN (
                SELECT id FROM outbox_messages
                WHERE  status = 'pending'
                AND    (locked_until IS NULL OR locked_until < now())
                ORDER  BY created_at
                LIMIT  $2
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, event_type, payload, aggregate_id, attempts, created_at
            "#,
            lock_secs.to_string(),
            batch_size,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|r| {
                Ok(OutboxMessage {
                    id: r.id,
                    event_type: EventType::new(r.event_type)?,
                    payload: r.payload,
                    aggregate_id: r.aggregate_id,
                    status: MessageStatus::Processing,
                    attempts: r.attempts,
                    last_error: None,
                    published_at: None,
                    created_at: r.created_at,
                })
            })
            .collect()
    }

    pub async fn mark_published(&self, id: Uuid) -> Result<(), OutboxError> {
        sqlx::query!(
            r#"
            UPDATE outbox_messages
            SET    status       = 'published',
                   published_at = now(),
                   locked_until = NULL
            WHERE  id = $1
            "#,
            id,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_failed(&self, id: Uuid, error: &str) -> Result<(), OutboxError> {
        sqlx::query!(
            r#"
            UPDATE outbox_messages
            SET    status        = 'failed',
                   last_error    = $1,
                   locked_until  = NULL
            WHERE  id = $2
            "#,
            error,
            id,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl Db {
    pub async fn count_orders(&self) -> Result<i64, OutboxError> {
        let n = sqlx::query_scalar!("SELECT COUNT(*) FROM orders")
            .fetch_one(&self.pool)
            .await?
            .unwrap_or(0);
        Ok(n)
    }

    pub async fn count_outbox_by_status(&self, status: &str) -> Result<i64, OutboxError> {
        let n = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM outbox_messages WHERE status = $1",
            status
        )
        .fetch_one(&self.pool)
        .await?
        .unwrap_or(0);
        Ok(n)
    }

    pub async fn latest_outbox_message(&self) -> Result<Option<OutboxMessage>, OutboxError> {
        let row = sqlx::query!(
            r#"
            SELECT id, event_type, payload, aggregate_id, status,
                   attempts, last_error, published_at, created_at
            FROM   outbox_messages
            ORDER  BY created_at DESC
            LIMIT  1
            "#
        )
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| {
            let status = match r.status.as_str() {
                "pending" => MessageStatus::Pending,
                "processing" => MessageStatus::Processing,
                "published" => MessageStatus::Published,
                "failed" => MessageStatus::Failed,
                other => {
                    return Err(OutboxError::Config(format!(
                        "unknown message status: {other}"
                    )));
                }
            };
            Ok(OutboxMessage {
                id: r.id,
                event_type: EventType::new(r.event_type)?,
                payload: r.payload,
                aggregate_id: r.aggregate_id,
                status,
                attempts: r.attempts,
                last_error: r.last_error,
                published_at: r.published_at,
                created_at: r.created_at,
            })
        })
        .transpose()
    }
}
