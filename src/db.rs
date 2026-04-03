use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    error::OutboxError,
    types::{CustomerId, EventType, Money, Order, OrderId, OrderStatus},
};

pub struct Db {
    pool: PgPool,
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

        Db::insert_outbox_message(&mut tx,
            &EventType::new("order.created")?, 
            json!({"order_id": order_id.as_uuid(), "customer_id": customer_id.as_uuid(), "amount": amount.cents()}), 
            &order_id.to_string()
        ).await?;

        Ok((order_id, tx))
        }).await
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
            amount.cents()
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
