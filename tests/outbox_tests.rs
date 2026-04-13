use order_service_with_outbox_pattern::{
    db::Db,
    error::OutboxError,
    poller::{Poller, PollerConfig},
    publisher::Publisher,
    types::{CustomerId, MessageStatus, Money},
};
use serial_test::serial;
use std::{collections::HashSet, sync::Arc, time::Duration};
use uuid::Uuid;

async fn setup() -> Db {
    dotenvy::dotenv().ok();
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL required for tests");
    let db = Db::new(&url).await.unwrap();

    // Clean state between tests
    sqlx::query("DELETE FROM outbox_messages")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM orders")
        .execute(&db.pool)
        .await
        .unwrap();

    db
}

fn customer() -> CustomerId {
    CustomerId::new()
}

fn ten_cents() -> Money {
    Money::from_cents(1000).unwrap()
}

fn fast_poller(db: Db) -> Poller {
    Poller::new(
        db,
        PollerConfig {
            batch_size: 10,
            poll_interval: Duration::from_millis(50),
            lock_secs: 30,
        },
    )
}

#[tokio::test]
#[serial]
async fn create_order_writes_both_rows_atomically() {
    let db = setup().await;

    let order_id = db.create_order(customer(), ten_cents()).await.unwrap();

    let order = db.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(order.id, order_id);
    assert_eq!(order.amount.cents(), 1000);

    let orders_count = db.count_orders().await.unwrap();
    let pending_count = db.count_outbox_by_status("pending").await.unwrap();
    assert_eq!(orders_count, 1, "exactly one order");
    assert_eq!(pending_count, 1, "exactly one pending outbox message");

    // Outbox payload contains the correct order_id
    let msg = db.latest_outbox_message().await.unwrap().unwrap();
    assert_eq!(msg.event_type.as_str(), "order.created");
    assert_eq!(msg.aggregate_id, order_id.to_string());
    assert_eq!(
        msg.payload["order_id"].as_str().unwrap(),
        order_id.as_uuid().to_string()
    );
}

#[tokio::test]
#[serial]
async fn the_dual_write_gap_is_visible_naively_and_closed_by_outbox() {
    let db = setup().await;

    sqlx::query!(
        "INSERT INTO orders (id, customer_id, amount) VALUES ($1, $2, $3)",
        Uuid::new_v4(),
        Uuid::new_v4(),
        1000i64,
    )
    .execute(&db.pool)
    .await
    .unwrap();

    let orders_before = db.count_orders().await.unwrap();
    let pending_before = db.count_outbox_by_status("pending").await.unwrap();
    assert_eq!(orders_before, 1, "order exists (the gap)");
    assert_eq!(
        pending_before, 0,
        "no outbox message — the fulfilment service never hears about this order"
    );

    sqlx::query("DELETE FROM orders")
        .execute(&db.pool)
        .await
        .unwrap();

    let _: Result<_, _> = db
        .with_transaction(|mut tx| async move {
            Db::insert_order(&mut tx, customer(), ten_cents()).await?;
            Err::<((), sqlx::Transaction<'_, sqlx::Postgres>), OutboxError>(OutboxError::Config(
                "simulated crash".into(),
            ))
        })
        .await;

    let orders_after = db.count_orders().await.unwrap();
    let pending_after = db.count_outbox_by_status("pending").await.unwrap();

    assert_eq!(
        orders_after, 0,
        "with outbox pattern: order does not exist after failure"
    );
    assert_eq!(
        pending_after, 0,
        "with outbox pattern: the gap cannot exist"
    );
}

#[tokio::test]
#[serial]
async fn poller_publishes_order_message_with_correct_payload() {
    let db = setup().await;

    let amqp_url = std::env::var("AMQP_URL").expect("AMQP_URL required for this test");
    let publisher = Arc::new(
        Publisher::connect(&amqp_url, "orders")
            .await
            .expect("failed to connect to RabbitMQ"),
    );

    let order_id = db.create_order(customer(), ten_cents()).await.unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let poller_db = db.clone();
    let pub_ref = Arc::clone(&publisher);

    let handle =
        tokio::spawn(async move { fast_poller(poller_db).run(pub_ref, shutdown_rx).await });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let published = db.count_outbox_by_status("published").await.unwrap();
        if published == 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timeout: message was not published within 5 seconds");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    let msg = db.latest_outbox_message().await.unwrap().unwrap();
    assert_eq!(msg.status, MessageStatus::Published);
    assert_eq!(
        msg.payload["order_id"].as_str().unwrap(),
        order_id.as_uuid().to_string(),
        "payload must contain the correct order_id"
    );
    assert_eq!(msg.aggregate_id, order_id.to_string());

    let order = db.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(
        order.amount.cents(),
        1000,
        "order amount unchanged by poller"
    );
}

#[tokio::test]
#[serial]
async fn concurrent_pollers_never_claim_same_message() {
    let db = setup().await;

    for _ in 0..10 {
        db.create_order(customer(), ten_cents()).await.unwrap();
    }

    assert_eq!(
        db.count_outbox_by_status("pending").await.unwrap(),
        10,
        "expected 10 pending messages before polling"
    );

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..3 {
        let db = db.clone();
        set.spawn(async move { db.poll(10, 30).await.unwrap() });
    }

    let mut all_ids: Vec<Uuid> = Vec::new();
    while let Some(result) = set.join_next().await {
        for msg in result.unwrap() {
            all_ids.push(msg.id);
        }
    }

    let unique: HashSet<_> = all_ids.iter().collect();

    assert_eq!(
        unique.len(),
        all_ids.len(),
        "duplicate message IDs claimed — FOR UPDATE SKIP LOCKED is not working"
    );

    assert_eq!(
        all_ids.len(),
        10,
        "all 10 messages must be claimed across the 3 pollers"
    );
}

#[tokio::test]
#[serial]
async fn publish_failure_marks_message_failed_and_leaves_order_intact() {
    let db = setup().await;

    let order_id = db.create_order(customer(), ten_cents()).await.unwrap();

    let mut messages = db.poll(10, 30).await.unwrap();
    assert_eq!(messages.len(), 1, "expected exactly one message to process");
    let msg = messages.remove(0);

    db.mark_failed(msg.id, "simulated broker rejection")
        .await
        .unwrap();

    let failed = db.count_outbox_by_status("failed").await.unwrap();
    let processing = db.count_outbox_by_status("processing").await.unwrap();
    assert_eq!(failed, 1, "message must be in failed status");
    assert_eq!(processing, 0, "no messages stuck in processing");

    let stored = db.latest_outbox_message().await.unwrap().unwrap();
    assert_eq!(stored.status, MessageStatus::Failed);
    assert_eq!(
        stored.last_error.as_deref(),
        Some("simulated broker rejection"),
        "last_error must record the failure reason"
    );

    let order = db.get_order(order_id).await.unwrap().unwrap();
    assert_eq!(
        order.amount.cents(),
        1000,
        "order amount unchanged by publish failure"
    );
    assert_eq!(
        order.status.to_string(),
        "pending",
        "order status unchanged"
    );
}
