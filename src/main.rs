use axum::{Json, Router, extract::State, routing::{get, post}};
use order_service_with_outbox_pattern::{
    db::Db,
    error::OutboxError,
    poller::{Poller, PollerConfig},
    publisher::Publisher,
    service::OrderService,
    types::{CustomerId, Money, OrderId},
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::watch;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt::layer().json())
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let amqp_url = std::env::var("AMQP_URL").expect("AMQP_URL must be set");

    let db = Db::new(&database_url).await?;
    db.migrate().await?;

    let publisher = Arc::new(Publisher::connect(&amqp_url, "orders").await?);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Poller runs as a background task, independent of the HTTP server.
    // It only touches outbox_messages — never the orders table.
    let poller_db = db.clone();
    let poller_pub = Arc::clone(&publisher);
    let poller_handle = tokio::spawn(async move {
        let poller = Poller::new(poller_db, PollerConfig::default());
        if let Err(e) = poller.run(poller_pub, shutdown_rx).await {
            tracing::error!(error = %e, "poller exited with error");
        }
    });

    let service = OrderService::new(db);

    let app = Router::new()
        .route("/orders", post(create_order_handler))
        .route("/orders/:id", get(get_order_handler))
        .with_state(service);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "order service listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("SIGINT received");
        })
        .await?;

    // Signal poller to finish its current batch and exit
    shutdown_tx.send(true)?;
    poller_handle.await?;

    tracing::info!("shutdown complete");

    Ok(())
}

#[derive(serde::Deserialize)]
struct CreateOrderRequest {
    customer_id: uuid::Uuid,
    amount: i64,
}

async fn create_order_handler(
    State(service): State<OrderService>,
    Json(req): Json<CreateOrderRequest>,
) -> Result<Json<serde_json::Value>, OutboxError> {
    let customer_id = CustomerId::from_uuid(req.customer_id);
    let amount = Money::from_cents(req.amount)?;

    let order_id = service.create_order(customer_id, amount).await?;

    tracing::info!(%order_id, "order created");

    Ok(Json(serde_json::json!({ "order_id": order_id })))
}

async fn get_order_handler(
    State(service): State<OrderService>,
    axum::extract::Path(id): axum::extract::Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, OutboxError> {
    let order_id = OrderId::from_uuid(id);
    match service.get_order(order_id).await? {
        Some(order) => Ok(Json(serde_json::json!({
            "order_id":    order.id,
            "customer_id": order.customer_id,
            "amount":      order.amount.cents(),
            "status":      order.status.to_string(),
            "created_at":  order.created_at,
        }))),
        None => Err(OutboxError::OrderNotFound(id)),
    }
}
