use axum::{Json, Router, extract::State, routing::post};
use order_service_with_outbox_pattern::{
    db::Db,
    error::OutboxError,
    poller::{Poller, PollerConfig},
    publisher::{Publisher},
    types::{CustomerId, Money},
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

    let app = Router::new()
        .route("/orders", post(create_order_handler))
        .with_state(db);

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
    State(db): State<Db>,
    Json(req): Json<CreateOrderRequest>,
) -> Result<Json<serde_json::Value>, OutboxError> {
    let customer_id = CustomerId::from_uuid(req.customer_id);
    let amount = Money::from_cents(req.amount)?;

    let order_id = db.create_order(customer_id, amount).await?;

    tracing::info!(%order_id, "order created");

    Ok(Json(serde_json::json!({ "order_id": order_id })))
}
