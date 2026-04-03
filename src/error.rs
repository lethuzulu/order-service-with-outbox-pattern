use uuid::Uuid;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    #[error("invalid amount: must be greater than zero cents")]
    InvalidAmount,

    #[error("invalid event type: must not be empty")]
    InvalidEventType,

    #[error("order not found: {0}")]
    OrderNotFound(Uuid),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("broker nacked message: {0}")]
    BrokerNack(Uuid),

    #[error("publisher confirms not enabled on channel")]
    ConfirmsNotEnabled,

    #[error("broker connection error: {0}")]
    BrokerConnection(#[from] lapin::Error),

    #[error("configuration error: {0}")]
    Config(String),
}

impl IntoResponse for OutboxError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            OutboxError::InvalidAmount | OutboxError::InvalidEventType => {
                (StatusCode::BAD_REQUEST, self.to_string())
            }
            OutboxError::OrderNotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            _ => {
                tracing::error!(error = %self, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            }
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}
