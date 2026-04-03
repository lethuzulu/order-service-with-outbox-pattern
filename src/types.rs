use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::error::OutboxError;

#[derive(Debug, Clone, Copy)]
pub struct Money(i64);

impl Money {
    pub fn from_cents(c: i64) -> Result<Self, OutboxError> {
        if c <= 0 {
            return Err(OutboxError::InvalidAmount);
        }
        Ok(Self(c))
    }

    pub fn cents(&self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OrderId(Uuid);

impl OrderId {
    pub fn new() -> Self {
        OrderId(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for OrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CustomerId(Uuid);

impl CustomerId {
    pub fn new() -> Self {
        CustomerId(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl std::fmt::Display for CustomerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct Order {
    pub id: OrderId,
    pub customer_id: CustomerId,
    pub amount: Money,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug)]
pub enum OrderStatus {
    Pending,
    Confirmed,
    Cancelled,
}

impl TryFrom<&str> for OrderStatus {
    type Error = OutboxError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "pending" => Ok(Self::Pending),
            "confirmed" => Ok(Self::Confirmed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(OutboxError::Config(format!("unknown order status: {s}"))),
        }
    }
}
#[derive(Debug)]
pub struct EventType(String);

impl EventType {
    pub fn new(s: impl Into<String>) -> Result<Self, OutboxError> {
        let s = s.into();
        if s.trim().is_empty() {
            return Err(OutboxError::InvalidEventType);
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug)]
pub enum MessageStatus {
    Pending,
    Processing,
    Published,
    Failed,
}
