use uuid::Uuid;

use crate::error::OutboxError;

#[derive(Debug)]
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

#[derive(Debug)]
pub struct OrderId(Uuid);

impl OrderId {
    pub fn new() -> Self {
        OrderId(Uuid::new_v4())
    }
}

#[derive(Debug)]
pub struct CustomerId(Uuid);

impl CustomerId {
    pub fn new() -> Self{
        CustomerId(Uuid::new_v4())
    }
}

#[derive(Debug)]
pub enum OrderStatus {
    Pending,
    Confirmed,
    Cancelled
}

#[derive(Debug)]
pub struct  EventType(String);

#[derive(Debug)]
pub enum MessageStatus {
    Pending, 
    Processing,
    Published,
    Failed
}

