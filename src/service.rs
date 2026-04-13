use crate::{
    db::Db,
    error::OutboxError,
    types::{CustomerId, Money, Order, OrderId},
};

#[derive(Debug, Clone)]
pub struct OrderService {
    db: Db,
}

impl OrderService {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub async fn create_order(
        &self,
        customer_id: CustomerId,
        amount: Money,
    ) -> Result<OrderId, OutboxError> {
        self.db.create_order(customer_id, amount).await
    }

    pub async fn get_order(&self, id: OrderId) -> Result<Option<Order>, OutboxError> {
        self.db.get_order(id).await
    }
}
