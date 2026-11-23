// SPDX-License-Identifier: MIT

//! The consumers' routes, as the Scala `PaymentAuthorizedConsumer` and
//! `OrderCancelledConsumer`. `PaymentAuthorized` looks the order up in
//! order-service and creates its task (idempotent); an order order-service
//! doesn't know or can't ship is skipped, a failing lookup or store
//! dead-lettered, and what it can't decode skipped (Scala decodes it outside
//! the DLQ guard). `OrderCancelled` aborts a task that hasn't dispatched and
//! flags one that has; what it can't decode is dead-lettered (Scala guards
//! it), as is a failing store.

use kafka_event_consumer::envelope::{Envelope, Routes};
use schemars::JsonSchema;
use serde::Deserialize;
use telemetry::logging::Logger;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::clients::{CallError, OrderLookup, Orders};
use crate::domain::{Command, Failure, FulfilmentState, parse_uuid};
use crate::store::{StoreError, Tasks};

/// The topic payment outcomes come on.
pub const PAYMENT_EVENTS: &str = "payment-events";
/// Its consumer group.
pub const PAYMENT_GROUP: &str = "fulfilment-service-payment-events";
/// The topic order outcomes come on.
pub const ORDER_EVENTS: &str = "order-events";
/// Its consumer group.
pub const ORDER_GROUP: &str = "fulfilment-service-order-events";

/// Funds held: the event that starts pick/pack.
pub const PAYMENT_AUTHORIZED: &str = "PaymentAuthorized";
/// The order was cancelled.
pub const ORDER_CANCELLED: &str = "OrderCancelled";

/// The flag an `OrderCancelled` after dispatch records.
pub const CANCEL_AFTER_DISPATCH: &str = "cancel_after_dispatch";

/// Funds held: POST /payments/authorize succeeded. Consumed by fulfilment-service to
/// start pick/pack.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "PaymentAuthorizedEvent")]
pub struct PaymentAuthorized {
    /// Order UUID (order-service's order id).
    #[schemars(example = &"3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")]
    pub order_id: String,
    /// Payment provider's authorization reference.
    #[schemars(example = &"auth-7f3a9c")]
    pub auth_id: String,
    /// When the hold lapses if never captured (ISO-8601 instant).
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String", example = &"2026-11-07T10:15:30Z")]
    pub expires_at: OffsetDateTime,
    /// When the authorization was made (ISO-8601 instant).
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String", example = &"2026-10-31T10:15:30Z")]
    pub timestamp: OffsetDateTime,
}

/// Order cancelled: a customer cancel or a consumed `PaymentAuthorizationExpired` moved
/// the order to cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "OrderCancelledEvent")]
pub struct OrderCancelled {
    /// Order UUID (order-service's order id).
    #[schemars(example = &"3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")]
    pub order_id: String,
    /// Customer who placed the order.
    #[schemars(example = &"7c9e6679-7425-40de-944b-e07fc1f90ae7")]
    pub customer_id: String,
    /// When the order was cancelled (ISO-8601 instant).
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String", example = &"2026-10-31T10:15:30Z")]
    pub timestamp: OffsetDateTime,
    /// Cancellation trigger: customer | `auth_expired` | stale | operator.
    #[serde(default)]
    #[schemars(example = &"customer")]
    pub reason: Option<String>,
}

/// Why an event wasn't handled.
#[derive(Debug, thiserror::Error)]
pub enum EventsError {
    /// The payload isn't the event's.
    #[error(transparent)]
    Payload(#[from] serde_json::Error),
    /// The payload's orderId isn't a UUID.
    #[error("Invalid order id: '{0}'")]
    OrderId(String),
    /// order-service gave no answer: retried, then dead-lettered.
    #[error(transparent)]
    Lookup(#[from] CallError),
    /// The store failed: retried, then dead-lettered.
    #[error(transparent)]
    Store(#[from] StoreError),
}

fn decoded<T: serde::de::DeserializeOwned>(
    envelope: &Envelope,
    order_id: impl Fn(&T) -> &str,
) -> Result<(Uuid, T), EventsError> {
    let event: T = serde_json::from_value(envelope.payload.clone())?;
    let raw = order_id(&event);
    let id = parse_uuid(raw).ok_or_else(|| EventsError::OrderId(raw.to_owned()))?;
    Ok((id, event))
}

/// `payment-events`: `PaymentAuthorized` → the order's task.
#[derive(Debug, Clone)]
pub struct PaymentEvents<S> {
    store: S,
    orders: Orders,
    logger: Logger,
}

impl<S> PaymentEvents<S> {
    /// Over `store`, looking orders up through `orders`, logging to `logger`.
    #[must_use]
    pub fn new(store: S, orders: Orders, logger: Logger) -> Self {
        Self {
            store,
            orders,
            logger,
        }
    }
}

impl<S: Tasks> Routes for PaymentEvents<S> {
    type Event = Uuid;
    type Error = EventsError;
    const DEAD_LETTERS_POISON: bool = false;

    fn decode(&self, envelope: &Envelope) -> Option<Result<Uuid, EventsError>> {
        (envelope.event_type == PAYMENT_AUTHORIZED)
            .then(|| decoded(envelope, |e: &PaymentAuthorized| &e.order_id).map(|(id, _)| id))
    }

    async fn handle(&self, _: &str, order_id: &Uuid) -> Result<(), EventsError> {
        let order = order_id.to_string();
        match self.orders.get(*order_id).await? {
            OrderLookup::NotFound => self.logger.warn(
                &[("order_id", &order)],
                "PaymentAuthorized for an unknown order - skipping",
            ),
            OrderLookup::NoShippingAddress => self.logger.error(
                &[("order_id", &order)],
                "PaymentAuthorized for an order with no shipping address - skipping",
            ),
            OrderLookup::Found(items, address) => {
                let task = self.store.create(*order_id, &items, &address).await?;
                self.logger.info(
                    &[("order_id", &order), ("state", task.state.as_str())],
                    "Fulfilment task ready",
                );
            }
        }
        Ok(())
    }
}

/// `order-events`: `OrderCancelled` → the task aborted, or flagged once dispatched.
#[derive(Debug, Clone)]
pub struct OrderEvents<S> {
    store: S,
    logger: Logger,
}

impl<S> OrderEvents<S> {
    /// Over `store`, logging to `logger`.
    #[must_use]
    pub fn new(store: S, logger: Logger) -> Self {
        Self { store, logger }
    }
}

impl<S: Tasks> Routes for OrderEvents<S> {
    type Event = (Uuid, Option<String>);
    type Error = EventsError;

    fn decode(&self, envelope: &Envelope) -> Option<Result<Self::Event, EventsError>> {
        (envelope.event_type == ORDER_CANCELLED).then(|| {
            decoded(envelope, |e: &OrderCancelled| &e.order_id).map(|(id, e)| (id, e.reason))
        })
    }

    async fn handle(&self, _: &str, (order_id, reason): &Self::Event) -> Result<(), EventsError> {
        let order = order_id.to_string();
        let reason = reason.as_deref().unwrap_or("-");
        let context = [("order_id", order.as_str()), ("reason", reason)];
        match self.store.transition(*order_id, &Command::Cancel).await? {
            Ok(task) => self.logger.info(
                &[context[0], context[1], ("state", task.state.as_str())],
                "Fulfilment task aborted on order cancellation",
            ),
            Err(Failure::NotFound) => self.logger.warn(
                &context,
                "OrderCancelled for an unknown fulfilment task - skipping",
            ),
            Err(Failure::InvalidTransition {
                message,
                from: Some(FulfilmentState::Dispatched),
            }) => {
                self.logger.warn(
                    &[context[0], context[1], ("detail", &message)],
                    "OrderCancelled after dispatch - reconciliation flagged",
                );
                self.store.flag(*order_id, CANCEL_AFTER_DISPATCH).await?;
            }
            Err(Failure::InvalidTransition { message, .. } | Failure::InvalidRequest(message)) => {
                self.logger.info(
                    &[context[0], context[1], ("detail", &message)],
                    "OrderCancelled for a task that already dispatched or was cancelled - skipping",
                );
            }
        }
        Ok(())
    }
}
