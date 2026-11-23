// SPDX-License-Identifier: MIT

//! order-service's order lookup (Scala `OrderClient`), through purerest's
//! resilience (gluon-rs `telemetry`'s `ResilientClient`: retry outside the
//! circuit breaker, `application.conf`'s 3 retries from 100 ms and 5 failures
//! for 30 s). A 404 or an order without a shipping address is a value;
//! anything else (5xx after the retries, an open breaker, a transport
//! failure, a body it can't read) is a [`CallError`], which the consumer's
//! DLQ guard retries and dead-letters.

use std::time::Duration;

use serde::Deserialize;
use telemetry::client::{ClientError, ResilientClient, TracedClient};
use telemetry::metrics::Metrics;
use telemetry::resilience::{CircuitBreakerConfig, RetryConfig};
use telemetry::tracing::SdkTracer;
use uuid::Uuid;

use crate::domain::{Address, FulfilmentItem};

/// Each request's timeout.
pub const TIMEOUT: Duration = Duration::from_secs(10);

/// `application.conf`'s `order-client.resilience.retry`.
pub const RETRY: RetryConfig = RetryConfig {
    max_retries: 3,
    base_delay: Duration::from_millis(100),
};

/// `application.conf`'s `order-client.resilience.circuit-breaker`.
pub const BREAKER: CircuitBreakerConfig = CircuitBreakerConfig {
    failure_threshold: 5,
    reset_timeout: Duration::from_secs(30),
};

/// A lookup that gave no answer.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// No response: an open breaker or a transport failure.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// A status other than 200 or 404, as the Scala message words it.
    #[error("order-service GET /orders/{order_id} returned {status}")]
    Unexpected {
        /// The order looked up.
        order_id: Uuid,
        /// The status code.
        status: u16,
    },
    /// A 200 whose body isn't an order.
    #[error("{0}")]
    Body(String),
}

/// The client: order-service's base URL behind its own breaker.
///
/// # Errors
/// When the HTTP client can't be built.
pub fn resilient(tracer: SdkTracer, metrics: &Metrics) -> Result<ResilientClient, reqwest::Error> {
    Ok(ResilientClient::new(
        TracedClient::new(TIMEOUT, tracer, metrics)?,
        RETRY,
        BREAKER,
        metrics,
    ))
}

/// What order-service said about an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderLookup {
    /// The order's lines and where it ships.
    Found(Vec<FulfilmentItem>, Address),
    /// order-service doesn't know it (404).
    NotFound,
    /// It has no shipping address (an order from before US-11.1).
    NoShippingAddress,
}

// Every field required, as circe decodes them; the ids aren't kept.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderItemBody {
    #[serde(rename = "id")]
    _id: String,
    sku: String,
    product_name: String,
    unit_price_cents: i32,
    quantity: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderBody {
    #[serde(rename = "id")]
    _id: String,
    items: Vec<OrderItemBody>,
    shipping_address: Option<Address>,
}

/// order-service's `GET /orders/{id}`.
#[derive(Debug, Clone)]
pub struct Orders {
    client: ResilientClient,
    base_url: String,
}

impl Orders {
    /// Calls `base_url` through `client`.
    #[must_use]
    pub fn new(base_url: &str, client: ResilientClient) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
        }
    }

    /// The order's snapshot.
    ///
    /// # Errors
    /// A [`CallError`].
    pub async fn get(&self, order_id: Uuid) -> Result<OrderLookup, CallError> {
        let url = format!("{}/orders/{order_id}", self.base_url);
        let response = self.client.get(&url).await?;
        match response.status().as_u16() {
            200 => {
                let order: OrderBody = response
                    .json()
                    .await
                    .map_err(|error| CallError::Body(error.to_string()))?;
                Ok(match order.shipping_address {
                    None => OrderLookup::NoShippingAddress,
                    Some(address) => OrderLookup::Found(
                        order
                            .items
                            .into_iter()
                            .map(|item| FulfilmentItem {
                                sku: item.sku,
                                product_name: item.product_name,
                                quantity: item.quantity,
                                unit_price_cents: item.unit_price_cents,
                            })
                            .collect(),
                        address,
                    ),
                })
            }
            404 => Ok(OrderLookup::NotFound),
            status => Err(CallError::Unexpected { order_id, status }),
        }
    }
}
