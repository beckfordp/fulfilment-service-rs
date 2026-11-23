// SPDX-License-Identifier: MIT

//! What the service publishes through the outbox on [`TOPIC`] (Scala
//! `FulfilmentEvents`), each written in the transaction that moves the task
//! (`store.rs`), keyed and correlated by the order id: `DispatchConfirmed` on
//! a dispatch, `DispatchFailed` on a failure; the other steps publish nothing.

use schemars::JsonSchema;
use serde::Serialize;

use crate::domain::FulfilmentItem;

/// The topic Debezium routes the outbox to.
pub const TOPIC: &str = "fulfilment-events";

/// The service's name as the envelope's `source`.
pub const SOURCE: &str = "fulfilment-service";

/// The order was dispatched.
pub const DISPATCH_CONFIRMED: &str = "DispatchConfirmed";
/// The order couldn't be dispatched.
pub const DISPATCH_FAILED: &str = "DispatchFailed";

/// Order dispatched: pick/pack/dispatch succeeded. Carries a full item snapshot so
/// consumers need no order lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "DispatchConfirmedEvent")]
pub struct DispatchConfirmed {
    /// Order UUID (order-service's order id).
    #[schemars(example = &"3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")]
    pub order_id: String,
    /// Carrier tracking number.
    #[schemars(example = &"TRK-123456789")]
    pub tracking_no: String,
    /// Items dispatched. Always sent.
    // Optional in the schema, as tapir states a list.
    #[serde(default)]
    pub items: Vec<FulfilmentItem>,
}

/// Dispatch failed: pick/pack/dispatch hit an error. payment-service voids the hold;
/// order-service marks the order failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "DispatchFailedEvent")]
pub struct DispatchFailed {
    /// Order UUID (order-service's order id).
    #[schemars(example = &"3f2b8c1e-5d4a-4e7b-9c6f-1a2b3c4d5e6f")]
    pub order_id: String,
    /// Why dispatch failed.
    #[schemars(example = &"carrier_unavailable")]
    pub reason: String,
}
