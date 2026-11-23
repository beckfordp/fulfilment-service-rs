// SPDX-License-Identifier: MIT

//! The fulfilment-service binary: the Rust port of gluon's Scala
//! fulfilment-service (ADR 0029). Pick/pack/dispatch tasks made from
//! `PaymentAuthorized` and order-service's snapshot, aborted by
//! `OrderCancelled`, moved by the operator API (or simulate-next), each
//! dispatch outcome published through the outbox.
pub mod clients;
pub mod config;
pub mod consumers;
pub mod db;
pub mod docs;
pub mod domain;
pub mod events;
pub mod lifecycle;
pub mod published;
pub mod routes;
pub mod server;
pub mod store;

/// The package version, as built.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
