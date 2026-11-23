// SPDX-License-Identifier: MIT

//! The two consumers' routes, less the stream, over the Postgres store (or one
//! that fails) and an order-service stand-in: which envelopes they act on,
//! what each does to the task, what each skips and what fails (to be
//! dead-lettered). The consumers against a broker are tests/contract's.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unused_async_trait_impl
)]

mod support;

use std::sync::Arc;

use fulfilment_service::consumers::{LAG_INTERVAL, settings};
use fulfilment_service::domain::{Command, FulfilmentState, ReconciliationStatus};
use fulfilment_service::events::{
    EventsError, ORDER_EVENTS, ORDER_GROUP, OrderEvents, PAYMENT_EVENTS, PAYMENT_GROUP,
    PaymentEvents,
};
use fulfilment_service::store::{PgTasks, Tasks};
use kafka_event_consumer::envelope::{Envelope, Routes};
use serde_json::{Value, json};
use support::{
    Broken, Callee, Postgres, address, captured_logger, items, messages, orders, pool, task,
};
use uuid::Uuid;

fn envelope(event_type: &str, payload: &Value) -> Envelope {
    serde_json::from_value(json!({
        "eventId": "e1",
        "eventType": event_type,
        "timestamp": "2026-11-23T10:00:00Z",
        "source": "test",
        "correlationId": "c1",
        "payload": payload,
    }))
    .unwrap()
}

const AT: &str = "2026-11-23T10:00:00Z";

fn authorized(order: &str) -> Value {
    json!({"orderId": order, "authId": "a1", "expiresAt": AT, "timestamp": AT})
}

fn cancelled(order: &str) -> Value {
    json!({"orderId": order, "customerId": "c1", "timestamp": AT, "reason": "customer"})
}

#[tokio::test]
async fn payment_events_decode_payment_authorized_only() {
    let callee = Callee::start().await;
    let (logger, _) = captured_logger();
    let route = PaymentEvents::new(Broken, orders(&callee), logger);
    let order = Uuid::new_v4();
    let decoded = route
        .decode(&envelope(
            "PaymentAuthorized",
            &authorized(&order.to_string()),
        ))
        .unwrap()
        .unwrap();
    assert_eq!(decoded, order);
    assert!(
        route
            .decode(&envelope("PaymentCaptured", &json!({})))
            .is_none()
    );
    for payload in [
        json!({"orderId": order.to_string()}),
        authorized("not-a-uuid"),
    ] {
        assert!(
            route
                .decode(&envelope("PaymentAuthorized", &payload))
                .unwrap()
                .is_err(),
            "{payload}"
        );
    }
    const { assert!(!PaymentEvents::<Broken>::DEAD_LETTERS_POISON) };
}

#[tokio::test]
async fn payment_authorized_creates_the_task_from_the_orders_snapshot_once() {
    let postgres = Postgres::start();
    let store = PgTasks::new(pool(&postgres).await);
    let callee = Callee::start().await;
    let (logger, logs) = captured_logger();
    let route = PaymentEvents::new(store.clone(), orders(&callee), logger);
    let order = Uuid::new_v4();
    route.handle("e1", &order).await.unwrap();
    route.handle("e2", &order).await.unwrap();
    let created = store.get(order).await.unwrap().unwrap();
    assert_eq!(
        (created.state, created.items, created.shipping_address),
        (FulfilmentState::Pending, items(), address())
    );
    assert_eq!(callee.seen(), vec![format!("/orders/{order}"); 2]);
    assert!(messages(&logs).contains(&"Fulfilment task ready".to_owned()));
}

#[tokio::test]
async fn an_order_order_service_doesnt_know_or_cant_ship_is_skipped() {
    let postgres = Postgres::start();
    let store = PgTasks::new(pool(&postgres).await);
    let callee = Callee::start().await;
    let (logger, logs) = captured_logger();
    let route = PaymentEvents::new(store.clone(), orders(&callee), logger);
    let unknown = Uuid::new_v4();
    callee.answering(Arc::new(|_| {
        (404, r#"{"error":"Order not found"}"#.to_owned())
    }));
    route.handle("e1", &unknown).await.unwrap();
    let unaddressed = Uuid::new_v4();
    callee.answering(Arc::new(|path| {
        let id = path.trim_start_matches("/orders/");
        (
            200,
            json!({"id": id, "items": [], "shippingAddress": null}).to_string(),
        )
    }));
    route.handle("e2", &unaddressed).await.unwrap();
    assert_eq!(store.get(unknown).await.unwrap(), None);
    assert_eq!(store.get(unaddressed).await.unwrap(), None);
    let said = messages(&logs);
    assert!(said.contains(&"PaymentAuthorized for an unknown order - skipping".to_owned()));
    assert!(said.iter().any(|m| m.contains("no shipping address")));
}

#[tokio::test]
async fn a_failing_lookup_or_store_fails_the_event() {
    let callee = Callee::start().await;
    let (logger, _) = captured_logger();
    let order = Uuid::new_v4();
    let broken_store = PaymentEvents::new(Broken, orders(&callee), logger.clone());
    assert!(matches!(
        broken_store.handle("e1", &order).await,
        Err(EventsError::Store(_))
    ));
    for answer in [(500, String::new()), (200, "{\"id\":\"x\"}".to_owned())] {
        let callee = Callee::start().await;
        callee.answering(Arc::new(move |_| answer.clone()));
        let route = PaymentEvents::new(Broken, orders(&callee), logger.clone());
        let error = route.handle("e1", &order).await.unwrap_err();
        assert!(matches!(error, EventsError::Lookup(_)), "{error}");
    }
}

#[tokio::test]
async fn order_events_decode_order_cancelled_and_dead_letter_poison() {
    let (logger, _) = captured_logger();
    let route = OrderEvents::new(Broken, logger);
    let order = Uuid::new_v4();
    let decoded = route
        .decode(&envelope("OrderCancelled", &cancelled(&order.to_string())))
        .unwrap()
        .unwrap();
    assert_eq!(decoded, (order, Some("customer".to_owned())));
    let no_reason = json!({"orderId": order.to_string(), "customerId": "c", "timestamp": AT});
    assert_eq!(
        route
            .decode(&envelope("OrderCancelled", &no_reason))
            .unwrap()
            .unwrap(),
        (order, None)
    );
    assert!(
        route
            .decode(&envelope("OrderConfirmed", &json!({})))
            .is_none()
    );
    for payload in [json!({"orderId": order.to_string()}), cancelled("nope")] {
        assert!(
            route
                .decode(&envelope("OrderCancelled", &payload))
                .unwrap()
                .is_err()
        );
    }
    const { assert!(OrderEvents::<Broken>::DEAD_LETTERS_POISON) };
    assert!(matches!(
        route.handle("e1", &(order, None)).await,
        Err(EventsError::Store(_))
    ));
}

#[tokio::test]
async fn order_cancelled_aborts_an_open_task_and_flags_a_dispatched_one() {
    let postgres = Postgres::start();
    let store = PgTasks::new(pool(&postgres).await);
    let (logger, logs) = captured_logger();
    let route = OrderEvents::new(store.clone(), logger);
    let open = task(&store).await;
    let shipped = task(&store).await;
    for command in [
        Command::StartPicking,
        Command::Pack,
        Command::dispatch("GLN-1", "DHL").unwrap(),
    ] {
        store.transition(shipped, &command).await.unwrap().unwrap();
    }
    let failed = task(&store).await;
    store
        .transition(failed, &Command::fail("x").unwrap())
        .await
        .unwrap()
        .unwrap();
    for order in [open, shipped, shipped, failed, open, Uuid::new_v4()] {
        route.handle("e", &(order, None)).await.unwrap();
    }
    let state = |order| {
        let store = store.clone();
        async move { store.get(order).await.unwrap().unwrap().state }
    };
    assert_eq!(state(open).await, FulfilmentState::Cancelled);
    assert_eq!(state(shipped).await, FulfilmentState::Dispatched);
    assert_eq!(state(failed).await, FulfilmentState::DispatchFailed);
    let flags = store.reconciliations(None, None).await.unwrap();
    assert_eq!(flags.len(), 1);
    assert_eq!(
        (flags[0].order_id, flags[0].reason.as_str(), flags[0].status),
        (shipped, "cancel_after_dispatch", ReconciliationStatus::Open)
    );
    let said = messages(&logs);
    for message in [
        "Fulfilment task aborted on order cancellation",
        "OrderCancelled after dispatch - reconciliation flagged",
        "OrderCancelled for an unknown fulfilment task - skipping",
    ] {
        assert!(said.contains(&message.to_owned()), "{message}");
    }
}

#[test]
fn each_consumer_has_its_topic_and_group() {
    for (topic, group) in [(PAYMENT_EVENTS, PAYMENT_GROUP), (ORDER_EVENTS, ORDER_GROUP)] {
        let settings = settings("kafka:9092", topic, group).unwrap();
        assert_eq!(settings.lag_interval, LAG_INTERVAL);
        assert_eq!(settings.bootstrap, "kafka:9092");
    }
    assert_eq!(PAYMENT_GROUP, "fulfilment-service-payment-events");
    assert_eq!(ORDER_GROUP, "fulfilment-service-order-events");
    assert!(settings("kafka:9092", "bad topic!", PAYMENT_GROUP).is_err());
}
