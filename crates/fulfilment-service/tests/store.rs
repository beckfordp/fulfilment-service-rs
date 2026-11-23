// SPDX-License-Identifier: MIT

//! The tables in a throwaway Postgres, migrated as the service migrates it:
//! idempotent creation, the list and its filter, a transition and its outbox
//! row in one transaction, the reconciliation flags.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty
)]

mod support;

use std::time::Duration;

use fulfilment_service::domain::{
    Command, Failure, FulfilmentState, ReconciliationStatus, ResolveOutcome,
};
use fulfilment_service::store::{PgTasks, Tasks};
use support::{Postgres, address, items, outbox, pool, task};
use time::macros::datetime;
use uuid::Uuid;

#[tokio::test]
async fn a_task_is_created_once_per_order() {
    let postgres = Postgres::start();
    let store = PgTasks::new(pool(&postgres).await);
    let order = Uuid::new_v4();
    let created = store.create(order, &items(), &address()).await.unwrap();
    assert_eq!(
        (created.state, &created.items, &created.shipping_address),
        (FulfilmentState::Pending, &items(), &address())
    );
    assert_eq!(created.created_at, created.updated_at);
    assert_eq!(
        (
            &created.tracking_no,
            &created.carrier,
            &created.failure_reason
        ),
        (&None, &None, &None)
    );
    let again = store.create(order, &[], &address()).await.unwrap();
    assert_eq!(again, created);
    assert_eq!(store.get(order).await.unwrap(), Some(created));
    assert_eq!(store.get(Uuid::new_v4()).await.unwrap(), None);
}

#[tokio::test]
async fn the_list_is_oldest_first_filtered_by_state() {
    let postgres = Postgres::start();
    let store = support::store(&postgres).await;
    let first = task(&store).await;
    let second = task(&store).await;
    store
        .transition(second, &Command::StartPicking)
        .await
        .unwrap()
        .unwrap();
    let all: Vec<Uuid> = store
        .list(None)
        .await
        .unwrap()
        .iter()
        .map(|t| t.order_id)
        .collect();
    assert_eq!(all, vec![first, second]);
    let picking = store.list(Some(FulfilmentState::Picking)).await.unwrap();
    assert_eq!(
        picking.iter().map(|t| t.order_id).collect::<Vec<_>>(),
        vec![second]
    );
    assert!(
        store
            .list(Some(FulfilmentState::Cancelled))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn dispatch_and_failure_write_their_event_in_the_transaction() {
    let postgres = Postgres::start();
    let pool = pool(&postgres).await;
    let store = PgTasks::new(pool.clone());
    let shipped = task(&store).await;
    for command in [Command::StartPicking, Command::Pack] {
        store.transition(shipped, &command).await.unwrap().unwrap();
    }
    assert!(outbox(&pool, shipped).await.is_empty());
    let dispatch = Command::dispatch("GLN-1", "DHL").unwrap();
    let moved = store.transition(shipped, &dispatch).await.unwrap().unwrap();
    assert_eq!(
        (
            moved.state,
            moved.tracking_no.as_deref(),
            moved.carrier.as_deref()
        ),
        (FulfilmentState::Dispatched, Some("GLN-1"), Some("DHL"))
    );
    assert!(moved.updated_at > moved.created_at);
    let [envelope] = outbox(&pool, shipped).await.try_into().unwrap();
    assert_eq!(envelope["eventType"], "DispatchConfirmed");
    assert_eq!(envelope["source"], "fulfilment-service");
    assert_eq!(envelope["correlationId"], shipped.to_string());
    assert_eq!(
        envelope["payload"],
        serde_json::json!({"orderId": shipped.to_string(), "trackingNo": "GLN-1",
                           "items": serde_json::to_value(items()).unwrap()})
    );
    let failed = task(&store).await;
    let fail = Command::fail("late").unwrap();
    store.transition(failed, &fail).await.unwrap().unwrap();
    let [envelope] = outbox(&pool, failed).await.try_into().unwrap();
    assert_eq!(envelope["eventType"], "DispatchFailed");
    assert_eq!(
        envelope["payload"],
        serde_json::json!({"orderId": failed.to_string(), "reason": "late"})
    );
}

#[tokio::test]
async fn a_refused_transition_changes_and_writes_nothing() {
    let postgres = Postgres::start();
    let pool = pool(&postgres).await;
    let store = PgTasks::new(pool.clone());
    let order = task(&store).await;
    let refused = store.transition(order, &Command::Pack).await.unwrap();
    assert!(
        matches!(
            refused,
            Err(Failure::InvalidTransition {
                from: Some(FulfilmentState::Pending),
                ..
            })
        ),
        "{refused:?}"
    );
    assert_eq!(
        store
            .transition(Uuid::new_v4(), &Command::Pack)
            .await
            .unwrap(),
        Err(Failure::NotFound)
    );
    assert_eq!(
        store.get(order).await.unwrap().unwrap().state,
        FulfilmentState::Pending
    );
    assert!(outbox(&pool, order).await.is_empty());
}

#[tokio::test]
async fn flags_are_first_wins_and_resolved_once() {
    let postgres = Postgres::start();
    let store = support::store(&postgres).await;
    let order = task(&store).await;
    assert!(store.flag(order, "cancel_after_dispatch").await.unwrap());
    assert!(!store.flag(order, "again").await.unwrap());
    assert!(!store.flag(Uuid::new_v4(), "no task").await.unwrap());
    let [open] = store
        .reconciliations(Some(ReconciliationStatus::Open), Some(order))
        .await
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        (open.reason.as_str(), open.status, &open.operator_id),
        ("cancel_after_dispatch", ReconciliationStatus::Open, &None)
    );
    let at = datetime!(2026-11-23 10:00 UTC);
    let ResolveOutcome::Resolved(resolved) =
        store.resolve(order, "op", Some("done"), at).await.unwrap()
    else {
        panic!("not resolved")
    };
    assert_eq!(
        (
            resolved.status,
            resolved.operator_id.as_deref(),
            resolved.note.as_deref(),
            resolved.resolved_at
        ),
        (
            ReconciliationStatus::Resolved,
            Some("op"),
            Some("done"),
            Some(at)
        )
    );
    assert_eq!(
        store.resolve(order, "other", None, at).await.unwrap(),
        ResolveOutcome::AlreadyResolved(resolved.clone())
    );
    assert_eq!(
        store.resolve(Uuid::new_v4(), "op", None, at).await.unwrap(),
        ResolveOutcome::NotFound
    );
    assert_eq!(
        store.reconciliations(None, None).await.unwrap(),
        vec![resolved]
    );
    assert!(
        store
            .reconciliations(Some(ReconciliationStatus::Open), None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn the_ping_follows_postgres() {
    let postgres = Postgres::start();
    let store = support::store(&postgres).await;
    assert!(store.ping(Duration::from_secs(2)).await);
    postgres.stop();
    assert!(!store.ping(Duration::from_secs(2)).await);
}
