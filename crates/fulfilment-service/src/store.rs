// SPDX-License-Identifier: MIT

//! The tables, as the Scala `FulfilmentStore` uses them (the same SQL): the
//! tasks, created idempotently per order; the one state-changing call, the
//! task locked, moved by the lifecycle and its outbox event written in the
//! transaction; the reconciliation flags. Handlers reach them through the
//! [`Tasks`] trait, so they are testable with a store that fails
//! (`tests/support`); [`PgTasks`] is the Postgres one.

use service_basics::outbox;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool, Row};
use std::future::Future;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    Address, Command, Failure, FulfilmentItem, FulfilmentState, Reconciliation,
    ReconciliationStatus, ResolveOutcome, Task,
};
use crate::published::{
    DISPATCH_CONFIRMED, DISPATCH_FAILED, DispatchConfirmed, DispatchFailed, SOURCE,
};

/// Why a store call failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database refused or failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// An event payload didn't serialise.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// What the service needs from its tables.
pub trait Tasks: Clone + Send + Sync + 'static {
    /// A PENDING task for the order with its lines and address; the existing
    /// one, untouched, when there is one (a redelivered `PaymentAuthorized`).
    fn create(
        &self,
        order_id: Uuid,
        items: &[FulfilmentItem],
        address: &Address,
    ) -> impl Future<Output = Result<Task, StoreError>> + Send;

    /// The order's task, if there is one.
    fn get(&self, order_id: Uuid) -> impl Future<Output = Result<Option<Task>, StoreError>> + Send;

    /// Every task in `state` (or every task), oldest first.
    fn list(
        &self,
        state: Option<FulfilmentState>,
    ) -> impl Future<Output = Result<Vec<Task>, StoreError>> + Send;

    /// Moves the order's task by `command`, writing its event (dispatch,
    /// failure) to the outbox in the transaction; the task as moved, or why it
    /// didn't move.
    fn transition(
        &self,
        order_id: Uuid,
        command: &Command,
    ) -> impl Future<Output = Result<Result<Task, Failure>, StoreError>> + Send;

    /// Flags the order's task for manual reconciliation, first wins; false
    /// when already flagged or there's no task.
    fn flag(
        &self,
        order_id: Uuid,
        reason: &str,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Flags, newest first, by status and order.
    fn reconciliations(
        &self,
        status: Option<ReconciliationStatus>,
        order_id: Option<Uuid>,
    ) -> impl Future<Output = Result<Vec<Reconciliation>, StoreError>> + Send;

    /// Resolves the order's open flag as `operator`; the first resolver wins.
    fn resolve(
        &self,
        order_id: Uuid,
        operator: &str,
        note: Option<&str>,
        at: OffsetDateTime,
    ) -> impl Future<Output = Result<ResolveOutcome, StoreError>> + Send;

    /// True when the database answers within `within` (readiness).
    fn ping(&self, within: Duration) -> impl Future<Output = bool> + Send;
}

/// The tables in Postgres.
#[derive(Debug, Clone)]
pub struct PgTasks {
    pool: PgPool,
}

impl PgTasks {
    /// Over `pool`, migrated.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const TASK_COLUMNS: &str = "order_id, state, items, shipping_address, tracking_no, carrier, \
     failure_reason, created_at, updated_at";

const RECONCILIATION_COLUMNS: &str =
    "order_id, reason, status, created_at, operator_id, note, resolved_at";

struct TaskRow(Task);

impl FromRow<'_, PgRow> for TaskRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let state: String = row.try_get("state")?;
        let state = FulfilmentState::parse(&state).ok_or_else(|| decode("state", &state))?;
        let Json(items) = row.try_get("items")?;
        let Json(shipping_address) = row.try_get("shipping_address")?;
        Ok(Self(Task {
            order_id: row.try_get("order_id")?,
            state,
            items,
            shipping_address,
            tracking_no: row.try_get("tracking_no")?,
            carrier: row.try_get("carrier")?,
            failure_reason: row.try_get("failure_reason")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        }))
    }
}

struct ReconciliationRow(Reconciliation);

impl FromRow<'_, PgRow> for ReconciliationRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let status: String = row.try_get("status")?;
        let status =
            ReconciliationStatus::parse(&status).ok_or_else(|| decode("status", &status))?;
        Ok(Self(Reconciliation {
            order_id: row.try_get("order_id")?,
            reason: row.try_get("reason")?,
            status,
            created_at: row.try_get("created_at")?,
            operator_id: row.try_get("operator_id")?,
            note: row.try_get("note")?,
            resolved_at: row.try_get("resolved_at")?,
        }))
    }
}

fn decode(column: &str, value: &str) -> sqlx::Error {
    sqlx::Error::ColumnDecode {
        index: column.to_owned(),
        source: format!("unknown value {value:?}").into(),
    }
}

/// The outbox event `command` publishes for the moved `task`, if any (Scala
/// `FulfilmentEvents.forCommand`).
fn event_for(
    task: &Task,
    command: &Command,
    now: OffsetDateTime,
) -> Result<Option<outbox::OutboxEvent>, serde_json::Error> {
    let order = task.order_id.to_string();
    let envelope = |event_type: &str, payload: &serde_json::Value| {
        outbox::envelope(event_type, SOURCE, &order, &order, payload, now)
    };
    match command {
        Command::Dispatch { tracking_no, .. } => {
            let payload = serde_json::to_value(DispatchConfirmed {
                order_id: order.clone(),
                tracking_no: tracking_no.clone(),
                items: task.items.clone(),
            })?;
            envelope(DISPATCH_CONFIRMED, &payload).map(Some)
        }
        Command::Fail(reason) => {
            let payload = serde_json::to_value(DispatchFailed {
                order_id: order.clone(),
                reason: reason.clone(),
            })?;
            envelope(DISPATCH_FAILED, &payload).map(Some)
        }
        Command::StartPicking | Command::Pack | Command::Cancel => Ok(None),
    }
}

impl Tasks for PgTasks {
    async fn create(
        &self,
        order_id: Uuid,
        items: &[FulfilmentItem],
        address: &Address,
    ) -> Result<Task, StoreError> {
        let inserted: Option<TaskRow> = sqlx::query_as(&format!(
            "insert into \"fulfilment_task\" (order_id, state, items, shipping_address) \
             values ($1, 'PENDING', $2, $3) on conflict (order_id) do nothing \
             returning {TASK_COLUMNS}"
        ))
        .bind(order_id)
        .bind(Json(items))
        .bind(Json(address))
        .fetch_optional(&self.pool)
        .await?;
        if let Some(row) = inserted {
            return Ok(row.0);
        }
        let existing: TaskRow = sqlx::query_as(&format!(
            "select {TASK_COLUMNS} from \"fulfilment_task\" where order_id = $1"
        ))
        .bind(order_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(existing.0)
    }

    async fn get(&self, order_id: Uuid) -> Result<Option<Task>, StoreError> {
        let row: Option<TaskRow> = sqlx::query_as(&format!(
            "select {TASK_COLUMNS} from \"fulfilment_task\" where order_id = $1"
        ))
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| row.0))
    }

    async fn list(&self, state: Option<FulfilmentState>) -> Result<Vec<Task>, StoreError> {
        let rows: Vec<TaskRow> = sqlx::query_as(&format!(
            "select {TASK_COLUMNS} from \"fulfilment_task\" \
             where ($1::text is null or state = $1) order by created_at"
        ))
        .bind(state.map(FulfilmentState::as_str))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    async fn transition(
        &self,
        order_id: Uuid,
        command: &Command,
    ) -> Result<Result<Task, Failure>, StoreError> {
        let mut tx = self.pool.begin().await?;
        let before: Option<TaskRow> = sqlx::query_as(&format!(
            "select {TASK_COLUMNS} from \"fulfilment_task\" where order_id = $1 for update"
        ))
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(TaskRow(before)) = before else {
            return Ok(Err(Failure::NotFound));
        };
        let now = OffsetDateTime::now_utc();
        let next = match before.apply(command, now) {
            Ok(next) => next,
            Err(failure) => return Ok(Err(failure)),
        };
        let TaskRow(updated) = sqlx::query_as(&format!(
            "update \"fulfilment_task\" set state = $1, tracking_no = $2, carrier = $3, \
             failure_reason = $4, updated_at = now() where order_id = $5 \
             returning {TASK_COLUMNS}"
        ))
        .bind(next.state.as_str())
        .bind(&next.tracking_no)
        .bind(&next.carrier)
        .bind(&next.failure_reason)
        .bind(order_id)
        .fetch_one(&mut *tx)
        .await?;
        if let Some(event) = event_for(&updated, command, now)? {
            // No span yet to parent consumers' traces.
            outbox::write(&mut tx, &event, None).await?;
        }
        tx.commit().await?;
        Ok(Ok(updated))
    }

    async fn flag(&self, order_id: Uuid, reason: &str) -> Result<bool, StoreError> {
        let flagged: Option<Uuid> = sqlx::query_scalar(
            "insert into fulfilment_reconciliation (order_id, reason) select $1, $2 \
             where exists (select 1 from \"fulfilment_task\" where order_id = $1) \
             on conflict (order_id) do nothing returning order_id",
        )
        .bind(order_id)
        .bind(reason)
        .fetch_optional(&self.pool)
        .await?;
        Ok(flagged.is_some())
    }

    async fn reconciliations(
        &self,
        status: Option<ReconciliationStatus>,
        order_id: Option<Uuid>,
    ) -> Result<Vec<Reconciliation>, StoreError> {
        let rows: Vec<ReconciliationRow> = sqlx::query_as(&format!(
            "select {RECONCILIATION_COLUMNS} from fulfilment_reconciliation \
             where ($1::text is null or status = $1) and ($2::uuid is null or order_id = $2) \
             order by created_at desc, order_id"
        ))
        .bind(status.map(ReconciliationStatus::as_str))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    async fn resolve(
        &self,
        order_id: Uuid,
        operator: &str,
        note: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<ResolveOutcome, StoreError> {
        let mut tx = self.pool.begin().await?;
        let resolved: Option<ReconciliationRow> = sqlx::query_as(&format!(
            "update fulfilment_reconciliation set status = 'resolved', operator_id = $1, \
             note = $2, resolved_at = $3 where order_id = $4 and status = 'open' \
             returning {RECONCILIATION_COLUMNS}"
        ))
        .bind(operator)
        .bind(note)
        .bind(at)
        .bind(order_id)
        .fetch_optional(&mut *tx)
        .await?;
        let outcome = if let Some(row) = resolved {
            ResolveOutcome::Resolved(row.0)
        } else {
            let existing: Option<ReconciliationRow> = sqlx::query_as(&format!(
                "select {RECONCILIATION_COLUMNS} from fulfilment_reconciliation \
                 where order_id = $1"
            ))
            .bind(order_id)
            .fetch_optional(&mut *tx)
            .await?;
            existing.map_or(ResolveOutcome::NotFound, |row| {
                ResolveOutcome::AlreadyResolved(row.0)
            })
        };
        tx.commit().await?;
        Ok(outcome)
    }

    async fn ping(&self, within: Duration) -> bool {
        service_basics::postgres::ping(&self.pool, within)
            .await
            .is_ok()
    }
}
