// SPDX-License-Identifier: MIT

//! The routes, as the Scala `FulfilmentRoutes` and `ReconciliationRoutes`: the
//! path, query and body decoded first (tapir's plain-text 400), then the rules
//! (400 with the JSON reason), then the store. A store failure is a 500.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use service_basics::jvm::java_instant;
use service_basics::text_error::TextError;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    Address, Command, Failure, FulfilmentItem, FulfilmentState, Reconciliation,
    ReconciliationStatus, ResolveOutcome, Task, parse_uuid,
};
use crate::server::AppState;
use crate::store::{StoreError, Tasks};

/// A fulfilment task as stored: order snapshot, lifecycle state and dispatch outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskResponse {
    /// Order id (UUID) from order-service.
    #[schemars(example = &"3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90")]
    pub order_id: String,
    /// Lifecycle state.
    #[schemars(example = &"PICKING")]
    pub state: String,
    /// Order lines snapshotted when the task was created.
    // Optional in the schema, as tapir states a list.
    #[serde(default)]
    pub items: Vec<FulfilmentItem>,
    /// Where the order ships.
    pub shipping_address: Address,
    /// Carrier tracking number; present once DISPATCHED.
    #[schemars(example = &"GLN-1A2B3C4D")]
    pub tracking_no: Option<String>,
    /// Carrier name; present once DISPATCHED.
    #[schemars(example = &"DHL Express")]
    pub carrier: Option<String>,
    /// Why dispatch failed; present once `DISPATCH_FAILED`.
    #[schemars(example = &"carrier_unavailable")]
    pub failure_reason: Option<String>,
    /// When the task was created (UTC).
    #[schemars(example = &"2026-10-31T10:15:30Z")]
    pub created_at: String,
    /// When the task last changed (UTC).
    #[schemars(example = &"2026-10-31T10:20:00Z")]
    pub updated_at: String,
}

impl From<Task> for TaskResponse {
    fn from(task: Task) -> Self {
        Self {
            order_id: task.order_id.to_string(),
            state: task.state.as_str().to_owned(),
            items: task.items,
            shipping_address: task.shipping_address,
            tracking_no: task.tracking_no,
            carrier: task.carrier,
            failure_reason: task.failure_reason,
            created_at: java_instant(task.created_at),
            updated_at: java_instant(task.updated_at),
        }
    }
}

/// Dispatch details: who is carrying the order and its tracking number.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DispatchRequest {
    /// Carrier tracking number; non-blank.
    #[schemars(example = &"GLN-1A2B3C4D")]
    pub tracking_no: String,
    /// Carrier name; non-blank.
    #[schemars(example = &"DHL Express")]
    pub carrier: String,
}

/// Why the task could not be dispatched.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct FailRequest {
    /// Failure reason; non-blank.
    #[schemars(example = &"carrier_unavailable")]
    pub reason: String,
}

/// Demo simulation options; both optional (send `{}` to advance one step).
#[derive(Debug, Clone, Default, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SimulateRequest {
    /// Fail the task instead of advancing it.
    #[schemars(example = true)]
    pub force_failure: Option<bool>,
    /// Failure reason when `forceFailure` is true; defaults to `Simulated dispatch failure`.
    #[schemars(example = &"simulated dispatch failure")]
    pub reason: Option<String>,
}

/// Error body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ErrorResponse {
    /// Human-readable reason.
    #[schemars(example = &"Fulfilment task not found")]
    pub error: String,
}

/// One manual-reconciliation flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationResponse {
    /// Order the flag belongs to.
    pub order_id: String,
    /// Why manual reconciliation is needed.
    pub reason: String,
    /// `open` or `resolved`.
    pub status: String,
    /// When the flag was recorded.
    pub created_at: String,
    /// Operator who resolved it, when resolved.
    pub operator_id: Option<String>,
    /// Operator's note, when supplied.
    pub note: Option<String>,
    /// When it was resolved.
    pub resolved_at: Option<String>,
}

impl From<Reconciliation> for ReconciliationResponse {
    fn from(flag: Reconciliation) -> Self {
        Self {
            order_id: flag.order_id.to_string(),
            reason: flag.reason,
            status: flag.status.as_str().to_owned(),
            created_at: java_instant(flag.created_at),
            operator_id: flag.operator_id,
            note: flag.note,
            resolved_at: flag.resolved_at.map(java_instant),
        }
    }
}

/// Resolve a reconciliation flag.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ResolveReconciliationRequest {
    /// Only `resolved` is accepted.
    pub status: String,
    /// Optional operator note.
    pub note: Option<String>,
}

/// The query parameters the reads take, each optional.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    /// A task state.
    pub state: Option<String>,
    /// A flag status.
    pub status: Option<String>,
    /// An order.
    pub order_id: Option<String>,
}

/// The reconciliation-status 400's reason, whatever status was asked for (as
/// tapir's single error variant maps it).
const ONLY_RESOLVED: &str = "Invalid reconciliation status: only 'resolved' is accepted";

/// simulate-next's carrier.
pub const SIMULATED_CARRIER: &str = "DHL Express";

/// simulate-next's failure reason when none is given.
pub const SIMULATED_FAILURE: &str = "Simulated dispatch failure";

/// Why a route didn't answer its success.
#[derive(Debug)]
pub enum RouteError {
    /// tapir's text 400: what didn't decode (`path parameter orderId`, `body (…)`).
    Decode(String),
    /// A rule's 400, a 401, 404 or 409, with its JSON reason.
    Refused(StatusCode, String),
    /// A failure the route has no error variant for: tapir's 500 (simulate-next's
    /// blank reason).
    Unmapped(String),
    /// Postgres failed.
    Store(StoreError),
}

impl From<StoreError> for RouteError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<Failure> for RouteError {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::NotFound => refused(StatusCode::NOT_FOUND, "Fulfilment task not found"),
            Failure::InvalidTransition { message, .. } => {
                Self::Refused(StatusCode::CONFLICT, message)
            }
            Failure::InvalidRequest(message) => Self::Refused(StatusCode::BAD_REQUEST, message),
        }
    }
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        match self {
            Self::Decode(what) => text(
                StatusCode::BAD_REQUEST,
                format!("Invalid value for: {what}"),
            ),
            Self::Refused(status, error) => (status, Json(ErrorResponse { error })).into_response(),
            Self::Unmapped(_) | Self::Store(_) => text(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_owned(),
            ),
        }
    }
}

fn text(status: StatusCode, message: String) -> Response {
    TextError { status, message }.into_response()
}

fn refused(status: StatusCode, reason: &str) -> RouteError {
    RouteError::Refused(status, reason.to_owned())
}

fn path_id(raw: &str) -> Result<Uuid, RouteError> {
    parse_uuid(raw).ok_or_else(|| RouteError::Decode("path parameter orderId".to_owned()))
}

fn body<T: serde::de::DeserializeOwned>(bytes: &Bytes) -> Result<T, RouteError> {
    serde_json::from_slice(bytes).map_err(|error| RouteError::Decode(format!("body ({error})")))
}

fn authorization(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
}

/// Trimmed, and absent when blank (as the reconciliation routes read their filters).
fn filter(raw: Option<&str>) -> Option<&str> {
    raw.map(str::trim).filter(|text| !text.is_empty())
}

/// Logs a failure the way the Scala routes do, and passes it on.
fn logged<T, S>(state: &AppState<S>, result: Result<T, RouteError>) -> Result<T, RouteError> {
    match &result {
        Err(RouteError::Store(error)) => {
            state
                .logger
                .error(&[("error", &error.to_string())], "Request failed");
        }
        Err(RouteError::Unmapped(reason)) => {
            state.logger.error(&[("error", reason)], "Request failed");
        }
        Err(RouteError::Refused(_, reason)) => {
            state
                .logger
                .warn(&[("reason", reason)], "Fulfilment task rejected");
        }
        _ => {}
    }
    result
}

/// Moves the order's task by `command` (built first, so its 400 comes before the lookup).
async fn run<S: Tasks>(
    state: &AppState<S>,
    order_id: Uuid,
    command: Result<Command, String>,
) -> Result<Json<TaskResponse>, RouteError> {
    let result = async {
        let command =
            command.map_err(|reason| RouteError::Refused(StatusCode::BAD_REQUEST, reason))?;
        let task = state.store.transition(order_id, &command).await??;
        state.logger.info(
            &[
                ("order_id", &order_id.to_string()),
                ("state", task.state.as_str()),
            ],
            "Fulfilment task updated",
        );
        Ok(task)
    }
    .await;
    Ok(Json(logged(state, result)?.into()))
}

/// `GET /fulfilment-tasks?state`: every task oldest first, filtered on the exact state.
///
/// # Errors
/// An unknown state (400); a store failure (500).
pub async fn list<S: Tasks>(
    State(state): State<AppState<S>>,
    Query(query): Query<Params>,
) -> Result<Json<Vec<TaskResponse>>, RouteError> {
    let result = async {
        let filter = query
            .state
            .as_deref()
            .map(|raw| {
                FulfilmentState::parse(raw).ok_or_else(|| {
                    RouteError::Refused(StatusCode::BAD_REQUEST, format!("Unknown state: {raw}"))
                })
            })
            .transpose()?;
        Ok(state.store.list(filter).await?)
    }
    .await;
    let tasks = logged(&state, result)?;
    Ok(Json(tasks.into_iter().map(Into::into).collect()))
}

/// `POST /fulfilment-tasks/{orderId}/start-picking`: PENDING → PICKING.
///
/// # Errors
/// A malformed id (400); no task (404); another state (409); a store failure (500).
pub async fn start_picking<S: Tasks>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
) -> Result<Json<TaskResponse>, RouteError> {
    let order = path_id(&raw)?;
    run(&state, order, Ok(Command::StartPicking)).await
}

/// `POST /fulfilment-tasks/{orderId}/pack`: PICKING → PACKED.
///
/// # Errors
/// A malformed id (400); no task (404); another state (409); a store failure (500).
pub async fn pack<S: Tasks>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
) -> Result<Json<TaskResponse>, RouteError> {
    let order = path_id(&raw)?;
    run(&state, order, Ok(Command::Pack)).await
}

/// `POST /fulfilment-tasks/{orderId}/dispatch`: PACKED → DISPATCHED, publishing
/// `DispatchConfirmed`.
///
/// # Errors
/// A malformed id or body, a blank field (400); no task (404); another state
/// (409); a store failure (500).
pub async fn dispatch<S: Tasks>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<Json<TaskResponse>, RouteError> {
    let order = path_id(&raw)?;
    let request: DispatchRequest = body(&bytes)?;
    run(
        &state,
        order,
        Command::dispatch(&request.tracking_no, &request.carrier),
    )
    .await
}

/// `POST /fulfilment-tasks/{orderId}/fail`: PENDING, PICKING or PACKED →
/// `DISPATCH_FAILED`, publishing `DispatchFailed`.
///
/// # Errors
/// A malformed id or body, a blank reason (400); no task (404); a terminal
/// state (409); a store failure (500).
pub async fn fail<S: Tasks>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<Json<TaskResponse>, RouteError> {
    let order = path_id(&raw)?;
    let request: FailRequest = body(&bytes)?;
    run(&state, order, Command::fail(&request.reason)).await
}

/// simulate-next's tracking number: `GLN-` and the first 8 characters of a random
/// UUID, upper case.
#[must_use]
pub fn tracking_no(id: Uuid) -> String {
    format!("GLN-{}", id.to_string()[..8].to_uppercase())
}

/// `POST /fulfilment-tasks/{orderId}/simulate-next` (demo only): one step
/// forward, minting the dispatch; `forceFailure` fails the task instead.
///
/// # Errors
/// A malformed id or body (400); no task (404); a terminal state (409); a
/// blank forced reason, which the route has no 400 for, and a store failure
/// (500).
pub async fn simulate_next<S: Tasks>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<Json<TaskResponse>, RouteError> {
    let order = path_id(&raw)?;
    let request: SimulateRequest = body(&bytes)?;
    let result = async {
        let task = state.store.get(order).await?.ok_or(Failure::NotFound)?;
        if request.force_failure.unwrap_or(false) {
            let reason = request.reason.as_deref().unwrap_or(SIMULATED_FAILURE);
            return Command::fail(reason).map_err(RouteError::Unmapped);
        }
        let dispatch = Command::dispatch(&tracking_no(Uuid::new_v4()), SIMULATED_CARRIER)
            .map_err(RouteError::Unmapped)?;
        Command::simulated_next(task.state, dispatch).ok_or_else(|| {
            RouteError::Refused(
                StatusCode::CONFLICT,
                format!("Order {order} is already {}", task.state.as_str()),
            )
        })
    }
    .await;
    let command = logged(&state, result)?;
    run(&state, order, Ok(command)).await
}

/// `GET /reconciliations?status&orderId`: flags, newest first; an orderId
/// that isn't a UUID has none.
///
/// # Errors
/// An unknown status (400); a store failure (500).
pub async fn reconciliations<S: Tasks>(
    State(state): State<AppState<S>>,
    Query(query): Query<Params>,
) -> Result<Json<Vec<ReconciliationResponse>>, RouteError> {
    let result = async {
        let status = filter(query.status.as_deref())
            .map(|raw| {
                ReconciliationStatus::parse(raw)
                    .ok_or_else(|| refused(StatusCode::BAD_REQUEST, ONLY_RESOLVED))
            })
            .transpose()?;
        match filter(query.order_id.as_deref()) {
            Some(raw) => match parse_uuid(raw) {
                Some(order) => Ok(state.store.reconciliations(status, Some(order)).await?),
                None => Ok(Vec::new()),
            },
            None => Ok(state.store.reconciliations(status, None).await?),
        }
    }
    .await;
    let flags = logged(&state, result)?;
    Ok(Json(flags.into_iter().map(Into::into).collect()))
}

/// `PATCH /reconciliations/{orderId}`: an operator resolves the flag; the
/// first resolver wins.
///
/// # Errors
/// A malformed id or body (400); no valid operator token (401); a status other
/// than `resolved` (400); no flag (404); a store failure (500).
pub async fn resolve<S: Tasks>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Json<ReconciliationResponse>, RouteError> {
    let order = path_id(&raw)?;
    let request: ResolveReconciliationRequest = body(&bytes)?;
    let result = async {
        let operator = state
            .auth
            .require_operator(authorization(&headers))
            .map_err(|_| refused(StatusCode::UNAUTHORIZED, "Invalid operator token"))?;
        if request.status.trim() != ReconciliationStatus::Resolved.as_str() {
            return Err(refused(StatusCode::BAD_REQUEST, ONLY_RESOLVED));
        }
        let note = request
            .note
            .as_deref()
            .map(str::trim)
            .filter(|note| !note.is_empty());
        match state
            .store
            .resolve(order, operator, note, OffsetDateTime::now_utc())
            .await?
        {
            ResolveOutcome::Resolved(flag) | ResolveOutcome::AlreadyResolved(flag) => Ok(flag),
            ResolveOutcome::NotFound => Err(refused(
                StatusCode::NOT_FOUND,
                "Reconciliation flag not found",
            )),
        }
    }
    .await;
    Ok(Json(logged(&state, result)?.into()))
}
