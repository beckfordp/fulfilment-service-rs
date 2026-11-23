// SPDX-License-Identifier: MIT

//! The service's API description: what tapir generated from the Scala
//! endpoints (capture 20261123 openapi.yaml), stated once here next to the DTO
//! types it derives the schemas from, served with Swagger UI (FUL-D1) —
//! simulate-next listed only when it is served, as tapir lists the endpoints it
//! serves; and the AsyncAPI document of what it publishes through the outbox
//! and the two topics it consumes, with its page (FUL-D2). The resolve route's
//! `Authorization` header parameter is not listed (the builder has path and
//! query parameters only).

use axum::Router;
use serde_json::{Value, json};
use telemetry::docs::asyncapi::{self, Channel};
use telemetry::docs::openapi::{Api, Operation};
use telemetry::docs::swagger;

use crate::domain::Address;
use crate::events::{
    ORDER_CANCELLED, ORDER_EVENTS, OrderCancelled, PAYMENT_AUTHORIZED, PAYMENT_EVENTS,
    PaymentAuthorized,
};
use crate::published::{
    DISPATCH_CONFIRMED, DISPATCH_FAILED, DispatchConfirmed, DispatchFailed, TOPIC,
};
use crate::routes::{
    DispatchRequest, ErrorResponse, FailRequest, ReconciliationResponse,
    ResolveReconciliationRequest, SimulateRequest, TaskResponse,
};

const ORDER: &str = "3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90";
const TAG: &str = "fulfilment-tasks";
const OPERATOR: &str = "The presented operator token is malformed or does not match (US-22 dev \
                        token).";
const NOT_FOUND: &str = "No task exists for this order id.";
const CONFLICT: &str = "The task can't move from its current state (wrong state for this action, or already \
     terminal).";
const INVALID: &str = "Invalid request input: an unknown `state` filter, or a blank \
                       `trackingNo`/`carrier`/`reason`.";

fn task() -> Value {
    json!({
        "orderId": ORDER,
        "state": "DISPATCHED",
        "items": [{
            "sku": "SKU-0042",
            "productName": "Espresso cup",
            "quantity": 2,
            "unitPriceCents": 1250,
        }],
        "shippingAddress": {
            "line1": "1 High Street",
            "line2": "Flat 2",
            "city": "London",
            "postalCode": "SW1A 1AA",
            "country": "GB",
        },
        "trackingNo": "GLN-1A2B3C4D",
        "carrier": "DHL Express",
        "createdAt": "2026-10-31T10:15:30Z",
        "updatedAt": "2026-10-31T10:20:00Z",
    })
}

fn flag() -> Value {
    json!({
        "orderId": ORDER,
        "reason": "cancel_after_dispatch",
        "status": "open",
        "createdAt": "2026-11-02T10:15:30Z",
    })
}

/// A JSON body: its schema component and example. (Always `Some`: the
/// builder takes `None` for a response without one.)
#[allow(clippy::unnecessary_wraps)]
fn body(schema: &'static str, example: Value) -> Option<(&'static str, Value)> {
    Some((schema, example))
}

#[allow(clippy::unnecessary_wraps)]
fn error(message: &str) -> Option<(&'static str, Value)> {
    Some(("ErrorResponse", json!({ "error": message })))
}

/// A step on a task: its path, the task answered, 404 and 409.
fn step(action: &str, summary: &str, description: &str) -> Operation {
    Operation::post(&format!("/fulfilment-tasks/{{orderId}}/{action}"))
        .tag(TAG)
        .summary(summary)
        .description(description)
        .path_parameter(
            "orderId",
            "Order id (UUID) the task was created for. A malformed id returns 400.",
            json!(ORDER),
        )
        .response(200, "", body("TaskResponse", task()))
        .response(404, NOT_FOUND, error("Fulfilment task not found"))
        .response(
            409,
            CONFLICT,
            error(&format!("Cannot transition order {ORDER} from PENDING")),
        )
}

fn tasks(api: Api, simulation: bool) -> Api {
    let api = api
        .operation(
            Operation::get("/fulfilment-tasks")
                .tag(TAG)
                .summary("List fulfilment tasks")
                .description(
                    "Every pick/pack task, oldest first (createdAt). Optional `state` filter; \
                     an unknown value is 400. Read directly from Postgres — no cache, so \
                     results are always current.",
                )
                .query_parameter(
                    "state",
                    "Filter by lifecycle state: `PENDING`, `PICKING`, `PACKED`, `DISPATCHED` \
                     or `DISPATCH_FAILED`.",
                    json!("PENDING"),
                )
                .response(200, "", body("TaskResponse[]", json!([task()])))
                .response(400, INVALID, error("Unknown state: SHIPPED")),
        )
        .operation(step(
            "start-picking",
            "Start picking a task",
            "Moves the task PENDING → PICKING. No event published. 404 if no task exists; 409 \
             from any other state (so repeating it while PICKING is a 409).",
        ))
        .operation(step(
            "pack",
            "Mark a task packed",
            "Moves the task PICKING → PACKED. No event published. 404 if no task exists; 409 \
             from any other state.",
        ))
        .operation(
            step(
                "dispatch",
                "Dispatch a task",
                "Moves the task PACKED → DISPATCHED, records the tracking number and carrier, \
                 and publishes `DispatchConfirmed` through the transactional outbox. Blank \
                 `trackingNo`/`carrier` → 400. 404 if no task exists; 409 from any other state. \
                 Not idempotent — a repeat is 409.",
            )
            .request_body(
                "DispatchRequest",
                json!({"trackingNo": "GLN-1A2B3C4D", "carrier": "DHL Express"}),
            )
            .response(400, INVALID, error("Unknown state: SHIPPED")),
        )
        .operation(
            step(
                "fail",
                "Fail a task",
                "Moves the task to DISPATCH_FAILED from PENDING, PICKING or PACKED, records the \
                 reason, and publishes `DispatchFailed` through the transactional outbox. Blank \
                 `reason` → 400. 404 if no task exists; 409 from a terminal state.",
            )
            .request_body("FailRequest", json!({"reason": "carrier_unavailable"}))
            .response(400, INVALID, error("Unknown state: SHIPPED")),
        );
    if !simulation {
        return api;
    }
    api.operation(
        step(
            "simulate-next",
            "Advance a task one step (simulation)",
            "Demo-only; registered only when `fulfilment.simulation.enabled`. Advances PENDING \
             → PICKING → PACKED → DISPATCHED, minting a `GLN-XXXXXXXX` tracking number and \
             carrier `DHL Express`; with `forceFailure` it fails the task instead. 404 if no \
             task exists; 409 from a terminal state.",
        )
        .request_body(
            "SimulateRequest",
            json!({"forceFailure": true, "reason": "simulated dispatch failure"}),
        ),
    )
}

fn reconciliations(api: Api) -> Api {
    let only = "Only `resolved` is accepted.";
    api.operation(
        Operation::get("/reconciliations")
            .tag("reconciliations")
            .summary("List reconciliation flags")
            .description(
                "The manual-reconciliation queue, newest first. Both filters are optional and \
                 ANDed; an unknown status → 400 and an unknown orderId returns an empty list. \
                 Unauthenticated local/demo read (ADM-4).",
            )
            .query_parameter("status", "open or resolved (default: both).", json!("open"))
            .query_parameter("orderId", "Only this order's flag.", json!(ORDER))
            .response(200, "", body("ReconciliationResponse[]", json!([flag()])))
            .response(400, only, error("Invalid reconciliation status: 'open'")),
    )
    .operation(
        Operation::patch("/reconciliations/{orderId}")
            .tag("reconciliations")
            .summary("Resolve a reconciliation flag")
            .description(
                "Marks an open flag resolved, recording the operator id from the token, an \
                 optional note and the timestamp. The first resolver wins: a repeat is an \
                 idempotent 200 and never overwrites the original. 401 without a valid token, \
                 404 when no flag exists.",
            )
            .path_parameter(
                "orderId",
                "Order id (UUID) whose flag to resolve.",
                json!(ORDER),
            )
            .request_body(
                "ResolveReconciliationRequest",
                json!({"status": "resolved", "note": "Aborted after dispatch"}),
            )
            .response(200, "", body("ReconciliationResponse", flag()))
            .response(400, only, error("Invalid reconciliation status: 'open'"))
            .response(401, OPERATOR, error("Invalid operator token"))
            .response(
                404,
                "No flag for this order.",
                error("Reconciliation flag not found"),
            ),
    )
}

/// The OpenAPI document: the task list and its steps (simulate-next when
/// `simulation`), reconciliations, then the two health checks.
#[must_use]
pub fn api(simulation: bool) -> Api {
    let api = Api::new("Fulfilment Service", "1.0")
        .schema::<Address>()
        .schema::<DispatchRequest>()
        .schema::<ErrorResponse>()
        .schema::<FailRequest>()
        .schema::<ReconciliationResponse>()
        .schema::<ResolveReconciliationRequest>()
        .schema::<SimulateRequest>()
        .schema::<TaskResponse>();
    reconciliations(tasks(api, simulation))
        .operation(
            Operation::get("/health")
                .tag("health")
                .summary("Liveness probe")
                .description("`200` while the process is up. Polled by Kubernetes.")
                .response(200, "", None),
        )
        .operation(
            Operation::get("/health/ready")
                .tag("health")
                .summary("Readiness probe")
                .description(
                    "`200` when Postgres is reachable, `503` otherwise. Polled by Kubernetes.",
                )
                .response(200, "", None)
                .response(503, "Postgres is unreachable.", None),
        )
}

/// The AsyncAPI document: the two `fulfilment-events` sent, `PaymentAuthorized`
/// and `OrderCancelled` received.
#[must_use]
pub fn asyncapi() -> Value {
    asyncapi::document(
        "Fulfilment Service",
        "1.0",
        &[
            Channel::publish_cdc::<DispatchConfirmed>(TOPIC, DISPATCH_CONFIRMED),
            Channel::publish_cdc::<DispatchFailed>(TOPIC, DISPATCH_FAILED),
            Channel::subscribe::<PaymentAuthorized>(PAYMENT_EVENTS, PAYMENT_AUTHORIZED),
            Channel::subscribe::<OrderCancelled>(ORDER_EVENTS, ORDER_CANCELLED),
        ],
    )
}

/// Swagger UI at `/docs/` (`/docs/docs.yaml`), and the AsyncAPI document and page.
pub fn router(simulation: bool) -> Router {
    swagger::router(api(simulation).to_yaml()).merge(asyncapi::router_with_page(&asyncapi()))
}
