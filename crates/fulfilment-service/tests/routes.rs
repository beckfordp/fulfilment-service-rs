// SPDX-License-Identifier: MIT

//! The routes in process over the Postgres store (or one that fails): the
//! list, the steps and what they refuse, simulate-next, reconciliations, the
//! edge (CORS, 404, 405).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use axum::body::Body;
use axum::http::Request;
use fulfilment_service::server;
use fulfilment_service::store::{PgTasks, Tasks};
use serde_json::json;
use support::{Broken, Postgres, call, messages, pool, state, task};
use tower::ServiceExt;
use uuid::Uuid;

const MISSING: &str = "00000000-0000-4000-8000-000000000000";
const TOKEN: &str = "Bearer dev-operator-token";

async fn store() -> (Postgres, PgTasks) {
    let postgres = Postgres::start();
    let store = PgTasks::new(pool(&postgres).await);
    (postgres, store)
}

#[tokio::test]
async fn the_list_is_every_task_oldest_first_and_filters_on_the_exact_state() {
    let (_postgres, store) = store().await;
    let first = task(&store).await;
    let second = task(&store).await;
    let (state, _) = state(store);
    call(
        &state,
        "POST",
        &format!("/fulfilment-tasks/{second}/start-picking"),
        None,
        &[],
    )
    .await;
    let listed = call(&state, "GET", "/fulfilment-tasks", None, &[]).await;
    assert_eq!(listed.status, 200);
    let body = listed.json();
    let ids: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["orderId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![first.to_string(), second.to_string()]);
    assert_eq!(body[0]["state"], "PENDING");
    assert_eq!(body[0]["trackingNo"], serde_json::Value::Null);
    assert_eq!(body[0]["items"][0]["productName"], "Fulfilment Mug");
    assert_eq!(body[0]["shippingAddress"]["postalCode"], "SW1A 1AA");
    let picking = call(&state, "GET", "/fulfilment-tasks?state=PICKING", None, &[])
        .await
        .json();
    assert_eq!(picking.as_array().unwrap().len(), 1);
    for raw in ["pending", "", "SHIPPED"] {
        let refused = call(
            &state,
            "GET",
            &format!("/fulfilment-tasks?state={raw}"),
            None,
            &[],
        )
        .await;
        assert_eq!(refused.status, 400, "{raw:?}");
        assert_eq!(
            refused.json(),
            json!({"error": format!("Unknown state: {raw}")})
        );
    }
}

#[tokio::test]
async fn steps_move_the_task_and_refuse_what_its_state_doesnt_allow() {
    let (_postgres, store) = store().await;
    let order = task(&store).await;
    let (state, logs) = state(store);
    let step = |action: &str| format!("/fulfilment-tasks/{order}/{action}");
    let packed_early = call(&state, "POST", &step("pack"), None, &[]).await;
    assert_eq!(
        (packed_early.status, packed_early.json()),
        (
            409,
            json!({"error": format!("Cannot transition order {order} from PENDING")})
        )
    );
    assert_eq!(
        call(&state, "POST", &step("start-picking"), None, &[])
            .await
            .json()["state"],
        "PICKING"
    );
    assert_eq!(
        call(&state, "POST", &step("pack"), None, &[]).await.json()["state"],
        "PACKED"
    );
    let dispatched = call(
        &state,
        "POST",
        &step("dispatch"),
        Some(r#"{"trackingNo":" GLN-1 ","carrier":" DHL "}"#),
        &[],
    )
    .await;
    assert_eq!(dispatched.status, 200);
    let body = dispatched.json();
    assert_eq!(
        (&body["state"], &body["trackingNo"], &body["carrier"]),
        (&json!("DISPATCHED"), &json!("GLN-1"), &json!("DHL"))
    );
    let fail = call(
        &state,
        "POST",
        &step("fail"),
        Some(r#"{"reason":"late"}"#),
        &[],
    )
    .await;
    assert_eq!(fail.status, 409);
    assert!(messages(&logs).contains(&"Fulfilment task updated".to_owned()));
    assert!(messages(&logs).contains(&"Fulfilment task rejected".to_owned()));
}

#[tokio::test]
async fn bad_input_is_400_before_the_task_is_looked_up() {
    let (_postgres, store) = store().await;
    let order = task(&store).await;
    let (state, _) = state(store);
    let malformed = call(
        &state,
        "POST",
        "/fulfilment-tasks/nope/start-picking",
        None,
        &[],
    )
    .await;
    assert_eq!(
        (malformed.status, malformed.text.as_str()),
        (400, "Invalid value for: path parameter orderId")
    );
    assert_eq!(
        malformed.content_type.as_deref(),
        Some("text/plain; charset=UTF-8")
    );
    for target in [order.to_string(), MISSING.to_owned()] {
        let blank = call(
            &state,
            "POST",
            &format!("/fulfilment-tasks/{target}/dispatch"),
            Some(r#"{"trackingNo":" ","carrier":"D"}"#),
            &[],
        )
        .await;
        assert_eq!(
            (blank.status, blank.json()),
            (400, json!({"error": "trackingNo and carrier are required"}))
        );
        let reason = call(
            &state,
            "POST",
            &format!("/fulfilment-tasks/{target}/fail"),
            Some(r#"{"reason":"  "}"#),
            &[],
        )
        .await;
        assert_eq!(
            (reason.status, reason.json()),
            (400, json!({"error": "reason is required"}))
        );
    }
    for raw in [r#"{"trackingNo":"G"}"#, r#"{"trackingNo":"#] {
        let undecodable = call(
            &state,
            "POST",
            &format!("/fulfilment-tasks/{order}/dispatch"),
            Some(raw),
            &[],
        )
        .await;
        assert_eq!(undecodable.status, 400);
        assert!(
            undecodable.text.starts_with("Invalid value for: body ("),
            "{}",
            undecodable.text
        );
    }
    for action in ["start-picking", "pack"] {
        let missing = call(
            &state,
            "POST",
            &format!("/fulfilment-tasks/{MISSING}/{action}"),
            None,
            &[],
        )
        .await;
        assert_eq!(
            (missing.status, missing.json()),
            (404, json!({"error": "Fulfilment task not found"}))
        );
    }
}

#[tokio::test]
async fn simulate_next_walks_to_dispatched_or_forces_a_failure() {
    let (_postgres, store) = store().await;
    let order = task(&store).await;
    let forced = task(&store).await;
    let blank = task(&store).await;
    let (state, _) = state(store);
    let simulate = |id: Uuid, body: &'static str| {
        let state = state.clone();
        async move {
            call(
                &state,
                "POST",
                &format!("/fulfilment-tasks/{id}/simulate-next"),
                Some(body),
                &[],
            )
            .await
        }
    };
    let mut states = Vec::new();
    for body in ["{}", "{}", r#"{"forceFailure":false}"#] {
        states.push(simulate(order, body).await.json()["state"].clone());
    }
    assert_eq!(
        states,
        vec![json!("PICKING"), json!("PACKED"), json!("DISPATCHED")]
    );
    let last = simulate(order, "{}").await;
    assert_eq!(
        (last.status, last.json()),
        (
            409,
            json!({"error": format!("Order {order} is already DISPATCHED")})
        )
    );
    let listed = call(
        &state,
        "GET",
        "/fulfilment-tasks?state=DISPATCHED",
        None,
        &[],
    )
    .await
    .json();
    let tracking = listed[0]["trackingNo"].as_str().unwrap().to_owned();
    assert!(
        tracking.starts_with("GLN-") && tracking.len() == 12,
        "{tracking}"
    );
    assert_eq!(tracking[4..].to_uppercase(), tracking[4..]);
    assert_eq!(listed[0]["carrier"], "DHL Express");
    let failed = simulate(forced, r#"{"forceFailure":true}"#).await.json();
    assert_eq!(
        (&failed["state"], &failed["failureReason"]),
        (
            &json!("DISPATCH_FAILED"),
            &json!("Simulated dispatch failure")
        )
    );
    let unmapped = simulate(blank, r#"{"forceFailure":true,"reason":" "}"#).await;
    assert_eq!(
        (unmapped.status, unmapped.text.as_str()),
        (500, "Internal server error")
    );
    assert_eq!(simulate(Uuid::new_v4(), "{}").await.status, 404);
    let no_body = call(
        &state,
        "POST",
        &format!("/fulfilment-tasks/{order}/simulate-next"),
        None,
        &[],
    )
    .await;
    assert_eq!(no_body.status, 400);
    assert!(
        no_body.text.starts_with("Invalid value for: body ("),
        "{}",
        no_body.text
    );
}

#[tokio::test]
async fn simulate_next_is_not_served_when_off() {
    let (state, _) = state(Broken);
    let request = Request::builder()
        .method("POST")
        .uri(format!("/fulfilment-tasks/{MISSING}/simulate-next"))
        .body(Body::from("{}"))
        .unwrap();
    let response = server::app(state, false).oneshot(request).await.unwrap();
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn reconciliations_list_and_resolve_first_wins() {
    let (_postgres, store) = store().await;
    let order = task(&store).await;
    store.flag(order, "cancel_after_dispatch").await.unwrap();
    let (state, _) = state(store);
    let listed = call(
        &state,
        "GET",
        &format!("/reconciliations?status=open&orderId={order}"),
        None,
        &[],
    )
    .await
    .json();
    assert_eq!(listed[0]["reason"], "cancel_after_dispatch");
    assert_eq!(listed[0]["operatorId"], serde_json::Value::Null);
    for query in ["orderId=nope", &format!("orderId={MISSING}")] {
        assert_eq!(
            call(
                &state,
                "GET",
                &format!("/reconciliations?{query}"),
                None,
                &[]
            )
            .await
            .json(),
            json!([])
        );
    }
    let bad = call(&state, "GET", "/reconciliations?status=closed", None, &[]).await;
    assert_eq!(
        (bad.status, bad.json()),
        (
            400,
            json!({"error": "Invalid reconciliation status: only 'resolved' is accepted"})
        )
    );
    let path = format!("/reconciliations/{order}");
    let resolve = |body: &'static str, headers: &'static [(&'static str, &'static str)]| {
        let (state, path) = (state.clone(), path.clone());
        async move { call(&state, "PATCH", &path, Some(body), headers).await }
    };
    assert_eq!(resolve(r#"{"status":"resolved"}"#, &[]).await.status, 401);
    assert_eq!(
        resolve(r#"{"status":"resolved"}"#, &[("Authorization", "Bearer x")])
            .await
            .json(),
        json!({"error": "Invalid operator token"})
    );
    assert_eq!(
        resolve(r#"{"status":"open"}"#, &[("Authorization", TOKEN)])
            .await
            .status,
        400
    );
    let first = resolve(
        r#"{"status":"resolved","note":" ok "}"#,
        &[("Authorization", "bearer  dev-operator-token ")],
    )
    .await;
    assert_eq!(first.status, 200);
    let body = first.json();
    assert_eq!(
        (&body["status"], &body["operatorId"], &body["note"]),
        (&json!("resolved"), &json!("dev-operator"), &json!("ok"))
    );
    assert_eq!(
        resolve(
            r#"{"status":"resolved","note":"two"}"#,
            &[("Authorization", TOKEN)]
        )
        .await
        .json(),
        body
    );
    let missing = call(
        &state,
        "PATCH",
        &format!("/reconciliations/{MISSING}"),
        Some(r#"{"status":"resolved"}"#),
        &[("Authorization", TOKEN)],
    )
    .await;
    assert_eq!(
        (missing.status, missing.json()),
        (404, json!({"error": "Reconciliation flag not found"}))
    );
}

#[tokio::test]
async fn a_failing_store_is_a_plain_500() {
    let (state, logs) = state(Broken);
    let paths = [
        ("GET", "/fulfilment-tasks".to_owned(), None),
        ("POST", format!("/fulfilment-tasks/{MISSING}/pack"), None),
        (
            "POST",
            format!("/fulfilment-tasks/{MISSING}/simulate-next"),
            Some("{}"),
        ),
        ("GET", "/reconciliations".to_owned(), None),
    ];
    for (method, path, body) in paths {
        let answered = call(&state, method, &path, body, &[]).await;
        assert_eq!(
            (answered.status, answered.text.as_str()),
            (500, "Internal server error"),
            "{path}"
        );
    }
    let resolved = call(
        &state,
        "PATCH",
        &format!("/reconciliations/{MISSING}"),
        Some(r#"{"status":"resolved"}"#),
        &[("Authorization", TOKEN)],
    )
    .await;
    assert_eq!(resolved.status, 500);
    assert!(messages(&logs).contains(&"Request failed".to_owned()));
}

#[tokio::test]
async fn the_edge_answers_cors_404_and_405() {
    let (state, _) = state(Broken);
    let preflight = Request::builder()
        .method("OPTIONS")
        .uri(format!("/fulfilment-tasks/{MISSING}/dispatch"))
        .header("Origin", "http://localhost:5173")
        .header("Access-Control-Request-Method", "POST")
        .header(
            "Access-Control-Request-Headers",
            "Content-Type, Authorization",
        )
        .body(Body::empty())
        .unwrap();
    let response = server::app(state.clone(), true)
        .oneshot(preflight)
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned()
    };
    assert_eq!(header("access-control-allow-origin"), "*");
    assert!(
        header("access-control-allow-headers")
            .to_lowercase()
            .contains("authorization")
    );
    let simple = Request::builder()
        .uri("/nowhere")
        .header("Origin", "http://localhost:5173")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        server::app(state.clone(), true)
            .oneshot(simple)
            .await
            .unwrap()
            .status()
            .as_u16(),
        404
    );
    assert_eq!(
        call(
            &state,
            "GET",
            &format!("/fulfilment-tasks/{MISSING}/pack"),
            None,
            &[]
        )
        .await
        .status,
        405
    );
}

#[tokio::test]
async fn the_docs_describe_the_routes_served_and_the_events() {
    let (state, _) = state(Broken);
    assert_eq!(call(&state, "GET", "/docs/", None, &[]).await.status, 200);
    let yaml = call(&state, "GET", "/docs/docs.yaml", None, &[]).await.text;
    for path in [
        "/fulfilment-tasks",
        "/fulfilment-tasks/{orderId}/dispatch",
        "/fulfilment-tasks/{orderId}/simulate-next",
        "/reconciliations/{orderId}",
        "TaskResponse",
    ] {
        assert!(yaml.contains(path), "{path} in {yaml}");
    }
    assert!(
        !fulfilment_service::docs::api(false)
            .to_yaml()
            .contains("simulate-next")
    );
    let asyncapi = call(&state, "GET", "/docs/asyncapi.json", None, &[])
        .await
        .json();
    let mut names: Vec<_> = asyncapi["channels"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    names.sort();
    assert_eq!(
        names,
        ["fulfilment-events", "order-events", "payment-events"]
    );
    let page = call(&state, "GET", "/docs/asyncapi/", None, &[]).await.text;
    for name in [
        "DispatchConfirmed",
        "DispatchFailed",
        "PaymentAuthorized",
        "OrderCancelled",
    ] {
        assert!(page.contains(name), "{name}");
    }
}

#[tokio::test]
async fn the_asyncapi_payloads_require_what_scala_requires() {
    let (state, _) = state(Broken);
    let document = call(&state, "GET", "/docs/asyncapi.json", None, &[])
        .await
        .json();
    let required = |channel: &str, message: &str| {
        let mut fields: Vec<String> = document["channels"][channel]["messages"][message]["payload"]
            ["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f.as_str().unwrap().to_owned())
            .collect();
        fields.sort();
        fields
    };
    assert_eq!(
        required("fulfilment-events", "DispatchConfirmed"),
        ["orderId", "trackingNo"]
    );
    assert_eq!(
        required("fulfilment-events", "DispatchFailed"),
        ["orderId", "reason"]
    );
    assert_eq!(
        required("payment-events", "PaymentAuthorized"),
        ["authId", "expiresAt", "orderId", "timestamp"]
    );
    assert_eq!(
        required("order-events", "OrderCancelled"),
        ["customerId", "orderId", "timestamp"]
    );
}
