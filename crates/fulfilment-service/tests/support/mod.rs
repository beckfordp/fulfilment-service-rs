// SPDX-License-Identifier: MIT

//! Shared by the integration tests: gluon-rs `service-testing`'s throwaway
//! PostgreSQL cluster, this service's store over it and its binary under
//! test, a store that fails, a local HTTP server standing in for
//! order-service, the app called in process, and a logger whose lines the
//! test can read back.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unused_async_trait_impl
)]

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{Request, StatusCode, Uri, header};
use axum::response::IntoResponse;
use fulfilment_service::clients::{self, Orders};
use fulfilment_service::config::{PORT_VARIABLE, PostgresConfig};
use fulfilment_service::db;
use fulfilment_service::domain::{
    Address, Command, Failure, FulfilmentItem, FulfilmentState, OperatorAuth, Reconciliation,
    ReconciliationStatus, ResolveOutcome, Task,
};
use fulfilment_service::server::{self, AppState};
use fulfilment_service::store::{PgTasks, StoreError, Tasks};
use serde_json::{Value, json};
use sqlx::PgPool;
use telemetry::logging::Logger;
use telemetry::metrics::Metrics;
use telemetry::tracing::Tracing;
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

// Not every test file uses both.
#[allow(unused_imports)]
pub use service_testing::{Postgres, free_port};

/// The fulfilment-service binary, on a fresh port.
pub struct Service(service_testing::Service);

impl Service {
    /// Starts it with exactly `extra` (plus its port); does not wait.
    pub fn spawn(extra: &[(&str, String)]) -> Self {
        Self(service_testing::Service::spawn(
            env!("CARGO_BIN_EXE_fulfilment-service"),
            PORT_VARIABLE,
            extra,
        ))
    }

    /// Starts it and waits until `/health` answers.
    pub fn start(extra: &[(&str, String)]) -> Self {
        Self(service_testing::Service::start(
            env!("CARGO_BIN_EXE_fulfilment-service"),
            PORT_VARIABLE,
            extra,
        ))
    }
}

impl Deref for Service {
    type Target = service_testing::Service;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Service {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The service's store over `postgres`, migrated as the service migrates it.
pub async fn store(postgres: &Postgres) -> PgTasks {
    PgTasks::new(pool(postgres).await)
}

/// A pool on `postgres`, migrated as the service migrates it.
pub async fn pool(postgres: &Postgres) -> PgPool {
    let env: HashMap<_, _> = postgres.env().into_iter().collect();
    let config = PostgresConfig {
        host: env["POSTGRES_HOST"].clone(),
        port: env["POSTGRES_PORT"].parse().unwrap(),
        database: env["POSTGRES_DB"].clone(),
        user: env["POSTGRES_USER"].clone(),
        password: env["POSTGRES_PASSWORD"].clone(),
    };
    let pool = db::connect(&config).await.unwrap();
    db::migrate(&pool).await.unwrap();
    pool
}

/// A store whose every call fails as a database would, and whose ping fails.
#[derive(Clone, Default)]
pub struct Broken;

fn broken<T>() -> Result<T, StoreError> {
    Err(StoreError::Database(sqlx::Error::PoolClosed))
}

impl Tasks for Broken {
    async fn create(&self, _: Uuid, _: &[FulfilmentItem], _: &Address) -> Result<Task, StoreError> {
        broken()
    }

    async fn get(&self, _: Uuid) -> Result<Option<Task>, StoreError> {
        broken()
    }

    async fn list(&self, _: Option<FulfilmentState>) -> Result<Vec<Task>, StoreError> {
        broken()
    }

    async fn transition(&self, _: Uuid, _: &Command) -> Result<Result<Task, Failure>, StoreError> {
        broken()
    }

    async fn flag(&self, _: Uuid, _: &str) -> Result<bool, StoreError> {
        broken()
    }

    async fn reconciliations(
        &self,
        _: Option<ReconciliationStatus>,
        _: Option<Uuid>,
    ) -> Result<Vec<Reconciliation>, StoreError> {
        broken()
    }

    async fn resolve(
        &self,
        _: Uuid,
        _: &str,
        _: Option<&str>,
        _: OffsetDateTime,
    ) -> Result<ResolveOutcome, StoreError> {
        broken()
    }

    async fn ping(&self, _: Duration) -> bool {
        false
    }
}

/// The lines order-service's stand-in answers with.
pub fn lines() -> Value {
    json!([
        {"id": "a1", "sku": "SKU-FUL-001", "productName": "Fulfilment Mug",
         "unitPriceCents": 1250, "quantity": 2},
        {"id": "a2", "sku": "SKU-FUL-002", "productName": "Fulfilment Cup",
         "unitPriceCents": 500, "quantity": 1},
    ])
}

/// The address order-service's stand-in answers with.
pub fn address() -> Address {
    Address {
        line1: "1 High Street".to_owned(),
        line2: Some("Flat 2".to_owned()),
        city: "London".to_owned(),
        postal_code: "SW1A 1AA".to_owned(),
        country: "GB".to_owned(),
    }
}

/// The lines as a task snapshots them.
pub fn items() -> Vec<FulfilmentItem> {
    vec![
        FulfilmentItem {
            sku: "SKU-FUL-001".to_owned(),
            product_name: "Fulfilment Mug".to_owned(),
            quantity: 2,
            unit_price_cents: 1250,
        },
        FulfilmentItem {
            sku: "SKU-FUL-002".to_owned(),
            product_name: "Fulfilment Cup".to_owned(),
            quantity: 1,
            unit_price_cents: 500,
        },
    ]
}

/// How the stand-in answers a path: status and body.
pub type Answer = Arc<dyn Fn(&str) -> (u16, String) + Send + Sync>;

/// Every order found with [`lines`] and [`address`].
pub fn found() -> Answer {
    Arc::new(|path| {
        let id = path.trim_start_matches("/orders/");
        let body = json!({"id": id, "items": lines(), "shippingAddress": address_json()});
        (200, body.to_string())
    })
}

fn address_json() -> Value {
    json!({"line1": "1 High Street", "line2": "Flat 2", "city": "London",
           "postalCode": "SW1A 1AA", "country": "GB"})
}

/// A local HTTP server standing in for order-service: answers as told,
/// recording every path asked for.
#[derive(Clone)]
pub struct Callee {
    /// Its base URL.
    pub url: String,
    seen: Arc<Mutex<Vec<String>>>,
    answer: Arc<Mutex<Answer>>,
}

type Shared = (Arc<Mutex<Vec<String>>>, Arc<Mutex<Answer>>);

async fn answer(State((seen, answer)): State<Shared>, uri: Uri) -> impl IntoResponse {
    seen.lock().unwrap().push(uri.path().to_owned());
    let answer = answer.lock().unwrap().clone();
    let (status, body) = answer(uri.path());
    (
        StatusCode::from_u16(status).unwrap(),
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
}

impl Callee {
    /// Started on a fresh port, answering [`found`].
    pub async fn start() -> Self {
        let seen: Arc<Mutex<Vec<String>>> = Arc::default();
        let answer = Arc::new(Mutex::new(found()));
        let app = axum::Router::new()
            .fallback(self::answer)
            .with_state((seen.clone(), answer.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, seen, answer }
    }

    /// Answers with `answer` from now on.
    pub fn answering(&self, answer: Answer) {
        *self.answer.lock().unwrap() = answer;
    }

    /// Every path asked for so far.
    pub fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

/// The order client, calling `callee`.
pub fn orders(callee: &Callee) -> Orders {
    let tracing = Tracing::with_exporter(
        "fulfilment-service",
        1.0,
        opentelemetry_sdk::trace::InMemorySpanExporter::default(),
    );
    let metrics = Metrics::new("fulfilment-service");
    Orders::new(
        &callee.url,
        clients::resilient(tracing.tracer(), &metrics).unwrap(),
    )
}

/// The handlers' state over `store`.
pub fn state<S: Tasks>(store: S) -> (AppState<S>, LogBuffer) {
    let (logger, buffer) = captured_logger();
    let state = AppState {
        store,
        auth: OperatorAuth::new("dev-operator-token", "dev-operator"),
        logger,
    };
    (state, buffer)
}

/// What the app answered: status, content type, body.
pub struct Answered {
    /// The status.
    pub status: u16,
    /// Its Content-Type.
    pub content_type: Option<String>,
    /// The body, as text.
    pub text: String,
}

impl Answered {
    /// The body as JSON.
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.text).unwrap_or_else(|_| panic!("not JSON: {}", self.text))
    }
}

/// Sends one request to the app over `state`, simulate-next on.
pub async fn call<S: Tasks>(
    state: &AppState<S>,
    method: &str,
    path: &str,
    body: Option<&str>,
    headers: &[(&str, &str)],
) -> Answered {
    let mut request = Request::builder().method(method).uri(path);
    if body.is_some() {
        request = request.header(header::CONTENT_TYPE, "application/json");
    }
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let request = request
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_owned())))
        .unwrap();
    let response = server::app(state.clone(), true)
        .oneshot(request)
        .await
        .unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    Answered {
        status,
        content_type,
        text: String::from_utf8(bytes.to_vec()).unwrap(),
    }
}

/// A fresh PENDING task in `store`; its order id.
pub async fn task<S: Tasks>(store: &S) -> Uuid {
    let order = Uuid::new_v4();
    store.create(order, &items(), &address()).await.unwrap();
    order
}

/// The order's outbox rows' envelopes, in write order.
pub async fn outbox(pool: &PgPool, order_id: Uuid) -> Vec<Value> {
    let rows: Vec<(sqlx::types::Json<Value>,)> = sqlx::query_as(
        "select payload from outbox_events where aggregate_id = $1 order by created_at, id",
    )
    .bind(order_id.to_string())
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter().map(|(json,)| json.0).collect()
}

/// Where a [`captured_logger`] writes.
pub type LogBuffer = Arc<Mutex<Vec<u8>>>;

/// A logger named as the service's, writing to a buffer the test reads back.
pub fn captured_logger() -> (Logger, LogBuffer) {
    let buffer = LogBuffer::default();
    (
        Logger::to_writer("fulfilment-service", buffer.clone()),
        buffer,
    )
}

/// The lines logged to `buffer` so far, each as JSON.
pub fn log_lines(buffer: &LogBuffer) -> Vec<Value> {
    let text = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    text.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The messages logged to `buffer` so far.
pub fn messages(buffer: &LogBuffer) -> Vec<String> {
    log_lines(buffer)
        .iter()
        .map(|line| line["message"].as_str().unwrap().to_owned())
        .collect()
}
