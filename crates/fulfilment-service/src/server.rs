// SPDX-License-Identifier: MIT

//! The HTTP server: routes, binding and graceful shutdown. The health routes,
//! tapir's plain-text errors and the HTTP edge (CORS, the Allow-less 405) are
//! gluon-rs `service-basics`.

use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use axum::Router;
use axum::routing::{get, patch, post};
use service_basics::cors::{CorsPolicy, Unrouted};
use service_basics::edge::edge;
use service_basics::health::{self, Readiness};
use service_basics::text_error::{self, TextError, method_not_allowed};
use telemetry::logging::Logger;
use tokio::net::TcpListener;

use crate::domain::OperatorAuth;
use crate::store::Tasks;
use crate::{docs, routes};

/// How long readiness waits for Postgres to answer.
const READY_PING: Duration = Duration::from_secs(2);

/// What the handlers share.
#[derive(Clone)]
pub struct AppState<S> {
    /// The service's tables.
    pub store: S,
    /// The dev operator token.
    pub auth: OperatorAuth,
    /// The service's JSON log.
    pub logger: Logger,
}

/// Ready when Postgres answers a ping within 2 s.
impl<S: Tasks> Readiness for AppState<S> {
    async fn ready(&self) -> bool {
        self.store.ping(READY_PING).await
    }
}

/// The request headers a preflight may ask for (`Cors.scala`: Authorization
/// for the operator token).
const ALLOWED_HEADERS: &[&str] = &["Content-Type", "Authorization"];

/// The service: its routes behind the Scala services' edge, CORS outermost.
pub fn app<S: Tasks>(state: AppState<S>, simulation: bool) -> Router {
    edge(
        routes(state, simulation),
        CorsPolicy::exposing("X-Total-Count").allowing(ALLOWED_HEADERS),
    )
}

/// The tasks, their steps (simulate-next only when `simulation`),
/// reconciliations, health and the docs; tapir's 405 for a known path with
/// another method and 404 for any other.
pub fn routes<S: Tasks>(state: AppState<S>, simulation: bool) -> Router {
    let mut router = health::routes()
        .route("/fulfilment-tasks", get(routes::list::<S>))
        .route(
            "/fulfilment-tasks/{order_id}/start-picking",
            post(routes::start_picking::<S>),
        )
        .route("/fulfilment-tasks/{order_id}/pack", post(routes::pack::<S>))
        .route(
            "/fulfilment-tasks/{order_id}/dispatch",
            post(routes::dispatch::<S>),
        )
        .route("/fulfilment-tasks/{order_id}/fail", post(routes::fail::<S>))
        .route("/reconciliations", get(routes::reconciliations::<S>))
        .route("/reconciliations/{order_id}", patch(routes::resolve::<S>));
    if simulation {
        router = router.route(
            "/fulfilment-tasks/{order_id}/simulate-next",
            post(routes::simulate_next::<S>),
        );
    }
    router
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .with_state(state)
        .merge(docs::router(simulation))
}

/// 404 for any unknown path, marked [`Unrouted`] for CORS.
async fn not_found() -> (Unrouted, TextError) {
    (Unrouted, text_error::not_found().await)
}

/// Serves the API on `0.0.0.0:<port>` until `shutdown` completes.
///
/// # Errors
/// When the port can't be bound or serving fails.
pub async fn run<S: Tasks>(
    port: u16,
    state: AppState<S>,
    simulation: bool,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port))).await?;
    axum::serve(listener, app(state, simulation))
        .with_graceful_shutdown(shutdown)
        .await
}
