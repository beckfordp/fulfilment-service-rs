// SPDX-License-Identifier: MIT

//! The config contract: the Scala service's environment variables and
//! defaults (its `application.conf`). Pure: an environment map in, a
//! [`Config`] or every problem out; the pieces every service reads the same
//! way are gluon-rs `service-basics` and `telemetry`.

use std::collections::HashMap;
use std::hash::BuildHasher;

pub use service_basics::config::{ConfigError, PostgresConfig};
use service_basics::config::{port, postgres};
pub use telemetry::config::TracingConfig;
use telemetry::config::tracing_from_env;

/// The HTTP port's variable.
pub const PORT_VARIABLE: &str = "FULFILMENT_SERVICE_PORT";

/// The simulate-next switch's variable.
pub const SIMULATION_VARIABLE: &str = "FULFILMENT_SIMULATION_ENABLED";

/// Everything the service reads from its environment.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// `FULFILMENT_SERVICE_PORT`: the HTTP API.
    pub port: u16,
    /// `FULFILMENT_SERVICE_NAME`: `service.name` on spans, the logger name.
    pub service_name: String,
    /// `POSTGRES_*`.
    pub postgres: PostgresConfig,
    /// `KAFKA_BOOTSTRAP_SERVERS`: the consumers' and DLQ producer's brokers.
    pub kafka_bootstrap: String,
    /// `ORDER_SERVICE_URL`: order-service, for the order's snapshot.
    pub order_url: String,
    /// `FULFILMENT_SIMULATION_ENABLED`: serve simulate-next (demo only; off).
    pub simulation_enabled: bool,
    /// `OPERATOR_TOKEN`: the dev operator token (ADR 0024).
    pub operator_token: String,
    /// `OPERATOR_ID`: the operator the token stands for.
    pub operator_id: String,
    /// `OTEL_*`.
    pub tracing: TracingConfig,
}

/// Reads the config from an environment map.
///
/// # Errors
/// [`ConfigError`] listing every invalid variable.
pub fn from_env<S: BuildHasher>(env: &HashMap<String, String, S>) -> Result<Config, ConfigError> {
    let mut problems = Vec::new();
    let text = |name: &str, default: &str| env.get(name).map_or(default, String::as_str).to_owned();
    let http_port = port(env, PORT_VARIABLE, 8080, &mut problems);
    let postgres = postgres(env, "fulfilment", &mut problems);
    let tracing = tracing_from_env(env, &mut problems);
    let simulation_enabled = match env.get(SIMULATION_VARIABLE).map(|raw| boolean(raw.trim())) {
        None => false,
        Some(Some(enabled)) => enabled,
        Some(None) => {
            problems.push(format!("{SIMULATION_VARIABLE} must be true or false"));
            false
        }
    };
    let operator_id = text("OPERATOR_ID", "dev-operator");
    if operator_id.trim().is_empty() {
        problems.push("operator-auth.operator-id must not be blank".to_owned());
    }
    if !problems.is_empty() {
        return Err(ConfigError { problems });
    }
    Ok(Config {
        port: http_port,
        service_name: text("FULFILMENT_SERVICE_NAME", "fulfilment-service"),
        postgres,
        kafka_bootstrap: text("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        order_url: text("ORDER_SERVICE_URL", "http://localhost:8083"),
        simulation_enabled,
        operator_token: text("OPERATOR_TOKEN", "dev-operator-token"),
        operator_id,
        tracing,
    })
}

/// A HOCON boolean: true, yes, on or false, no, off.
fn boolean(raw: &str) -> Option<bool> {
    match raw {
        "true" | "yes" | "on" => Some(true),
        "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
