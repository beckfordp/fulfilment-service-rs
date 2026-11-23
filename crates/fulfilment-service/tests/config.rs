// SPDX-License-Identifier: MIT

//! The config contract: the Scala service's environment variables and defaults.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use fulfilment_service::config::{Config, PostgresConfig, TracingConfig, from_env};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn defaults() {
    assert_eq!(
        from_env(&HashMap::new()).unwrap(),
        Config {
            port: 8080,
            service_name: "fulfilment-service".to_owned(),
            postgres: PostgresConfig {
                host: "localhost".to_owned(),
                port: 5432,
                database: "fulfilment".to_owned(),
                user: "fulfilment".to_owned(),
                password: "fulfilment".to_owned(),
            },
            kafka_bootstrap: "localhost:9092".to_owned(),
            order_url: "http://localhost:8083".to_owned(),
            simulation_enabled: false,
            operator_token: "dev-operator-token".to_owned(),
            operator_id: "dev-operator".to_owned(),
            tracing: TracingConfig {
                otlp_endpoint: None,
                sampling_ratio: 1.0,
            },
        }
    );
}

#[test]
fn every_variable_overrides_its_default() {
    let config = from_env(&env(&[
        ("FULFILMENT_SERVICE_PORT", "18080"),
        ("FULFILMENT_SERVICE_NAME", "fulfilment-service-rs"),
        ("POSTGRES_HOST", "127.0.0.1"),
        ("POSTGRES_PORT", "15432"),
        ("POSTGRES_DB", "db"),
        ("POSTGRES_USER", "u"),
        ("POSTGRES_PASSWORD", "p"),
        ("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"),
        ("ORDER_SERVICE_URL", "http://127.0.0.1:18083"),
        ("FULFILMENT_SIMULATION_ENABLED", "true"),
        ("OPERATOR_TOKEN", "t"),
        ("OPERATOR_ID", "op"),
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"),
        ("OTEL_TRACES_SAMPLER_ARG", "0.25"),
    ]))
    .unwrap();
    assert_eq!(config.port, 18080);
    assert_eq!(config.service_name, "fulfilment-service-rs");
    assert_eq!(config.postgres.port, 15432);
    assert_eq!(config.kafka_bootstrap, "127.0.0.1:19092");
    assert_eq!(config.tracing.sampling_ratio, 0.25);
    assert_eq!(config.order_url, "http://127.0.0.1:18083");
    assert!(config.simulation_enabled);
    assert_eq!(
        (config.operator_token.as_str(), config.operator_id.as_str()),
        ("t", "op")
    );
}

#[test]
fn the_simulation_switch_reads_hocon_booleans() {
    for (raw, want) in [
        ("on", true),
        ("yes", true),
        (" false ", false),
        ("off", false),
    ] {
        let config = from_env(&env(&[("FULFILMENT_SIMULATION_ENABLED", raw)])).unwrap();
        assert_eq!(config.simulation_enabled, want, "{raw}");
    }
}

#[test]
fn every_problem_is_reported() {
    let error = from_env(&env(&[
        ("FULFILMENT_SERVICE_PORT", "http"),
        ("POSTGRES_PORT", "x"),
        ("FULFILMENT_SIMULATION_ENABLED", "maybe"),
        ("OPERATOR_ID", " "),
    ]))
    .unwrap_err();
    let text = error.to_string();
    assert!(text.contains("FULFILMENT_SERVICE_PORT"), "{text}");
    assert!(text.contains("POSTGRES_PORT"), "{text}");
    assert!(text.contains("FULFILMENT_SIMULATION_ENABLED"), "{text}");
    assert!(text.contains("operator-id"), "{text}");
}
