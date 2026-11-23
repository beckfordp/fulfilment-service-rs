# Spec — port fulfilment-service (gluon all-Rust Stage A, GREEN)

## Overview
The Scala fulfilment-service in idiomatic Rust on the template's shape: its tables (the
Flyway V1–V5 end state); the lifecycle (PENDING → PICKING → PACKED → DISPATCHED, failure
and cancellation); the `payment-events` consumer (PaymentAuthorized → order-service's
snapshot through purerest's resilience → a PENDING task, idempotent; undecodable records
skipped, a lookup failure dead-lettered) and the `order-events` consumer (OrderCancelled
aborts an undispatched task, flags a dispatched one; undecodable records dead-lettered);
the task list and the operator steps, dispatch and fail writing `DispatchConfirmed` /
`DispatchFailed` to the outbox in the transaction; simulate-next (config-gated);
reconciliations and operator auth; readiness on Postgres; CORS; the docs (OpenAPI and
AsyncAPI).

## Functional requirements
FUL-F1–F15, FUL-D1–D2 (tests/contract `docs/behaviours.md`).

## Acceptance criteria
`scripts/ci.sh` green, ≥90% lines; `bin/verify-port fulfilment-service` all MET.

## Out of scope
Wire rows: metrics (db durations, pool, journey, flag counter, event age), log messages,
tracing spans and the outbox's traceparent.
