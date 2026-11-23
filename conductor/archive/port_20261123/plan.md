# Plan — port fulfilment-service (gluon all-Rust Stage A, GREEN)

Spec: [spec.md](./spec.md).

## Phase 1: Port
- [x] Task 1.1: schema (the V1–V5 end state) and domain (state, lifecycle, commands, simulate-next, reconciliation, operator auth) — tests first `9aa980c`
- [x] Task 1.2: Postgres store (the Scala SQL; transitions and their outbox row in one transaction; flags first-wins) — tests first (throwaway Postgres) `9aa980c`
- [x] Task 1.3: order-service client and routes (list, steps, simulate-next, reconciliations); docs — tests first `9aa980c`
- [x] Task 1.4: consumers (payment-events, order-events, DLQ) — tests first `9aa980c`
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md): gluon `bin/verify-port fulfilment-service` all MET (contract 22/22; gluon `docs/ports/evidence/fulfilment-service/green.txt`); CI 97.11% lines

## Archive (2026-11-23)

Reviewed: no High/Medium. Functional 15/15 on the first contract run; FUL-D2 failed once
(DispatchConfirmed's `items` required in the schema, tapir lists it optional) and was
fixed with `#[serde(default)]` plus a test of every payload's required fields. Mirrored
on purpose: simulate-next's blank forced reason is a 500 (tapir has no 400 variant
there); `payment-events` poison is skipped, `order-events` poison dead-lettered; docs
list simulate-next only when it is served. Two schema descriptions gain backticks
(clippy `doc_markdown`); not compared by any check.
