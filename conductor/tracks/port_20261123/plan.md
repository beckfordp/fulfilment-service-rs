# Plan — port fulfilment-service (gluon all-Rust Stage A, GREEN)

Spec: [spec.md](./spec.md).

## Phase 1: Port
- [ ] Task 1.1: schema (the V1–V5 end state) and domain (state, lifecycle, commands, simulate-next, reconciliation, operator auth) — tests first
- [ ] Task 1.2: Postgres store (the Scala SQL; transitions and their outbox row in one transaction; flags first-wins) — tests first (throwaway Postgres)
- [ ] Task 1.3: order-service client and routes (list, steps, simulate-next, reconciliations); docs — tests first
- [ ] Task 1.4: consumers (payment-events, order-events, DLQ) — tests first
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md): gluon `bin/verify-port fulfilment-service` all MET; CI ≥90% lines
