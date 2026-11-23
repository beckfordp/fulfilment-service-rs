// SPDX-License-Identifier: MIT

//! The pure core: the states, the lifecycle's one rule, the commands' checks,
//! simulate-next's step, and the operator token.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use fulfilment_service::domain::{
    Address, Command, Failure, FulfilmentState, OperatorAuth, ReconciliationStatus, Task,
    parse_uuid,
};
use time::macros::datetime;
use uuid::Uuid;

use FulfilmentState::{Cancelled, DispatchFailed, Dispatched, Packed, Pending, Picking};

const ALL: [FulfilmentState; 6] = [
    Pending,
    Picking,
    Packed,
    Dispatched,
    DispatchFailed,
    Cancelled,
];

fn dispatch() -> Command {
    Command::dispatch("GLN-1", "DHL").unwrap()
}

#[test]
fn a_state_parses_only_as_written() {
    for state in ALL {
        assert_eq!(FulfilmentState::parse(state.as_str()), Some(state));
    }
    assert_eq!(DispatchFailed.as_str(), "DISPATCH_FAILED");
    for raw in ["pending", "", "SHIPPED", " PENDING"] {
        assert_eq!(FulfilmentState::parse(raw), None, "{raw:?}");
    }
}

#[test]
fn uuids_are_parsed_in_the_canonical_form_only() {
    let id = "3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90";
    assert_eq!(parse_uuid(id), Uuid::parse_str(id).ok());
    for raw in [
        "nope",
        "3f2b8c1e7a4d4e9b9c2a1d5e6f7a8b90",
        "{3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b9}",
    ] {
        assert_eq!(parse_uuid(raw), None, "{raw}");
    }
}

#[test]
fn the_lifecycle_allows_each_step_from_its_state_only() {
    let fail = Command::fail("x").unwrap();
    let cases = [
        (Command::StartPicking, vec![(Pending, Picking)]),
        (Command::Pack, vec![(Picking, Packed)]),
        (dispatch(), vec![(Packed, Dispatched)]),
        (
            fail,
            vec![
                (Pending, DispatchFailed),
                (Picking, DispatchFailed),
                (Packed, DispatchFailed),
            ],
        ),
        (
            Command::Cancel,
            vec![
                (Pending, Cancelled),
                (Picking, Cancelled),
                (Packed, Cancelled),
            ],
        ),
    ];
    for (command, allowed) in cases {
        for from in ALL {
            let want = allowed
                .iter()
                .find(|(state, _)| *state == from)
                .map_or(Err(from), |(_, to)| Ok(*to));
            assert_eq!(command.transition(from), want, "{command:?} from {from:?}");
        }
    }
}

#[test]
fn dispatch_and_fail_are_trimmed_and_refuse_blanks() {
    assert_eq!(
        Command::dispatch(" GLN-1 ", " DHL "),
        Ok(Command::Dispatch {
            tracking_no: "GLN-1".to_owned(),
            carrier: "DHL".to_owned()
        })
    );
    for (tracking, carrier) in [(" ", "DHL"), ("GLN-1", "")] {
        assert_eq!(
            Command::dispatch(tracking, carrier),
            Err("trackingNo and carrier are required".to_owned())
        );
    }
    assert_eq!(
        Command::fail(" late "),
        Ok(Command::Fail("late".to_owned()))
    );
    assert_eq!(Command::fail("  "), Err("reason is required".to_owned()));
}

#[test]
fn simulate_next_steps_forward_until_terminal() {
    assert_eq!(
        Command::simulated_next(Pending, dispatch()),
        Some(Command::StartPicking)
    );
    assert_eq!(
        Command::simulated_next(Picking, dispatch()),
        Some(Command::Pack)
    );
    assert_eq!(
        Command::simulated_next(Packed, dispatch()),
        Some(dispatch())
    );
    for terminal in [Dispatched, DispatchFailed, Cancelled] {
        assert_eq!(Command::simulated_next(terminal, dispatch()), None);
    }
}

fn task(state: FulfilmentState) -> Task {
    let at = datetime!(2026-11-23 10:00 UTC);
    Task {
        order_id: Uuid::nil(),
        state,
        items: Vec::new(),
        shipping_address: Address {
            line1: "1".to_owned(),
            line2: None,
            city: "c".to_owned(),
            postal_code: "p".to_owned(),
            country: "GB".to_owned(),
        },
        tracking_no: None,
        carrier: None,
        failure_reason: None,
        created_at: at,
        updated_at: at,
    }
}

#[test]
fn applying_a_command_records_its_outcome_or_names_the_state() {
    let later = datetime!(2026-11-23 11:00 UTC);
    let dispatched = task(Packed).apply(&dispatch(), later).unwrap();
    assert_eq!(
        (
            dispatched.state,
            dispatched.tracking_no.as_deref(),
            dispatched.carrier.as_deref(),
            dispatched.updated_at
        ),
        (Dispatched, Some("GLN-1"), Some("DHL"), later)
    );
    let failed = task(Picking)
        .apply(&Command::fail("late").unwrap(), later)
        .unwrap();
    assert_eq!(
        (failed.state, failed.failure_reason.as_deref()),
        (DispatchFailed, Some("late"))
    );
    assert_eq!(
        task(Pending).apply(&Command::Pack, later),
        Err(Failure::InvalidTransition {
            message: format!("Cannot transition order {} from PENDING", Uuid::nil()),
            from: Some(Pending),
        })
    );
}

#[test]
fn reconciliation_statuses_parse_as_written() {
    assert_eq!(
        ReconciliationStatus::parse("open"),
        Some(ReconciliationStatus::Open)
    );
    assert_eq!(ReconciliationStatus::Resolved.as_str(), "resolved");
    assert_eq!(ReconciliationStatus::parse("Open"), None);
}

#[test]
fn the_operator_token_is_a_bearer_in_any_case_trimmed() {
    let auth = OperatorAuth::new(" t0k ", "op");
    for header in ["Bearer t0k", "bearer  t0k ", "BEARER t0k"] {
        assert_eq!(auth.require_operator(Some(header)), Ok("op"), "{header}");
    }
    for header in [
        None,
        Some("Bearer"),
        Some("Bearer x"),
        Some("Basic t0k"),
        Some("é"),
    ] {
        assert!(auth.require_operator(header).is_err(), "{header:?}");
    }
    assert!(
        OperatorAuth::new(" ", "op")
            .require_operator(Some("Bearer  "))
            .is_err()
    );
}
