// SPDX-License-Identifier: MIT

//! The fulfilment domain, as the Scala `FulfilmentState`, `FulfilmentCommand`,
//! `FulfilmentLifecycle`, `FulfilmentTask`, `Reconciliation` and `OperatorAuth`:
//! values parsed at the edge, and the lifecycle's one rule for every state
//! change (operator steps, simulate-next, cancellation).

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// Java's `UUID.fromString` for the canonical form: 8-4-4-4-12 hex digits.
/// (Rust's parser also takes braced, URN and unhyphenated forms; Java doesn't.)
#[must_use]
pub fn parse_uuid(raw: &str) -> Option<Uuid> {
    let canonical = raw.len() == 36
        && raw
            .char_indices()
            .all(|(i, c)| matches!(i, 8 | 13 | 18 | 23) == (c == '-'));
    canonical.then(|| Uuid::parse_str(raw).ok()).flatten()
}

/// A task's lifecycle state; `as_str` is the stored and wire value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FulfilmentState {
    /// Created from a `PaymentAuthorized`, waiting to be picked.
    Pending,
    /// Being picked.
    Picking,
    /// Packed, waiting for the carrier.
    Packed,
    /// With the carrier (terminal).
    Dispatched,
    /// Couldn't be dispatched (terminal).
    DispatchFailed,
    /// The order was cancelled first (terminal).
    Cancelled,
}

impl FulfilmentState {
    /// The wire and column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Picking => "PICKING",
            Self::Packed => "PACKED",
            Self::Dispatched => "DISPATCHED",
            Self::DispatchFailed => "DISPATCH_FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    /// The state `raw` names exactly, if it names one.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        [
            Self::Pending,
            Self::Picking,
            Self::Packed,
            Self::Dispatched,
            Self::DispatchFailed,
            Self::Cancelled,
        ]
        .into_iter()
        .find(|state| state.as_str() == raw)
    }
}

/// One order line as snapshotted when the task was created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(inline)]
pub struct FulfilmentItem {
    /// Product SKU.
    #[schemars(example = &"SKU-0042")]
    pub sku: String,
    /// Product name at dispatch time.
    #[schemars(example = &"Espresso cup")]
    pub product_name: String,
    /// Units dispatched.
    #[schemars(example = 2)]
    pub quantity: i32,
    /// Unit price, in cents.
    #[schemars(example = 1250)]
    pub unit_price_cents: i32,
}

/// Ship-to address snapshotted from the order when the task was created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Address {
    /// First address line.
    #[schemars(example = &"1 High Street")]
    pub line1: String,
    /// Second address line, if any.
    #[serde(default)]
    #[schemars(example = &"Flat 2")]
    pub line2: Option<String>,
    /// City or town.
    #[schemars(example = &"London")]
    pub city: String,
    /// Postcode / ZIP.
    #[schemars(example = &"SW1A 1AA")]
    pub postal_code: String,
    /// ISO 3166-1 alpha-2 country code.
    #[schemars(example = &"GB")]
    pub country: String,
}

/// An operator (or simulated) step on a task. `Dispatch` and `Fail` are only
/// built by [`Command::dispatch`] and [`Command::fail`], so their fields are
/// trimmed and non-blank.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// PENDING → PICKING.
    StartPicking,
    /// PICKING → PACKED.
    Pack,
    /// PENDING, PICKING or PACKED → CANCELLED (the order was cancelled).
    Cancel,
    /// PACKED → DISPATCHED.
    Dispatch {
        /// The carrier's tracking number.
        tracking_no: String,
        /// The carrier.
        carrier: String,
    },
    /// PENDING, PICKING or PACKED → `DISPATCH_FAILED`.
    Fail(String),
}

impl Command {
    /// A dispatch, its fields trimmed.
    ///
    /// # Errors
    /// "trackingNo and carrier are required" when either is blank.
    pub fn dispatch(tracking_no: &str, carrier: &str) -> Result<Self, String> {
        let (tracking_no, carrier) = (tracking_no.trim(), carrier.trim());
        if tracking_no.is_empty() || carrier.is_empty() {
            return Err("trackingNo and carrier are required".to_owned());
        }
        Ok(Self::Dispatch {
            tracking_no: tracking_no.to_owned(),
            carrier: carrier.to_owned(),
        })
    }

    /// A failure, its reason trimmed.
    ///
    /// # Errors
    /// "reason is required" when it is blank.
    pub fn fail(reason: &str) -> Result<Self, String> {
        let reason = reason.trim();
        if reason.is_empty() {
            return Err("reason is required".to_owned());
        }
        Ok(Self::Fail(reason.to_owned()))
    }

    /// The state this command moves `current` to.
    ///
    /// # Errors
    /// `current` itself when the command isn't legal from it.
    pub fn transition(&self, current: FulfilmentState) -> Result<FulfilmentState, FulfilmentState> {
        use FulfilmentState::{Cancelled, DispatchFailed, Dispatched, Packed, Pending, Picking};
        match (current, self) {
            (Pending, Self::StartPicking) => Ok(Picking),
            (Picking, Self::Pack) => Ok(Packed),
            (Packed, Self::Dispatch { .. }) => Ok(Dispatched),
            (Pending | Picking | Packed, Self::Fail(_)) => Ok(DispatchFailed),
            (Pending | Picking | Packed, Self::Cancel) => Ok(Cancelled),
            _ => Err(current),
        }
    }

    /// simulate-next's one step forward from `current`, dispatching with
    /// `dispatch`; `None` once terminal.
    #[must_use]
    pub fn simulated_next(current: FulfilmentState, dispatch: Self) -> Option<Self> {
        match current {
            FulfilmentState::Pending => Some(Self::StartPicking),
            FulfilmentState::Picking => Some(Self::Pack),
            FulfilmentState::Packed => Some(dispatch),
            FulfilmentState::Dispatched
            | FulfilmentState::DispatchFailed
            | FulfilmentState::Cancelled => None,
        }
    }
}

/// The operator API's expected failures, mapped to statuses at the edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// No task for the order (404).
    NotFound,
    /// The task can't move (409), from the state it's in when that's known.
    InvalidTransition {
        /// What the 409 says.
        message: String,
        /// The task's state.
        from: Option<FulfilmentState>,
    },
    /// A blank field (400).
    InvalidRequest(String),
}

/// A pick/pack task: the order's snapshot, its state and the dispatch outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    /// The order it was created for.
    pub order_id: Uuid,
    /// Where it is in the lifecycle.
    pub state: FulfilmentState,
    /// The order's lines.
    pub items: Vec<FulfilmentItem>,
    /// Where it ships.
    pub shipping_address: Address,
    /// Set once DISPATCHED.
    pub tracking_no: Option<String>,
    /// Set once DISPATCHED.
    pub carrier: Option<String>,
    /// Set once `DISPATCH_FAILED`.
    pub failure_reason: Option<String>,
    /// When created.
    pub created_at: OffsetDateTime,
    /// When last changed.
    pub updated_at: OffsetDateTime,
}

impl Task {
    /// The task `command` makes of this one (Scala `FulfilmentTask.applyCommand`).
    ///
    /// # Errors
    /// [`Failure::InvalidTransition`] "Cannot transition order <id> from <STATE>".
    pub fn apply(&self, command: &Command, now: OffsetDateTime) -> Result<Self, Failure> {
        let state = command
            .transition(self.state)
            .map_err(|from| Failure::InvalidTransition {
                message: format!(
                    "Cannot transition order {} from {}",
                    self.order_id,
                    from.as_str()
                ),
                from: Some(from),
            })?;
        let mut next = Self {
            state,
            updated_at: now,
            ..self.clone()
        };
        match command {
            Command::Dispatch {
                tracking_no,
                carrier,
            } => {
                next.tracking_no = Some(tracking_no.clone());
                next.carrier = Some(carrier.clone());
            }
            Command::Fail(reason) => next.failure_reason = Some(reason.clone()),
            Command::StartPicking | Command::Pack | Command::Cancel => {}
        }
        Ok(next)
    }
}

/// A reconciliation flag's state (ADM-4, ADR 0026).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationStatus {
    /// Waiting for an operator.
    Open,
    /// An operator resolved it.
    Resolved,
}

impl ReconciliationStatus {
    /// The wire and column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
        }
    }

    /// The status `raw` names, if it names one.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "open" => Some(Self::Open),
            "resolved" => Some(Self::Resolved),
            _ => None,
        }
    }
}

/// A first-wins manual-reconciliation flag, and its resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconciliation {
    /// The flagged order.
    pub order_id: Uuid,
    /// Why it needs reconciling.
    pub reason: String,
    /// Open or resolved.
    pub status: ReconciliationStatus,
    /// When flagged.
    pub created_at: OffsetDateTime,
    /// Who resolved it.
    pub operator_id: Option<String>,
    /// The operator's note.
    pub note: Option<String>,
    /// When resolved.
    pub resolved_at: Option<OffsetDateTime>,
}

/// What resolving a flag found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// Resolved now.
    Resolved(Reconciliation),
    /// Already resolved: the first resolution, untouched.
    AlreadyResolved(Reconciliation),
    /// No flag for the order.
    NotFound,
}

/// The dev/demo operator gate (US-22, ADR 0024): not a real authorization boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorAuth {
    token: Option<String>,
    operator_id: String,
}

/// A presented operator token that is malformed or doesn't match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unauthorized;

impl OperatorAuth {
    /// From config: a blank token is none (every presented token refused).
    #[must_use]
    pub fn new(token: &str, operator_id: &str) -> Self {
        let token = token.trim();
        Self {
            token: (!token.is_empty()).then(|| token.to_owned()),
            operator_id: operator_id.trim().to_owned(),
        }
    }

    /// The operator, when `authorization` is `Bearer <token>` (the scheme in
    /// any case, the token trimmed; compared in constant time).
    ///
    /// # Errors
    /// [`Unauthorized`] otherwise, a missing header too.
    pub fn require_operator(&self, authorization: Option<&str>) -> Result<&str, Unauthorized> {
        const SCHEME: &str = "bearer ";
        let expected = self.token.as_deref().ok_or(Unauthorized)?;
        let raw = authorization.ok_or(Unauthorized)?;
        if raw.len() <= SCHEME.len() || !raw.is_char_boundary(SCHEME.len()) {
            return Err(Unauthorized);
        }
        let (scheme, presented) = raw.split_at(SCHEME.len());
        if scheme.eq_ignore_ascii_case(SCHEME) && constant_eq(presented.trim(), expected) {
            Ok(&self.operator_id)
        } else {
            Err(Unauthorized)
        }
    }
}

/// `MessageDigest.isEqual`: the whole of both compared, whatever differs first.
fn constant_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0_u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
