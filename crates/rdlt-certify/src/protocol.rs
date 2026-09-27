//! The protocol's clauses (`P`), checked by a client that speaks the protocol raw, each over
//! connections of its own.

mod credit;
mod handshake;
mod heartbeat;
mod malformed;

use std::future::Future;
use std::time::Duration;

use rdlt_connector::testing::{Clause, ClauseResult, Outcome};
use rdlt_connector::wire::v1;
use rdlt_connector::{ConnectorError, Role};
use rdlt_host::remote::Client;
use rdlt_wire::tonic::Status;
use rdlt_wire::{PROTOCOL_MAJOR, PROTOCOL_MINOR};

use crate::target::Target;

/// The clauses [`certify_source`](crate::certify_source) and
/// [`certify_destination`](crate::certify_destination) check first, in order.
pub const PROTOCOL_CLAUSES: &[Clause] = &[
    Clause {
        id: "P-HANDSHAKE",
        statement: "the handshake answers the protocol's major version with the connector's spec \
                    and limits, and refuses another major version as unsupported",
    },
    Clause {
        id: "P-ORDER",
        statement: "a call before the handshake, and a second handshake, are refused with typed \
                    errors",
    },
    Clause {
        id: "P-ROLE",
        statement: "a role the connector does not serve is refused as unsupported",
    },
    Clause {
        id: "P-LIMITS",
        statement: "a configuration beyond the connector's limit is refused with `limit_exceeded`",
    },
    Clause {
        id: "P-HEARTBEAT",
        statement: "each heartbeat is answered with its sequence number, in order",
    },
    Clause {
        id: "P-MALFORMED",
        statement: "a call the connector cannot read is refused with a typed error, and the \
                    connection serves on",
    },
    Clause {
        id: "P-CREDIT",
        statement: "a read sends nothing more once its credit is spent, until more is granted",
    },
];

/// How long any one protocol clause may take.
const CLAUSE_TIME: Duration = Duration::from_secs(30);

/// Why a connector breaks a clause.
#[derive(Debug)]
pub(crate) struct Violation(pub(crate) String);

impl Violation {
    /// The violation `error` describes.
    pub(crate) fn of(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }
}

impl From<String> for Violation {
    fn from(reason: String) -> Self {
        Self(reason)
    }
}

impl From<&str> for Violation {
    fn from(reason: &str) -> Self {
        Self(reason.to_owned())
    }
}

/// What a clause found: nothing wrong, a violation, or a reason it does not apply.
pub(crate) enum Found {
    Kept,
    Broken(Violation),
    Inapplicable(String),
}

impl From<Result<(), Violation>> for Found {
    fn from(result: Result<(), Violation>) -> Self {
        match result {
            Ok(()) => Self::Kept,
            Err(violation) => Self::Broken(violation),
        }
    }
}

impl From<Result<Option<String>, Violation>> for Found {
    fn from(result: Result<Option<String>, Violation>) -> Self {
        match result {
            Ok(None) => Self::Kept,
            Ok(Some(reason)) => Self::Inapplicable(reason),
            Err(violation) => Self::Broken(violation),
        }
    }
}

/// Checks every protocol clause against `target` as `role`, with `config`.
pub(crate) async fn check(
    target: &Target,
    role: Role,
    config: &serde_json::Value,
) -> Vec<ClauseResult> {
    let config = config.to_string();
    let mut results = Vec::new();
    for clause in PROTOCOL_CLAUSES {
        let checking = async {
            match clause.id {
                "P-HANDSHAKE" => handshake::answered(target, role, &config).await,
                "P-ORDER" => handshake::ordered(target, role, &config).await,
                "P-ROLE" => handshake::roles(target, role, &config).await,
                "P-LIMITS" => handshake::limited(target, role, &config).await,
                "P-HEARTBEAT" => heartbeat::echoed(target, role, &config).await,
                "P-MALFORMED" => malformed::refused(target, role, &config).await,
                _ => credit::respected(target, role, &config).await,
            }
        };
        let outcome = match within(checking).await {
            Found::Kept => Outcome::Passed,
            Found::Broken(Violation(reason)) => Outcome::Failed(reason),
            Found::Inapplicable(reason) => Outcome::Skipped(reason),
        };
        results.push(ClauseResult {
            clause: *clause,
            outcome,
        });
    }
    results
}

/// `checking`'s finding, or a violation once it takes longer than [`CLAUSE_TIME`].
async fn within(checking: impl Future<Output = Found>) -> Found {
    tokio::time::timeout(CLAUSE_TIME, checking)
        .await
        .unwrap_or_else(|_| Found::Broken(Violation(format!("took longer than {CLAUSE_TIME:?}"))))
}

/// A handshake as `role` with `config`, at `major`.
pub(crate) fn request(role: Role, config: &str, major: u32) -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: major,
        protocol_minor: PROTOCOL_MINOR,
        features: Vec::new(),
        role: wire_role(role) as i32,
        config_json: config.to_owned(),
        traceparent: String::new(),
        limits: Some(rdlt_wire::Limits::default().into()),
    }
}

fn wire_role(role: Role) -> v1::Role {
    match role {
        Role::Source => v1::Role::Source,
        Role::Destination => v1::Role::Destination,
    }
}

/// A client of `target`, handshaken as `role` with `config`, and the connector's answer.
async fn handshaken(
    target: &Target,
    role: Role,
    config: &str,
) -> Result<(Client, v1::HandshakeResponse), Violation> {
    let mut client = target.client().await.map_err(Violation::of)?;
    let answer = client
        .handshake(request(role, config, PROTOCOL_MAJOR))
        .await
        .map_err(|status| format!("the handshake failed: {}", error(&status)))?;
    Ok((client, answer.into_inner()))
}

/// The error a status carries.
fn error(status: &Status) -> ConnectorError {
    rdlt_connector::wire::error(status)
}

/// The refusal `result` is, when it is one with `code`; `what` names the call.
fn refused_with<T>(
    result: Result<T, Status>,
    code: &str,
    what: &str,
) -> Result<ConnectorError, Violation> {
    match result {
        Ok(_) => Err(Violation(format!(
            "{what} was answered, not refused with `{code}`"
        ))),
        Err(status) => {
            let error = error(&status);
            if error.code() == Some(code) {
                Ok(error)
            } else {
                Err(Violation(format!(
                    "{what} was refused with `{error}`, not with `{code}`"
                )))
            }
        }
    }
}

/// Whether `error` is refused as unsupported; `what` names the call.
fn unsupported(error: &ConnectorError, what: &str) -> Result<(), Violation> {
    if error.kind() == rdlt_connector::ConnectorErrorKind::Unsupported {
        Ok(())
    } else {
        Err(Violation(format!(
            "{what} was refused as {:?}, not as unsupported",
            error.kind()
        )))
    }
}
