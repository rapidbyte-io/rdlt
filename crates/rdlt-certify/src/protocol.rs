//! The protocol's clauses (`P`), checked by a client that speaks the protocol raw, each over
//! connections of its own.

mod credit;
mod handshake;
mod heartbeat;
mod limits;
mod malformed;
mod writing;

use std::future::Future;
use std::time::Duration;

use rdlt_connector::testing::{Clause, ClauseResult, Observed, Outcome};
use rdlt_connector::wire::v1;
use rdlt_connector::{ConnectorError, Role};
use rdlt_host::remote::Client;
use rdlt_wire::tonic::{Response, Status, Streaming};
use rdlt_wire::{PROTOCOL_MAJOR, PROTOCOL_MINOR};

use crate::target::Target;

/// The clauses [`certify_source`](crate::certify_source) and
/// [`certify_destination`](crate::certify_destination) check first, in order.
pub const PROTOCOL_CLAUSES: &[Clause] = &[
    Clause {
        id: "P-HANDSHAKE",
        statement: "the handshake answers the protocol's major version with the connector's spec \
                    and limits before any configuration, ignores features it does not know, and \
                    refuses another major version as unsupported; the configuration answers the \
                    same connector's spec",
        unless: "",
    },
    Clause {
        id: "P-ORDER",
        statement: "a call before the handshake or before the configuration, and a second \
                    handshake or configuration, are refused with typed errors",
        unless: "",
    },
    Clause {
        id: "P-ROLE",
        statement: "a role the connector does not serve is refused as unsupported",
        unless: "the connector serves both roles",
    },
    Clause {
        id: "P-LIMITS",
        statement: "a configuration, a source's cursor or a destination's batch frame beyond the \
                    connector's limit is refused with `limit_exceeded`",
        unless: "the connector declares no limit this host can exceed",
    },
    Clause {
        id: "P-HEARTBEAT",
        statement: "each heartbeat is answered with its sequence number, in order",
        unless: "",
    },
    Clause {
        id: "P-MALFORMED",
        statement: "a call the connector cannot read, or a frame it cannot decode, is refused \
                    with a typed error, and the connection serves on",
        unless: "",
    },
    Clause {
        id: "P-CREDIT",
        statement: "a read sends nothing more once its credit is spent, until more is granted",
        unless: "the connector is certified as a destination, which grants credit and spends none",
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

/// What a clause found: nothing wrong, a violation, a reason it does not apply to what the
/// connector declares, or a reason what it requires was not seen.
pub(crate) enum Found {
    Kept,
    Broken(Violation),
    Inapplicable(String),
    Unobserved(String),
}

impl From<Result<(), Violation>> for Found {
    fn from(result: Result<(), Violation>) -> Self {
        match result {
            Ok(()) => Self::Kept,
            Err(violation) => Self::Broken(violation),
        }
    }
}

impl From<Result<Found, Violation>> for Found {
    fn from(result: Result<Found, Violation>) -> Self {
        result.unwrap_or_else(Self::Broken)
    }
}

/// Checks every protocol clause against `target` as `role`, with `config`, telling `observed`
/// each result as its check ends.
pub(crate) async fn check(
    target: &Target,
    role: Role,
    config: &serde_json::Value,
    observed: &Observed,
) -> Vec<ClauseResult> {
    let config = config.to_string();
    let mut results = Vec::new();
    for clause in PROTOCOL_CLAUSES {
        let checking = async {
            match clause.id {
                "P-HANDSHAKE" => handshake::answered(target, role, &config).await,
                "P-ORDER" => handshake::ordered(target, role, &config).await,
                "P-ROLE" => handshake::roles(target, role, &config).await,
                "P-LIMITS" => limits::kept(target, role, &config).await,
                "P-HEARTBEAT" => heartbeat::echoed(target, role, &config).await,
                "P-MALFORMED" => malformed::refused(target, role, &config).await,
                _ => credit::respected(target, role, &config).await,
            }
        };
        let outcome = match within(checking).await {
            Found::Kept => Outcome::Passed,
            Found::Broken(Violation(reason)) => Outcome::Failed(reason.into()),
            Found::Inapplicable(reason) => Outcome::Inapplicable(reason.into()),
            Found::Unobserved(reason) => Outcome::Unobserved(reason.into()),
        };
        let result = ClauseResult {
            clause: *clause,
            outcome,
        };
        observed.tell(result.clone());
        results.push(result);
    }
    results
}

/// `checking`'s finding, or a violation once it takes longer than [`CLAUSE_TIME`].
async fn within(checking: impl Future<Output = Found>) -> Found {
    tokio::time::timeout(CLAUSE_TIME, checking)
        .await
        .unwrap_or_else(|_| Found::Broken(Violation(format!("took longer than {CLAUSE_TIME:?}"))))
}

/// A handshake as `role`, at `major`, offering no feature, as an engine's does.
pub(crate) fn request(role: Role, major: u32) -> v1::HandshakeRequest {
    v1::HandshakeRequest {
        protocol_major: major,
        protocol_minor: PROTOCOL_MINOR,
        features: Vec::new(),
        role: wire_role(role) as i32,
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

/// A client of `target`, handshaken as `role` and configured with `config`, and the connector's
/// answer to the handshake.
async fn handshaken(
    target: &Target,
    role: Role,
    config: &str,
) -> Result<(Client, v1::HandshakeResponse), Violation> {
    let mut client = target.client().await.map_err(Violation::of)?;
    let answer = client
        .handshake(request(role, PROTOCOL_MAJOR))
        .await
        .map_err(|status| format!("the handshake failed: {}", error(&status)))?;
    client
        .configure(configure_request(config))
        .await
        .map_err(|status| format!("the configuration failed: {}", error(&status)))?;
    Ok((client, answer.into_inner()))
}

/// The configuration `config`, as sent.
pub(crate) fn configure_request(config: &str) -> v1::ConfigureRequest {
    v1::ConfigureRequest {
        config_json: config.to_owned(),
    }
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

/// How a streaming call ended, when it was refused: before its answer's headers, or as the first
/// message of its answer.
async fn first_refusal<T>(result: Result<Response<Streaming<T>>, Status>) -> Result<(), Status> {
    let mut answer = result?.into_inner();
    answer.message().await.map(drop)
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
