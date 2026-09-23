//! Conformance clauses that check a connector in-process, inside `cargo test`.
//!
//! ```no_run
//! # async fn example() {
//! use rdlt_connector::testing::certify_source;
//! # struct Tickets;
//! # impl rdlt_connector::SourceConnector for Tickets {
//! #     const ID: &'static str = "io.example.tickets";
//! #     const VERSION: &'static str = "0.1.0";
//! #     type Config = serde_json::Value;
//! #     async fn connect(_: serde_json::Value, _: &rdlt_connector::ConnectContext) -> rdlt_connector::Result<Self> { Ok(Self) }
//! #     async fn check(&self) -> rdlt_connector::Result<()> { Ok(()) }
//! #     fn streams(&self) -> rdlt_connector::Streams<Self> { rdlt_connector::Streams::new() }
//! # }
//! certify_source::<Tickets>(serde_json::json!({ "token": "t" })).await.assert_passed();
//! # }
//! ```

mod destination;
mod source;
#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::time::Duration;

pub use destination::{
    DESTINATION_CLAUSES, Probe, certify_destination, certify_destination_factory,
};
pub use source::{SOURCE_CLAUSES, certify_source, certify_source_factory};

/// How long any single connector call may take during certification.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// One conformance clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clause {
    /// The clause's id, such as `S-RESUME`.
    pub id: &'static str,
    /// What the clause requires.
    pub statement: &'static str,
}

/// The result of checking one clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The connector meets the clause.
    Passed,
    /// The connector breaks the clause, for the stated reason.
    Failed(String),
    /// The clause does not apply to this connector, for the stated reason.
    Skipped(String),
}

/// One clause and its outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClauseResult {
    /// The clause.
    pub clause: Clause,
    /// Its outcome.
    pub outcome: Outcome,
}

/// Every clause's outcome for one connector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// The connector's id.
    pub connector: String,
    /// One result per clause, in clause order.
    pub results: Vec<ClauseResult>,
}

impl Report {
    /// The failed clauses.
    pub fn failures(&self) -> impl Iterator<Item = &ClauseResult> {
        self.results
            .iter()
            .filter(|result| matches!(result.outcome, Outcome::Failed(_)))
    }

    /// Whether no clause failed.
    pub fn passed(&self) -> bool {
        self.failures().next().is_none()
    }

    /// The outcome of the clause with `id`.
    pub fn outcome(&self, id: &str) -> Option<&Outcome> {
        self.results
            .iter()
            .find(|result| result.clause.id == id)
            .map(|result| &result.outcome)
    }

    /// Panics with the whole report unless every clause passed or was skipped.
    ///
    /// # Panics
    ///
    /// Panics when a clause failed.
    pub fn assert_passed(&self) {
        assert!(self.passed(), "{self}");
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "certification of {}", self.connector)?;
        for result in &self.results {
            match &result.outcome {
                Outcome::Passed => writeln!(f, "  pass {}", result.clause.id)?,
                Outcome::Failed(reason) => writeln!(
                    f,
                    "  FAIL {}: {} ({reason})",
                    result.clause.id, result.clause.statement
                )?,
                Outcome::Skipped(reason) => writeln!(f, "  skip {}: {reason}", result.clause.id)?,
            }
        }
        Ok(())
    }
}

/// Why a connector breaks a clause.
#[derive(Debug)]
struct Violation(String);

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

/// Runs a clause body, turning a violation into a failure.
fn outcome(result: Result<(), Violation>) -> Outcome {
    match result {
        Ok(()) => Outcome::Passed,
        Err(Violation(reason)) => Outcome::Failed(reason),
    }
}

/// Awaits `future` for at most [`CALL_TIMEOUT`], naming `what` if it takes longer.
async fn bounded<T>(what: &str, future: impl Future<Output = T>) -> Result<T, Violation> {
    tokio::time::timeout(CALL_TIMEOUT, future)
        .await
        .map_err(|_| format!("{what} took longer than {CALL_TIMEOUT:?}").into())
}
