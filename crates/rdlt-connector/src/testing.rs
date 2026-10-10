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

mod allowance;
mod denoted;
mod destination;
mod limits;
mod reason;
pub mod render;
mod source;
#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;

pub use allowance::Allowance;
pub use denoted::denoted;
pub use destination::{
    DESTINATION_CLAUSES, Probe, Unprobed, certify_destination, certify_destination_factory,
    certify_destination_factory_observed, read_back_integers,
};
use limits::{CALL_TIMEOUT, CLAUSE_TIMEOUT};
pub use limits::{REASON_BYTES, RENDERED_BYTES};
pub use reason::Reason;
pub use source::{
    SOURCE_CLAUSES, certify_source, certify_source_factory, certify_source_factory_observed,
};

/// Runs a clause for at most [`CLAUSE_TIMEOUT`].
async fn timed(check: impl Future<Output = Result<(), Violation>>) -> Result<(), Violation> {
    tokio::time::timeout(CLAUSE_TIMEOUT, check)
        .await
        .unwrap_or_else(|_| Err(format!("the clause took longer than {CLAUSE_TIMEOUT:?}").into()))
}

/// One conformance clause.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clause {
    /// The clause's id, such as `S-RESUME`.
    pub id: &'static str,
    /// What the clause requires.
    pub statement: &'static str,
    /// What a connector declares that makes the clause not apply to it; empty for a clause that
    /// applies to every connector of its role.
    pub unless: &'static str,
}

/// The result of checking one clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The connector was seen to meet the clause.
    Passed,
    /// The connector breaks the clause, for the stated reason.
    Failed(Reason),
    /// The clause does not apply to this connector, by what it declares, for the stated reason.
    Inapplicable(Reason),
    /// The clause applies, yet what it requires was not seen, for the stated reason: it neither
    /// passed nor failed.
    Unobserved(Reason),
}

/// One clause and its outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClauseResult {
    /// The clause.
    pub clause: Clause,
    /// Its outcome.
    pub outcome: Outcome,
    /// What is worth saying of an outcome beside its reason: of a clause that passed, the
    /// proof it passed on, where a clause passes on more than one.
    pub note: Option<Reason>,
}

/// What a certification has found so far: each clause's result as its check ends, and the
/// connector's id once it is known.
///
/// Whoever cuts a certification short reads it, to report what was found before the cut.
#[derive(Clone, Debug, Default)]
pub struct Observed {
    found: std::sync::Arc<std::sync::Mutex<(Option<String>, Vec<ClauseResult>)>>,
}

impl Observed {
    /// Nothing found yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Tells the id of the connector under certification.
    pub fn named(&self, connector: &str) {
        self.found().0 = Some(connector.to_owned());
    }

    /// Tells a clause's result, as its check ends.
    pub fn tell(&self, result: ClauseResult) {
        self.found().1.push(result);
    }

    /// The connector's id, once it is known.
    pub fn connector(&self) -> Option<String> {
        self.found().0.clone()
    }

    /// The results told so far, in the order their checks ended.
    pub fn results(&self) -> Vec<ClauseResult> {
        self.found().1.clone()
    }

    fn found(&self) -> std::sync::MutexGuard<'_, (Option<String>, Vec<ClauseResult>)> {
        let found = self.found.lock();
        found.unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// What a report's outcomes amount to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Every clause that applies was seen to be met, and one at least applies.
    Passed,
    /// No clause failed, yet one that applies was not observed, or none applies.
    Incomplete,
    /// A clause failed.
    Failed,
}

impl Verdict {
    /// The verdict's name, as reports print it: `passed`, `incomplete` or `failed`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Incomplete => "incomplete",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
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

    /// The clauses that apply and were not observed.
    pub fn unobserved(&self) -> impl Iterator<Item = &ClauseResult> {
        self.results
            .iter()
            .filter(|result| matches!(result.outcome, Outcome::Unobserved(_)))
    }

    /// What the outcomes amount to: failed when a clause failed, incomplete when one that
    /// applies was not observed or none passed, and passed otherwise.
    pub fn verdict(&self) -> Verdict {
        let passed = |result: &ClauseResult| result.outcome == Outcome::Passed;
        if self.failures().next().is_some() {
            Verdict::Failed
        } else if self.unobserved().next().is_some() || !self.results.iter().any(passed) {
            Verdict::Incomplete
        } else {
            Verdict::Passed
        }
    }

    /// The verdict, and how many clauses passed, failed, were not observed and do not apply, in
    /// one line.
    pub fn summary(&self) -> String {
        let count = |counted: fn(&Outcome) -> bool| {
            let results = self.results.iter();
            results.filter(|result| counted(&result.outcome)).count()
        };
        format!(
            "{}: {} passed, {} failed, {} not observed, {} not applicable",
            self.verdict(),
            count(|outcome| matches!(outcome, Outcome::Passed)),
            count(|outcome| matches!(outcome, Outcome::Failed(_))),
            count(|outcome| matches!(outcome, Outcome::Unobserved(_))),
            count(|outcome| matches!(outcome, Outcome::Inapplicable(_))),
        )
    }

    /// Whether every clause that applies was seen to be met: the [`Verdict::Passed`] verdict.
    pub fn passed(&self) -> bool {
        self.verdict() == Verdict::Passed
    }

    /// The outcome of the clause with `id`.
    pub fn outcome(&self, id: &str) -> Option<&Outcome> {
        self.results
            .iter()
            .find(|result| result.clause.id == id)
            .map(|result| &result.outcome)
    }

    /// What the clause with `id` is noted to have passed on.
    pub fn note(&self, id: &str) -> Option<&str> {
        let result = self.results.iter().find(|result| result.clause.id == id)?;
        result.note.as_ref().map(Reason::as_str)
    }

    /// Panics with the whole report unless it [passed](Self::passed).
    ///
    /// # Panics
    ///
    /// Panics when a clause failed, one that applies was not observed, or none passed.
    pub fn assert_passed(&self) {
        assert!(self.passed(), "{self}");
    }

    /// Panics with the whole report when a clause failed, or none passed: what a certification
    /// that cannot observe every clause, as one with no probe, asserts.
    ///
    /// # Panics
    ///
    /// Panics when a clause failed, or none passed.
    pub fn assert_none_failed(&self) {
        let passed = |result: &ClauseResult| result.outcome == Outcome::Passed;
        let kept = self.verdict() != Verdict::Failed && self.results.iter().any(passed);
        assert!(kept, "{self}");
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let connector = crate::text::shown(&self.connector, REASON_BYTES);
        writeln!(f, "certification of {connector}")?;
        for result in &self.results {
            let id = result.clause.id;
            match &result.outcome {
                Outcome::Passed => match &result.note {
                    Some(note) => writeln!(f, "  pass {id} ({note})")?,
                    None => writeln!(f, "  pass {id}")?,
                },
                Outcome::Failed(reason) => {
                    writeln!(f, "  FAIL {id}: {} ({reason})", result.clause.statement)?;
                }
                Outcome::Inapplicable(reason) => writeln!(f, "  n/a  {id}: {reason}")?,
                Outcome::Unobserved(reason) => writeln!(f, "  skip {id}: {reason}")?,
            }
        }
        writeln!(f, "{}", self.summary())
    }
}

/// Why a clause did not pass: the connector breaks it, or what it requires could not be seen.
#[derive(Debug)]
struct Violation {
    reason: Reason,
    /// Whether the clause, rather than broken, was not observed.
    unobserved: bool,
}

impl Violation {
    /// A clause not observed, for `reason`.
    fn unobserved(reason: impl fmt::Display) -> Self {
        Self {
            reason: Reason::new(reason),
            unobserved: true,
        }
    }

    /// This violation, said to be of `what`.
    fn of(self, what: impl fmt::Display) -> Self {
        Self {
            reason: Reason::new(format_args!("{what}: {}", self.reason)),
            unobserved: self.unobserved,
        }
    }

    /// The outcome of the clause this ends.
    fn outcome(self) -> Outcome {
        if self.unobserved {
            Outcome::Unobserved(self.reason)
        } else {
            Outcome::Failed(self.reason)
        }
    }
}

impl From<String> for Violation {
    fn from(reason: String) -> Self {
        Self {
            reason: Reason::new(reason),
            unobserved: false,
        }
    }
}

impl From<&str> for Violation {
    fn from(reason: &str) -> Self {
        Self::from(reason.to_owned())
    }
}

impl From<fmt::Arguments<'_>> for Violation {
    fn from(reason: fmt::Arguments<'_>) -> Self {
        Self {
            reason: Reason::new(reason),
            unobserved: false,
        }
    }
}

/// Runs a clause body, turning a violation into its outcome.
fn outcome(result: Result<(), Violation>) -> Outcome {
    match result {
        Ok(()) => Outcome::Passed,
        Err(violation) => violation.outcome(),
    }
}

/// Awaits `future` for at most [`CALL_TIMEOUT`], naming `what` if it takes longer.
async fn bounded<T>(what: &str, future: impl Future<Output = T>) -> Result<T, Violation> {
    tokio::time::timeout(CALL_TIMEOUT, future)
        .await
        .map_err(|_| format!("{what} took longer than {CALL_TIMEOUT:?}").into())
}

/// Awaits a connector call for at most [`CALL_TIMEOUT`]; its error becomes the violation.
async fn bounded_call<T>(
    what: &str,
    call: impl Future<Output = crate::error::Result<T>>,
) -> Result<T, Violation> {
    bounded(what, call)
        .await?
        .map_err(|error| Violation::from(described(&error)))
}

/// `error`, and each error that caused it, in turn.
fn described(error: &dyn std::error::Error) -> String {
    let mut described = error.to_string();
    let mut cause = error.source();
    while let Some(error) = cause {
        described = format!("{described}: {error}");
        cause = error.source();
    }
    described
}
