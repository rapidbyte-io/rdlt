//! The conformance verdict vocabulary: one violated clause, one clause
//! a suite could not exercise, and the assert-style entry point that
//! reports every violation at once.

use std::fmt;

/// One violated contract clause, with an actionable diagnostic.
#[derive(Debug, Clone)]
pub struct ConformanceFailure {
    /// Clause id from the contract, e.g. `"S1"`, `"D3"`.
    pub clause: &'static str,
    /// What the connector did, in terms an author can act on.
    pub message: String,
}

/// One clause a suite could not exercise, with the honest reason — a
/// non-verdict: nothing was proven (not a pass) and nothing was
/// violated (not a failure). A certifier renders these as SKIP lines;
/// suites that expect every clause exercised promote them back to
/// failures (see `SourceConformance::expecting_no_skips`).
#[derive(Debug, Clone)]
pub struct ConformanceSkip {
    /// Clause id from the contract, e.g. `"S2"`.
    pub clause: &'static str,
    /// Why the clause could not be exercised.
    pub reason: String,
}

impl fmt::Display for ConformanceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "violates clause {}: {}", self.clause, self.message)
    }
}

/// Panics with all failures listed — the assert-style entry point for CI.
pub fn assert_conformant(failures: Vec<ConformanceFailure>) {
    if !failures.is_empty() {
        let listing = failures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  - ");
        panic!("connector fails conformance:\n  - {listing}");
    }
}

/// What one conformance run concluded — the ONE shape both suites
/// return (round-5 fix: source and destination carried verbatim
/// twins): the violated clauses, and the clauses the suite could not
/// exercise, each with its reason. The role-specific names
/// (`SourceConformance`, `DestinationConformance`) are aliases so a
/// caller reads the suite it certified.
#[derive(Debug, Default)]
pub struct Conformance {
    /// Violated clauses, in discovery order. PRIVATE (round-7 fix):
    /// reading failures alone silently bypassed the skip guard — a
    /// caller must now either take the strict fold or acknowledge the
    /// skips by name.
    pub(crate) failures: Vec<ConformanceFailure>,
    /// Clauses the suite could not exercise — the source suite's S2
    /// snapshot door, the destination suite's unreached-abort tail —
    /// with reasons.
    pub(crate) skips: Vec<ConformanceSkip>,
}

impl ConformanceSkip {
    /// The one skip→failure promotion spelling — every strict fold
    /// routes through here.
    pub fn into_failure(self) -> ConformanceFailure {
        ConformanceFailure {
            clause: self.clause,
            message: format!("not exercised: {}", self.reason),
        }
    }
}

impl Conformance {
    /// The strict fold, THE default consumption (round-7 fix): the
    /// failures plus each skip promoted — a suite outcome read this
    /// way can never certify a skipped clause silently green.
    pub fn into_failures(self) -> Vec<ConformanceFailure> {
        self.expecting_no_skips()
    }

    /// The strict fold under its assert-helper name; identical to
    /// [`Self::into_failures`].
    pub fn expecting_no_skips(self) -> Vec<ConformanceFailure> {
        let mut failures = self.failures;
        failures.extend(self.skips.into_iter().map(ConformanceSkip::into_failure));
        failures
    }

    /// The explicit escape: the caller ACKNOWLEDGES the skips by
    /// taking them separately — the only way to read failures without
    /// the promotion.
    pub fn tolerating_skips(self) -> (Vec<ConformanceFailure>, Vec<ConformanceSkip>) {
        (self.failures, self.skips)
    }
}
