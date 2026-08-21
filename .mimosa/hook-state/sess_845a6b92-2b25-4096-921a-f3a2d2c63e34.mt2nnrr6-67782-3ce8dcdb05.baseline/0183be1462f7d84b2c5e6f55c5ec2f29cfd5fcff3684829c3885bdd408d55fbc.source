//! Connector conformance: "certified = passes conformance". Every check
//! names the SPI clause it enforces, so a failure reads as "violates D3",
//! not "test failed". The clause ids are a FIXED vocabulary — sources
//! S1/S2/S4, destinations D1–D6 and D8 (each suite's module doc lists its
//! own); renumbering is forbidden, and a clause without a check keeps its
//! number until one exists.
//!
//! - [`source`] — the source suite and its clause list.
//! - [`destination`] — the destination suite, its clause list, and the
//!   author-supplied [`destination::TableProbe`].
//!
//! Both suites return one [`Verdict`]; [`assert_conformant`] is the
//! assert-style consumption for CI.

pub mod destination;
pub mod source;

use std::fmt;

/// One violated contract clause, with an actionable diagnostic.
#[derive(Debug, Clone)]
pub struct Failure {
    /// Clause id from the contract, e.g. `"S1"`, `"D3"`.
    pub clause: &'static str,
    /// What the connector did, in terms an author can act on.
    pub message: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "violates clause {}: {}", self.clause, self.message)
    }
}

/// One clause a suite could not exercise, with the honest reason — a
/// non-verdict: nothing was proven (not a pass) and nothing was violated
/// (not a failure). A certifier renders these as SKIP lines; a suite run
/// that expects every clause exercised promotes them back to failures
/// through [`Verdict::expecting_no_skips`].
#[derive(Debug, Clone)]
pub struct Skip {
    /// Clause id from the contract, e.g. `"S2"`.
    pub clause: &'static str,
    /// Why the clause could not be exercised.
    pub reason: String,
}

impl Skip {
    /// The one skip→failure promotion spelling — every strict fold
    /// routes through here.
    pub fn into_failure(self) -> Failure {
        Failure {
            clause: self.clause,
            message: format!("not exercised: {}", self.reason),
        }
    }
}

/// What one conformance run concluded — the ONE shape both suites
/// return. The fields are private so a caller cannot read the failures
/// alone and silently bypass the skips: it either takes the strict fold
/// or acknowledges the skips by name.
#[derive(Debug, Default)]
pub struct Verdict {
    /// Violated clauses, in discovery order.
    pub(crate) failures: Vec<Failure>,
    /// Clauses the suite could not exercise, with reasons.
    pub(crate) skips: Vec<Skip>,
    /// Clauses whose checks RAN TO A VERDICT — a recorded failure, an
    /// honest skip, or silence meaning "nothing found against it". A
    /// clause outside this set that also reports no failure was never
    /// reached (the run died mid-suite), and a consumer minting a pass
    /// for it would certify silence.
    pub(crate) concluded: Vec<&'static str>,
}

impl Verdict {
    /// The strict fold, THE default consumption: the failures plus each
    /// skip promoted — read this way, a suite outcome can never certify a
    /// skipped clause silently green.
    pub fn expecting_no_skips(self) -> Vec<Failure> {
        let mut failures = self.failures;
        failures.extend(self.skips.into_iter().map(Skip::into_failure));
        failures
    }

    /// The explicit escape: the caller ACKNOWLEDGES the skips by taking
    /// them separately — the only way to read failures without the
    /// promotion. The concluded record rides along as the third element
    /// so it cannot be left behind: a consumer folding failures and skips
    /// must refuse to mint a pass for an asserted clause outside it.
    pub fn tolerating_skips(self) -> (Vec<Failure>, Vec<Skip>, Vec<&'static str>) {
        (self.failures, self.skips, self.concluded)
    }
}

/// Panics with all failures listed — the assert-style entry point for CI.
pub fn assert_conformant(failures: Vec<Failure>) {
    if !failures.is_empty() {
        let listing = failures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n  - ");
        panic!("connector fails conformance:\n  - {listing}");
    }
}
