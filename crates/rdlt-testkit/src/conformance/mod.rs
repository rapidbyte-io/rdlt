//! Public connector conformance suites: "certified = passes conformance".
//! Every check names the connector-SPI clause it enforces — the destination
//! clauses D1–D8 and source clauses S1–S6 defined by these suites — so a
//! failure reads as "violates D3", not "test failed".

pub mod dest;
pub mod source;

use std::fmt;

/// One violated contract clause, with an actionable diagnostic.
#[derive(Debug, Clone)]
pub struct ConformanceFailure {
    /// Clause id from the contract, e.g. `"S1"`, `"D3"`.
    pub clause: &'static str,
    pub message: String,
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
