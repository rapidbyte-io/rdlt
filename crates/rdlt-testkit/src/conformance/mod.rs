//! Public connector conformance suites: "certified = passes conformance".
//! Every check names the connector-SPI clause it enforces, so a failure
//! reads as "violates D3", not "test failed". The clause ids are a FIXED
//! vocabulary — the suites assert source clauses S1/S2/S4 and destination
//! clauses D1–D6 and D8 (each suite's module doc lists its own);
//! renumbering is forbidden, and a clause not yet asserted keeps its
//! number until a check exists for it.

pub mod destination;
mod failure;
pub mod source;

pub use failure::{Conformance, ConformanceFailure, ConformanceSkip, assert_conformant};
