//! Public connector conformance suites: "certified = passes conformance".
//! Every check names the connector-SPI clause it enforces — the destination
//! clauses D1–D8 and source clauses S1–S6 defined by these suites — so a
//! failure reads as "violates D3", not "test failed".

pub mod dest;
mod failure;
pub mod source;

pub use failure::{ConformanceFailure, assert_conformant};
