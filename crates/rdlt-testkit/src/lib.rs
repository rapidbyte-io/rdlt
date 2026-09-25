//! Drawn Arrow data and an exact value model, shared by rdlt's property tests and its
//! deterministic simulation.
//!
//! [`drawn`] draws every logical type in every Arrow encoding a source may send, with values across
//! each type's whole range; [`canon`] says what each value means, whatever type or text holds it;
//! [`decode`] reads stored cells back to that meaning. Property tests shrink what they draw; the
//! simulation draws the same values from its seed.

#![expect(
    clippy::missing_panics_doc,
    reason = "a test kit panics only on values its generators never draw"
)]

pub mod canon;
pub mod decode;
pub mod draw;
pub mod drawn;

/// Cases a property test runs: `PROPTEST_CASES` where set, for long local runs, else `default`.
pub fn cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(default)
}
