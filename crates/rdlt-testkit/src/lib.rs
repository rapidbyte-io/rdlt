//! Drawn Arrow data and an exact value model, shared by rdlt's property tests and its
//! deterministic simulation.
//!
//! [`drawn`] draws every logical type in every Arrow encoding a source may send, with values across
//! each type's whole range; [`canon`] says what each value means, whatever type or text holds it;
//! [`decode`] reads stored cells back to that meaning. Property tests shrink what they draw; the
//! simulation draws the same values from its seed.
//!
//! [`cancel`] drops a call at each point it waits and checks what the call left, for every object
//! a caller may call again after a call it dropped.
//!
//! [`fake`], with the `fake` feature, serves a connector that answers the handshake and then
//! breaks the protocol, over any transport and on the clock of the runtime that serves it.

#![forbid(unsafe_code)]
#![expect(
    clippy::missing_panics_doc,
    reason = "a test kit panics only on values its generators never draw"
)]

#[cfg(panic = "abort")]
compile_error!(
    "rdlt-testkit's cancel harness reports the wait a panic came from: build with panic = \"unwind\""
);

pub mod cancel;
pub mod canon;
pub mod decode;
pub mod draw;
pub mod drawn;
#[cfg(feature = "fake")]
pub mod fake;
pub mod held;
pub mod nested;
pub mod process;
pub mod required;
pub mod tls;

/// Cases a property test runs: `PROPTEST_CASES` where set, for long local runs, else `default`.
pub fn cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(default)
}
