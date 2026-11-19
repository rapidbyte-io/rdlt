//! The facade as an embedder uses it.

#![forbid(unsafe_code)]

mod events;
mod facade;
mod ledger;
mod masks;
mod metered;
mod placement;
#[cfg(feature = "local")]
mod secrets;
mod support;
#[cfg(feature = "otel")]
mod telemetry;
#[cfg(any(feature = "local", feature = "remote"))]
mod told;
