//! The facade as an embedder uses it.

#![forbid(unsafe_code)]

mod events;
mod facade;
mod ledger;
mod metered;
mod placement;
#[cfg(feature = "local")]
mod secrets;
mod support;
#[cfg(feature = "otel")]
mod telemetry;
