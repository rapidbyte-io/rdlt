//! Engine-private test doubles: what the engine's own suites need beyond
//! the shipped memory pair.
//!
//! - [`crash`] — a destination wrapper that fails once at a chosen point.
//! - [`scripted`] — a source with injected failures and a `since` log.

pub(crate) mod crash;
pub(crate) mod scripted;
