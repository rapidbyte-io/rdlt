//! # rdlt-connector-client — the wire client an adapter drives a served
//! connector through
//!
//! The out-of-process counterpart to the sdk's `serve` half: [`wire::dial`]
//! connects to the Unix domain socket a spawned connector's handshake
//! line advertised, [`handshake::run`] verifies the connector is the
//! one the provider resolved and decodes its self-description, and
//! the error module maps wire [`proto::ErrorFrame`]s back to the SPI's
//! own classifications so the engine's retry machinery never learns the
//! wire exists. [`source::Source`] is those seams composed into the
//! SPI's read half — a `Source` whose every method is an RPC;
//! [`destination::Destination`] is its write-side twin, boxing the
//! sdk's own `Session` over a [`destination::Backend`] so the
//! exactly-once commit choreography runs client-side by identical
//! type; `rdlt-runtime` re-exports [`handshake::Requirement`] (the
//! client verifies, the runtime resolves).
//!
//! Every wire await — the dial, the handshake, each read frame's
//! quiet interval, each reply — is bounded by the requirement's RPC
//! deadline ([`wire::DEFAULT_DEADLINE`], overridable through
//! [`handshake::Requirement::with_rpc_deadline`]): a dead OR silent
//! connector yields a typed [`error::Error::Timeout`] within it, never
//! a hang.
//!
//! The wire this crate speaks is FROZEN (2026-08-07): field numbers
//! never move, evolution is additive only, and an unrecognized value
//! from a newer peer is tolerated safe-loud rather than guessed at —
//! which is why [`error::Error::Handshake`] normalizes an
//! `Unspecified` or unknown [`error::Classification`] to `Fatal`. The
//! crate stays unpublished alongside the protocol crate: that posture
//! is a separate, owner-scheduled decision and did not move with the
//! freeze.
//!
//! Every name has exactly one canonical path, at its module —
//! [`handshake::Requirement`], [`source::Source`],
//! [`destination::Destination`], [`destination::Backend`], the
//! transport seams at [`wire`], the error surface at [`error`] — with
//! no crate-root aliases.
//!
//! [`proto::ErrorFrame`]: rdlt_connector_protocol::proto::ErrorFrame

pub mod destination;
pub mod error;
#[doc(hidden)]
pub mod fuzzing;
mod gate;
pub mod handshake;
pub mod source;
pub mod wire;
