//! # rdlt-connector-client — the wire client an adapter drives a served
//! connector through (039)
//!
//! The out-of-process counterpart to the sdk's `serve` half: [`dial`]
//! connects to the Unix domain socket a spawned connector's handshake
//! line advertised, [`handshake`] verifies the connector is the one the
//! provider resolved (D-039-2) and decodes its self-description, and
//! the error module maps wire [`proto::ErrorFrame`]s back to the SPI's
//! own classifications so the engine's retry machinery never learns the
//! wire exists. [`source::Source`] is those seams composed into the
//! SPI's read half — a `Source` whose every method is an RPC;
//! [`destination::Destination`] is its write-side twin, boxing the
//! sdk's own `Session` over a [`destination::Backend`] so the D3
//! exactly-once choreography runs client-side by identical type;
//! `rdlt-runtime` re-exports [`ConnectorRequirement`] (the client
//! verifies, the runtime resolves).
//!
//! Every wire await — the dial, the handshake, each read frame's
//! quiet interval, each reply — is bounded by the requirement's RPC
//! deadline ([`DEFAULT_RPC_DEADLINE`], overridable through
//! [`ConnectorRequirement::with_rpc_deadline`]): a dead OR silent
//! connector yields a typed [`ClientError::Timeout`] within it, never
//! a hang.
//!
//! The wire this crate speaks is FROZEN (2026-08-07; ADR 0001 D8's
//! amendment): field numbers never move, evolution is additive only,
//! and an unrecognized value from a newer peer is tolerated safe-loud
//! rather than guessed at — which is why [`ClientError::Handshake`]
//! normalizes an `Unspecified` or unknown [`Classification`] to
//! `Fatal`. The crate stays unpublished alongside the protocol crate:
//! that posture is a separate, owner-scheduled decision and did not
//! move with the freeze.
//!
//! Every name has exactly one canonical path: the adapter types live
//! at their modules — [`source::Source`], [`destination::Destination`],
//! [`destination::Backend`] — with no crate-root aliases, and the
//! wiring seams below (dial, error mapping, handshake) re-export flat
//! from private modules.
//!
//! [`proto::ErrorFrame`]: rdlt_connector_protocol::proto::ErrorFrame

pub mod destination;
mod dial;
mod error;
#[doc(hidden)]
pub mod fuzzing;
mod gate;
mod handshake;
pub mod source;

pub use dial::{connector_client, destination_client, dial, source_client};
pub use error::{Classification, ClientError, TimedOutOperation};
pub use handshake::{
    ConnectorRequirement, DEFAULT_RPC_DEADLINE, HandshakeOutcome, Role, handshake,
};

// `error::source_error_from_frame`/`error::dest_error_from_frame` stay
// crate-internal at their defining paths — the adapters reach them as
// `crate::error::…`.
