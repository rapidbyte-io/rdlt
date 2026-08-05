//! # rdlt-runtime — the provider layer over spawned connectors (039)
//!
//! The client crate ([`rdlt_connector_client`]) knows how to DRIVE a
//! served connector once a socket exists; this crate knows how to GET
//! one: [`ConnectorProvider`] turns a [`ConnectorRequirement`] plus the
//! connector's own config document into a ready-to-use SPI object, and
//! [`LocalBinaryConnectorProvider`] is its first implementation —
//! resolve a binary (D-039-1's PATH convention, or the requirement's
//! explicit `path` override), spawn it, read the one stdout handshake
//! line, dial, handshake (the client verifies identity, D-039-2), and
//! wrap the adapter with a [`LifecycleGuard`] so the process dies and
//! its socket unlinks when the last holder lets go.
//!
//! [`ManagedSource`]/[`ManagedDestination`] are what a provider hands
//! back: the remote adapter plus what the handshake established
//! (identity, resolved version, negotiated protocol,
//! state-format versions) plus the guard — and they IMPLEMENT the
//! SPI's `Source`/`Destination` by delegation, so `Engine::new` takes
//! them unchanged and the guard's lifetime rides the engine's `Arc`
//! (the connector process provably outlives the run).
//!
//! **EXPERIMENTAL**, exactly as far as the protocol crate is (ADR 0001
//! D8): the wire is versioned but unfrozen, so this crate stays
//! unpublished alongside it.
//!
//! The modules are private and the surface below is the one canonical
//! path to every name.

mod local;
mod managed;
mod provider;
mod requirement;

pub use local::LocalBinaryConnectorProvider;
pub use managed::{LifecycleGuard, ManagedDestination, ManagedSource};
pub use provider::{ConnectorProvider, ProviderError};
pub use requirement::{ClientError, ConnectorRequirement, HandshakeOutcome, Role};
