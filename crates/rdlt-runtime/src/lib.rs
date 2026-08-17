//! # rdlt-runtime
//!
//! The provider layer over spawned connectors. The client crate knows
//! how to DRIVE a served connector once a socket exists; this crate
//! knows how to GET one — it owns the lifecycle of a connector PROCESS:
//! resolve, spawn, supervise, kill.
//!
//! [`provider::Provider`] turns a connector requirement plus the
//! connector's own config document into a ready SPI object, and
//! [`local::Local`] is its implementation for binaries on this machine:
//! resolve by the PATH convention (or the requirement's explicit
//! `path`), spawn under the frozen contract, read the one stdout
//! handshake line, then hand the socket to the client crate to dial and
//! verify. The private `spawn` module is the OS seam alone.
//!
//! [`managed::Managed`] is what a provider hands back: the wire adapter
//! plus the handshake's findings plus a [`managed::Guard`]. It
//! implements the SPI's `Source`/`Destination` by delegation, so
//! `Engine::new` takes it unchanged and the guard's lifetime rides the
//! engine's `Arc` — the connector process provably outlives the run,
//! and dies with its socket unlinked when the last holder lets go.
//!
//! Every type the client crate names — the requirement, the handshake
//! outcome, roles, the client's error — is spelled at the client's own
//! paths; nothing is re-exported here.
//!
//! The wire underneath is frozen (field numbers never move, evolution
//! is additive), and this crate stays unpublished alongside the
//! protocol and client crates until publishing is scheduled.

pub mod local;
pub mod managed;
pub mod provider;
mod spawn;
