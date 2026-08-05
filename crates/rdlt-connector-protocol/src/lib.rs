//! # rdlt-connector-protocol — the out-of-process connector wire protocol (v0)
//!
//! **EXPERIMENTAL** (ADR 0001 D8): versioned but unfrozen until the
//! conformance kit and one non-Rust implementation have exercised it. Field
//! numbers are frozen from day one — evolution is additive only — but the
//! surface itself may still move.
//!
//! Two halves: [`handshake`] is the plaintext stdout line a spawned
//! connector prints before it starts serving; [`proto`] is the generated
//! gRPC/protobuf types and services compiled at build time from
//! `proto/rdlt_connector_v0.proto` (hermetically — `build.rs` vendors its
//! own `protoc`, so no system install is required).

pub mod handshake;

/// Generated protobuf/gRPC types for `rdlt.connector.v0`: [`Connector`],
/// [`SourceService`], [`DestinationService`] and every request/reply
/// message the proto declares. See `proto/rdlt_connector_v0.proto` for the
/// source of truth and `src/generated.rs` for how the build-time output
/// lands here.
///
/// [`Connector`]: proto::connector_server::Connector
/// [`SourceService`]: proto::source_service_server::SourceService
/// [`DestinationService`]: proto::destination_service_server::DestinationService
pub mod proto {
    include!("generated.rs");
}

/// The protocol version this crate's generated code implements — the value
/// a connector advertises and a provider negotiates over
/// [`proto::HandshakeRequest::protocol_version`]. Distinct from the
/// handshake line's own format version, which is pinned separately at `1`
/// (see [`handshake::Line`]).
pub const PROTOCOL_VERSION: u32 = 0;
