//! # rdlt-connector-protocol — the out-of-process connector wire protocol (v0)
//!
//! **FROZEN** as of 2026-08-07
//! (`docs/adr/0001-out-of-process-connectors.md`, decision D8 — amended
//! there with the
//! evidence that closed its experimental period: a standalone certifier
//! driving any connector executable in any language against 29
//! fail-proven conformance clauses plus a `SIGKILL` matrix at every
//! message boundary, a non-Rust connector certified by that same
//! binary, and a recorded benchmark session in which every throughput
//! bar held with the connectors out of process).
//!
//! Frozen means the RULES OF CHANGE now bind: field numbers are never
//! renumbered, repurposed, or recycled — a retired number is
//! `reserved`; evolution is ADDITIVE ONLY (new fields take fresh
//! numbers; new messages, RPCs, `oneof` arms and enum values may be
//! added; nothing is removed, narrowed, or given a second meaning); and
//! a receiver tolerates what a newer peer sends without knowing it —
//! an unrecognized [`proto::Classification`] normalizes safe-loud to
//! `Fatal` rather than being guessed retryable. The `#[non_exhaustive]`
//! discipline on the SPI and client types the wire maps onto is what
//! keeps such an addition from being a semver break in Rust.
//!
//! The negotiated version NUMBER stays `0` (see [`PROTOCOL_VERSION`]):
//! it is the identifier both sides compare at the handshake, and
//! bumping it for a freeze that moves no byte would break every shipped
//! handshake for nothing. "v1" is the name of the frozen contract, not
//! a value on the wire. The crate's `README.md` carries the rules in
//! full, the named frozen clauses, and what the freeze deliberately
//! leaves open — three additive doors this proto keeps room for: a
//! `ReadCredit` message should flow control alone ever prove
//! insufficient for backpressure; negotiation semantics for
//! [`proto::HandshakeOk::state_format_versions`], whose field is frozen
//! at its number but ships EMPTY because there is nothing to negotiate
//! until a second state-format version exists; and network transports
//! as a future binding of this same proto.
//!
//! Two halves: [`handshake`] is the plaintext stdout line a spawned
//! connector prints before it starts serving; [`proto`] is the generated
//! gRPC/protobuf types and services compiled at build time from
//! `proto/rdlt_connector_v0.proto` (hermetically — `build.rs` vendors its
//! own `protoc`, so no system install is required).
//!
//! ## Trust model (owner decision D-038-1)
//!
//! Config documents — which may carry credentials — cross the Unix
//! domain socket **in the clear**. There is no protocol-level
//! encryption or authentication in v0: the socket file is created
//! owner-only (`0600`, enforced by the sdk's `serve::common::bind_uds`,
//! not by anything in this crate), and a spawned connector process
//! inherits its operator's trust exactly like any other child process —
//! the same trust boundary a locally-installed CLI plugin or a `sudo`
//! child crosses. Never log `config_json`/`table_schema_json`/any other
//! `*_json` payload verbatim; it may contain a `Secret`'s revealed
//! value. `Secret` references (a config field naming where a credential
//! lives — an env var, a secret-manager path — rather than carrying the
//! credential itself) are the recorded direction for network
//! transports, not built in v0. Network transports (TCP+mTLS for
//! provider-managed remote fleets, ADR 0001 D3) are a future binding of
//! this SAME proto — a different trust model belongs to that binding,
//! not retrofitted onto UDS.
//!
//! ## Payload discipline (ADR 0001 D4)
//!
//! The proto owns RPC shape and field-number evolution; it does NOT own
//! the shape of what rides inside it. Every field named `*_json` carries
//! an opaque `serde_json`-encoded document whose OWN `format_version`
//! (or equivalent) is the source of truth for its evolution — the proto
//! never re-derives or re-validates that structure, only moves the
//! bytes. `arrow_ipc` fields carry raw Arrow IPC *stream* bytes (one
//! schema message, one record-batch message — see [`proto::Write`] and
//! [`proto::ReadFrame`]'s own field docs for the one-batch-per-frame
//! rule), Flight-style without adopting Flight itself. This keeps ONE
//! evolution system per concern: proto/serde drift is confined to the
//! envelope, never the payload.
//!
//! See the crate's `README.md` for the handshake line format spelled
//! out field-by-field, the three services' shapes, and the operational
//! gotchas measured by the research spike (`specs/038-connector-protocol/
//! research.md`) — this module doc stays scoped to what governs the
//! Rust API surface; the README is the page a third-party integrator
//! (including a non-Rust one) actually needs.

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

/// The per-message receive ceiling BOTH sides of the wire install
/// (`.max_decoding_message_size` on every served service wrapper in the
/// sdk's `serve::source::serve_on`/`serve::destination::serve_on`, and
/// the matching decode cap on any client that dials one), replacing
/// tonic's 4 MiB default decode cap. The SPI's byte-budget channels run
/// 8-64 MiB, so ONE Arrow batch in a `Write` frame may legitimately
/// exceed 4 MiB — under tonic's default, such a batch kills the session
/// with an opaque transport `Status`, and the frozen
/// one-batch-per-frame rule means there is NO conforming way to
/// deliver it smaller. h2 flow-control windows remain the PACING
/// mechanism (see the README's flow-control note); this cap is the hard
/// refusal ceiling, deliberately above any in-tree budget. Both sides
/// import THIS constant — a dialing side left at the 4 MiB default dies
/// the same way on the first over-4 MiB `ReadFrame` a server legally
/// sends.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// The protocol version this crate's generated code implements — the value
/// a connector advertises and a provider negotiates over
/// [`proto::HandshakeRequest::protocol_version`]. Distinct from the
/// handshake line's own format version, which is pinned separately at `1`
/// (see [`handshake::Line`]).
pub const PROTOCOL_VERSION: u32 = 0;
