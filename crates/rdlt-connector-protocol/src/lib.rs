//! # rdlt-connector-protocol — the out-of-process connector wire protocol (v1)
//!
//! Frozen 2026-08-07 and RE-OPENED 2026-08-19 by the owner while the
//! workspace is pre-release: both halves of the wire live in this
//! repository, so a change lands in both at once and the negotiated
//! version refuses any peer built against the other. Both manifests
//! carry the same dates. While it is open the rules of change still
//! bind, because they are what keeps a change survivable:
//! field numbers are never renumbered, repurposed, or recycled — a
//! retired number is `reserved`; evolution is ADDITIVE ONLY (new fields
//! take fresh numbers; new messages, RPCs, `oneof` arms and enum values
//! may be added; nothing is removed, narrowed, or given a second
//! meaning); and a receiver tolerates what a newer peer sends without
//! knowing it — an unrecognized [`proto::Classification`] normalizes
//! safe-loud to `Fatal` rather than being guessed retryable.
//!
//! The negotiated version NUMBER is [`PROTOCOL_VERSION`], the
//! identifier both sides compare at the handshake. It moves when the
//! wire moves: a peer built against a different number is refused
//! there, loudly, instead of mis-reading fields that changed shape
//! under it. The proto package carries the same number, so the service
//! paths themselves refuse a mismatched peer one layer lower.
//!
//! Two halves: [`handshake`] is the plaintext stdout line a spawned
//! connector prints before it starts serving; [`proto`] is the
//! generated gRPC/protobuf types and services. (The shared
//! control-and-invisible codepoint table the handshake's socket-path
//! gate refuses by lives in `rdlt_core::inventory` — it is vocabulary
//! several crates agree on, not wire envelope.)
//!
//! Trust model: config documents — which may carry credentials — cross
//! the Unix domain socket in the clear; the wire's boundary is the owner-only
//! (`0600`) socket file plus the operator trust any locally spawned
//! child process inherits, the same boundary a CLI plugin crosses.
//! Never log `config_json`, `table_schema_json`, or any other `*_json`
//! payload verbatim — it may contain a revealed credential.
//!
//! Payload discipline: the proto owns RPC shape and field-number
//! evolution, never the shape of what rides inside it — every `*_json`
//! field carries an opaque `serde_json`-encoded document whose OWN
//! format gate governs its evolution, and `arrow_ipc` fields carry raw
//! Arrow IPC stream bytes, one batch per frame.
//!
//! The crate's `README.md` is the page an integrator (including a
//! non-Rust one) actually needs: the handshake line spelled out
//! field-by-field, the three services, the named freeze clauses and the
//! doors the freeze deliberately leaves open, the document ceilings,
//! and the operational gotchas.

pub mod handshake;

/// Generated protobuf/gRPC types for `rdlt.connector.v1`, compiled at
/// build time from `proto/rdlt_connector_v1.proto` (build.rs vendors its
/// own protoc, so no system install is needed). The include splices the
/// generated source in as this module's body — the file name follows the
/// proto `package`. The `*_json` document ceilings and the cursor
/// contract are spelled out in the crate's `README.md`, beside the SPI
/// constants that define them.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/rdlt.connector.v1.rs"));
}

/// The per-message receive ceiling BOTH sides of the wire install in
/// place of tonic's 4 MiB default: one legal Arrow batch can exceed
/// 4 MiB and the one-batch-per-frame rule forbids delivering it
/// smaller. h2 flow-control windows remain the pacing mechanism; this
/// is the hard refusal ceiling.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// The protocol version this crate's generated code implements — the
/// identifier a connector advertises and a provider negotiates over
/// [`proto::HandshakeRequest::protocol_version`]. Distinct from the
/// handshake line's own format version, which is pinned separately at
/// `1` (see [`handshake::Line`]).
///
/// Version 1 retired two decode-amplifying field shapes for ceilinged
/// documents (the streams reply's repeated bytes → one newline-joined
/// blob; the handshake's state-format map → one JSON object) — the
/// bump makes a skewed old binary refuse LOUDLY at the handshake's
/// version check instead of mis-reading the reshaped fields.
pub const PROTOCOL_VERSION: u32 = 1;
