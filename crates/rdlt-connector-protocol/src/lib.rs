//! # rdlt-connector-protocol — the out-of-process connector wire protocol (v0)
//!
//! FROZEN as of 2026-08-07. Frozen means the rules of change bind:
//! field numbers are never renumbered, repurposed, or recycled — a
//! retired number is `reserved`; evolution is ADDITIVE ONLY (new fields
//! take fresh numbers; new messages, RPCs, `oneof` arms and enum values
//! may be added; nothing is removed, narrowed, or given a second
//! meaning); and a receiver tolerates what a newer peer sends without
//! knowing it — an unrecognized [`proto::Classification`] normalizes
//! safe-loud to `Fatal` rather than being guessed retryable.
//!
//! The negotiated version NUMBER stays `0` ([`PROTOCOL_VERSION`]): it
//! is the identifier both sides compare at the handshake, and bumping
//! it for a freeze that moves no byte would break every shipped
//! handshake for nothing — "v1" is the name of the frozen contract, not
//! a value on the wire.
//!
//! Two halves: [`handshake`] is the plaintext stdout line a spawned
//! connector prints before it starts serving; [`proto`] is the
//! generated gRPC/protobuf types and services. Beside them,
//! [`inventory`] carries the wire's shared control-and-invisible
//! codepoint table — the one table the handshake's socket-path gate and
//! the client's identifier and display seats all refuse and escape by.
//!
//! Trust model: config documents — which may carry credentials — cross
//! the Unix domain socket in the clear; v0's boundary is the owner-only
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
pub mod inventory;

/// Generated protobuf/gRPC types for `rdlt.connector.v0`, compiled at
/// build time from `proto/rdlt_connector_v0.proto` (build.rs vendors its
/// own protoc, so no system install is needed). The include splices the
/// generated source in as this module's body — the file name follows the
/// proto `package`. The `*_json` document ceilings and the cursor
/// contract are spelled out in the crate's `README.md`, beside the SPI
/// constants that define them.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/rdlt.connector.v0.rs"));
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
pub const PROTOCOL_VERSION: u32 = 0;
