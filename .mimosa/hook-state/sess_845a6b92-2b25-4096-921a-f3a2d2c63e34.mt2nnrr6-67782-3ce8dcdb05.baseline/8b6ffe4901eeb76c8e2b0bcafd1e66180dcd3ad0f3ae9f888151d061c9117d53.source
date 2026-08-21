//! # rdlt-connector-client
//!
//! The wire client an adapter drives a served connector through — the
//! out-of-process counterpart to the sdk's serve half.
//!
//! Three concerns, two modules each. [`wire`] owns the transport: it
//! dials the Unix domain socket a spawned connector advertised and
//! bounds every await, while the private `gate` module holds the
//! wire-edge defenses its siblings share. [`handshake`] verifies the
//! connector is the one the provider resolved and decodes its
//! self-description; [`error`] maps wire error frames back to the
//! SPI's own classifications so the engine's retry machinery never
//! learns the wire exists. [`source::Remote`] and
//! [`destination::Remote`] are those seams composed into the SPI's two
//! halves — the destination boxing the sdk's own `Session` over a
//! [`destination::Backend`], so the exactly-once commit choreography
//! runs client-side by identical type.
//!
//! The deadline law: every wire await — the dial, the handshake, each
//! read frame's quiet interval, each reply — is bounded by the
//! requirement's RPC deadline, so a dead OR silent connector yields a
//! typed [`error::Error::Timeout`] within it, never a hang.
//!
//! The trust posture: the connector process is untrusted, so
//! everything it sends crosses a gate before this crate acts on it —
//! size ceilings ahead of any parse, control characters spelled as
//! inert escapes, identifier rules on reported names, the wait-hint
//! clamp.
//!
//! Two refusal spellings are frozen verbatim: `read frame violated the
//! one-batch rule` and `the connector session ended before replying`.
//!
//! The wire's rules of change bind whether or not it is frozen (it was
//! frozen 2026-08-07 and re-opened 2026-08-19 while the workspace is
//! pre-release — the protocol crate's doc carries the dates): field
//! numbers never move, evolution is additive, and an unknown value
//! from a newer peer normalizes
//! safe-loud — an unrecognized classification arrives as
//! [`error::Classification::Fatal`], never a guess.

mod gate;

pub mod destination;
pub mod error;
#[doc(hidden)]
pub mod fuzzing;
pub mod handshake;
pub mod source;
pub mod wire;
