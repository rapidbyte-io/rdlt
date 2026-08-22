//! # rdlt-connector — the in-process connector SPI
//!
//! The PROTOCOL layer of the connector SDK: [`source::Source`],
//! [`destination::Destination`]/[`destination::LoadSession`] and their
//! adjuncts, and nothing that moves fast. The behavioral contract the
//! traits obey — sources resume from a committed cursor without
//! re-emitting rows, destinations stage writes invisibly and publish
//! them atomically with pipeline state, delivery is at-least-once and
//! idempotent per commit — is enforced by the public conformance suites
//! in `rdlt-testkit` ("certified = passes conformance"). A connector can
//! be written against this crate alone.
//!
//! Vocabulary types come from `rdlt_core`, re-exported as [`core`] —
//! connector code takes them from HERE, never from `rdlt_core` directly,
//! so a connector crate has exactly one foundation dependency and
//! single-version identity across a workspace.
//!
//! Traits are object-safe; the declaration and state vocabulary is
//! serde-serializable and record payloads carry wire forms (NDJSON bytes,
//! Arrow IPC), so a future process/WASM host can adapt this SPI over a
//! wire without engine changes.
//!
//! The whole protocol fits in one example — declare, probe, read, and let
//! the channel's byte budget carry the backpressure:
//!
//! ```
//! use rdlt_connector::channel::{self, PushPayload};
//! use rdlt_connector::error::SourceError;
//! use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
//! use rdlt_connector::spec::ConnectorSpec;
//!
//! struct Countdown;
//!
//! #[async_trait::async_trait]
//! impl Source for Countdown {
//!     fn spec(&self) -> ConnectorSpec {
//!         ConnectorSpec::new("countdown", "1.0.0")
//!     }
//!     async fn check(&self) -> Result<(), SourceError> {
//!         // Nothing external to reach: holding the data IS reachability.
//!         Ok(())
//!     }
//!     async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
//!         Ok(vec![StreamSpec::new("ticks")])
//!     }
//!     async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
//!         let rows = (0..3).rev().map(|n| serde_json::json!({"tick": n}));
//!         // A closed channel is cancellation, not an error.
//!         let _ = request.out.rows(rows).await;
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let source = Countdown;
//! source.check().await.expect("the probe succeeds");
//!
//! let (out, mut input) = channel::records(1 << 20);
//! let streams = source.streams().await.expect("declared");
//! source
//!     .read(ReadRequest::new(streams[0].clone(), None, out))
//!     .await
//!     .expect("read");
//! let push = input.recv().await.expect("one NDJSON push");
//! match push.payload {
//!     PushPayload::RawJson(bytes) => {
//!         assert_eq!(&bytes[..], b"{\"tick\":2}\n{\"tick\":1}\n{\"tick\":0}\n");
//!     }
//!     other => panic!("rows land as RawJson, got {other:?}"),
//! }
//! # }
//! ```
//!
//! SEMVER-SACRED: gated by `cargo semver-checks` in the gate.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

pub mod channel;
pub mod destination;
pub mod error;
pub mod gate;
pub mod secret;
pub mod source;
pub mod spec;

/// The rdlt vocabulary. Connectors MUST take these types from here — the
/// single-foundation rule IS this line: one dependency, single-version
/// identity across a workspace.
pub use rdlt_core as core;

/// The Arrow array vocabulary record batches are built from, re-exported
/// for the same single-version reason (`arrow::RecordBatch` is the
/// canonical spelling).
pub use arrow_array as arrow;
/// The Arrow schema vocabulary those batches describe themselves with
/// (`Schema`, `Field`, `DataType`), re-exported for the same reason.
pub use arrow_schema;
