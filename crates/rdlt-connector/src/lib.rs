//! # rdlt-connector — the in-process connector SPI
//!
//! The PROTOCOL layer of the connector SDK: [`Source`]/[`Destination`]/
//! [`LoadSession`] and their adjuncts, and nothing that moves fast. The
//! behavioral contract the traits obey — sources resume from a committed
//! cursor without re-emitting rows, destinations stage writes invisibly
//! and publish them atomically with pipeline state, delivery is
//! at-least-once and idempotent per commit — is enforced by the public
//! conformance suites in `rdlt-testkit` ("certified = passes
//! conformance"). A connector can be written against this crate alone.
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
//! use rdlt_connector::{
//!     ConnectorSpec, PushPayload, ReadRequest, Source, SourceError, StreamSpec,
//!     records_channel,
//! };
//!
//! struct Countdown;
//!
//! #[async_trait::async_trait]
//! impl Source for Countdown {
//!     fn spec(&self) -> ConnectorSpec {
//!         ConnectorSpec::new("countdown", "1.0.0")
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
//! source.check().await.expect("the default probe succeeds");
//!
//! let (out, mut input) = records_channel(1 << 20);
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

pub mod capabilities;
pub mod channel;
pub mod destination;
pub mod error;
pub mod ipc;
pub mod parquet;
pub mod pem;
pub mod secret;
pub mod source;
pub mod spec;
#[cfg(feature = "object-store")]
pub mod store;
pub mod stream;

pub use arrow_array as arrow;
pub use arrow_array::RecordBatch;
pub use capabilities::DestinationCapabilities;
pub use channel::{
    ChannelClosed, PushPayload, RecordsIn, RecordsOut, SharedBudget, SourcePush, records_channel,
    records_channel_shared,
};
pub use destination::{
    Destination, LoadSession, OpenContext, PartCloseReason, PartClosed, PartEventFn,
};
pub use error::{BoxError, DestinationError, SourceError};
/// How a destination writes parquet — plain data, no parquet dependency
/// in the SPI. Connectors re-export these from their own config paths and
/// translate them into `WriterProperties` at their own boundary.
pub use parquet::{ParquetCompression, ParquetOptions, PartOptions, PartsError};
pub use pem::PemSource;
/// The rdlt vocabulary. Connectors MUST take these types from here
/// (single-version identity across the workspace).
pub use rdlt_core as core;
pub use rdlt_core::{
    CommitMeta, CommitReceipt, Cursor, LoadId, PipelineId, StateDoc, TableName, TableSchema,
    WriteMode,
};
/// The shared credential newtype: serde-transparent, renders as `***`,
/// [`Secret::reveal`] the sole (grep-able) accessor. Connectors re-export
/// it from their own config paths.
pub use secret::Secret;
pub use source::{ReadRequest, Source};
pub use spec::ConnectorSpec;
pub use stream::StreamSpec;

/// The most a JSON configuration/state DOCUMENT may weigh, in bytes
/// (047 wave 5, 4L10/4I10): connector config documents and persisted
/// cursors are hand-written or summarized state measured in kilobytes,
/// and an untyped `serde_json::Value` materializes at many times its
/// wire size, so every seat that parses or forwards one enforces this
/// ONE ceiling — the sdk's config parse gate, the out-of-process
/// `serve` document refusals (`config_json`, `since_cursor_json`), the
/// client's pre-send check, and the host's file read all import this
/// spelling so the seats cannot drift apart. The cursor contract for
/// connector authors: persisted state MUST serialize under this
/// ceiling — a connector whose state can outgrow it summarizes (a
/// high-water mark, an offset, a resume token) rather than embedding
/// the data.
pub const MAX_DOCUMENT_BYTES: u64 = 8 * 1024 * 1024;

/// The most a persisted CURSOR may weigh, in bytes — the effective
/// cursor contract (5M1/5L3): TIGHTER than [`MAX_DOCUMENT_BYTES`],
/// because a cursor is not only parsed and sent but recorded in the
/// engine's WAL, whose per-line metadata cap is sized to carry one
/// maximal cursor line. A cursor over this bound is refused typed at
/// every seat — the client's inbound frame decode and its pre-send
/// check, the serve gate's `since_cursor_json` refusal, and the WAL's
/// write-time line cap — so an over-scale cursor fails LOUDLY at first
/// contact (the connector must summarize: a high-water mark, an offset,
/// a resume token) rather than crash-looping a resume against a line
/// its own recovery could never scan back.
pub const MAX_CURSOR_BYTES: u64 = 4 * 1024 * 1024;
