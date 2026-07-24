//! # rdlt-connector-file — the file family (source + destination)
//!
//! Reads and writes JSONL, CSV, and Parquet files — by explicit path or glob — on the
//! local filesystem or S3-compatible object storage. Sources track per-file incremental
//! cursors (completed files skipped, in-progress files resumed at their offset,
//! shrunk/rewritten files rejected loudly); Parquet streams are STRUCTURED (Arrow
//! batches with run-level provenance only). Layout: `source/` and `dest/` sides over a
//! shared `location/` (where files live) and `formats/` (how they are encoded); this
//! façade re-exports the public surface.

pub mod dest;
pub mod formats;
pub mod location;
pub mod source;

pub use dest::ParquetDir;
pub use formats::Format;
pub use source::{FileConfig, FileSource, FileStream, config, config_schema, cursor};
