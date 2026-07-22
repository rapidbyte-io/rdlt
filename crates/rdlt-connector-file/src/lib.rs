//! # rdlt-connector-file — the file family (source; dest joins in 015)
//!
//! JSONL and Parquet files by explicit path or glob, with per-file incremental
//! cursors (completed files skipped, in-progress files resumed at their offset,
//! shrunk/rewritten files rejected loudly). Parquet streams are STRUCTURED
//! (contract clause S7). Family layout: `source/` + shared `formats/`; this
//! façade re-exports the public surface (015 FF1 — spellings and paths frozen).

pub mod formats;
pub mod source;

pub use formats::Format;
pub use source::{FileConfig, FileSource, FileStream, config, config_schema, cursor};
