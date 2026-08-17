//! Shared plumbing for the reference connector's suites: the
//! directory-counting probe both kits read visibility through, and a
//! small full-read harness for the exactly-once pins.

use std::path::PathBuf;

use rdlt_connector_sdk::spi::channel::{PushPayload, records};
use rdlt_connector_sdk::spi::core::cursor::Cursor;
use rdlt_connector_sdk::spi::core::id::TableName;
use rdlt_connector_sdk::spi::error::SourceError;
use rdlt_connector_sdk::spi::source::{ReadRequest, Source, StreamSpec};
use rdlt_testkit::conformance::destination::{ProbeError, TableProbe};

/// Counts reader-VISIBLE rows for a table: the lines of every published
/// `<table>-…` jsonl part in the output directory. Bookkeeping files
/// (`_reference_receipts.json`, `_reference_state.json`) and staged
/// temporaries are underscore-prefixed, so a table prefix never matches
/// them — staged rows are invisible to this probe by construction.
pub struct DirProbe(pub PathBuf);

#[async_trait::async_trait]
impl TableProbe for DirProbe {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        // The destination creates its directory at the first connect, so
        // an absent directory means nothing was ever published — that
        // zero is a fact, not a store the probe failed to read.
        let entries = match std::fs::read_dir(&self.0) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => {
                return Err(ProbeError {
                    message: format!("read_dir {}: {error}", self.0.display()),
                });
            }
        };
        let prefix = format!("{table}-");
        let mut rows = 0u64;
        for entry in entries {
            let entry = entry.map_err(|error| ProbeError {
                message: format!("read_dir entry: {error}"),
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix) && name.ends_with(".jsonl") {
                let text = std::fs::read_to_string(entry.path()).map_err(|error| ProbeError {
                    message: format!("read part {name}: {error}"),
                })?;
                rows += text.lines().filter(|line| !line.trim().is_empty()).count() as u64;
            }
        }
        Ok(rows)
    }
}

/// One complete read of `spec` through a fresh channel: the pushed rows
/// (parsed back from their raw-json wire form) and the LAST checkpoint
/// the read emitted. The channel budget dwarfs every fixture here, so
/// the read runs to completion before the drain — no select loop.
pub async fn read_stream<S: Source>(
    source: &S,
    spec: &StreamSpec,
    since: Option<Cursor>,
) -> Result<(Vec<serde_json::Value>, Option<Cursor>), SourceError> {
    let (out, mut input) = records(1 << 20);
    source
        .read(ReadRequest::new(spec.clone(), since, out))
        .await?;
    let mut rows = Vec::new();
    let mut last_checkpoint = None;
    while let Some(push) = input.recv().await {
        match push.payload {
            PushPayload::RawJson(bytes) => {
                for doc in serde_json::Deserializer::from_slice(&bytes).into_iter() {
                    rows.push(doc.expect("the source pushes valid JSON"));
                }
            }
            PushPayload::Arrow(batch) => {
                panic!("the reference source pushes json, never Arrow: {batch:?}")
            }
            PushPayload::Checkpoint(cursor) => last_checkpoint = Some(cursor),
        }
    }
    Ok((rows, last_checkpoint))
}
