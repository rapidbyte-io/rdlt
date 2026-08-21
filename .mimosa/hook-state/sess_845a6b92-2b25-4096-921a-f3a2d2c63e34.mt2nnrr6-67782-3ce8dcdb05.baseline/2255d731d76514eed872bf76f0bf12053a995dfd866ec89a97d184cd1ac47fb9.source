//! The reference source: ONE jsonl file, resumed by byte offset.
//!
//! The stream is named by the file's stem; the persisted cursor (v1
//! wire keys `v`/`bytes_read`/`tail_hash`) is the count of consumed
//! bytes plus a hash of the bytes just before it, so a re-run over an
//! unchanged file reads zero rows, a grown file yields only its tail,
//! and a file that shrank — or was rewritten in place — refuses typed
//! rather than emitting unrelated bytes as if they were appended rows.
//! Only newline-TERMINATED lines are ever consumed: a newline-less
//! tail is a row still being written, left for the read that sees its
//! newline.

use async_trait::async_trait;
use rdlt_connector_sdk::config::{self, Document};
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::core::Cursor;
use rdlt_connector_sdk::spi::{SourceError, StreamSpec};

/// The reference source document: ONE jsonl file, nothing else.
/// `{ "path": "/abs/or/rel/file.jsonl" }`
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The jsonl file to read. Its stem names the one stream.
    pub path: String,
}

/// The source's configuration error — parser framings plus the config
/// gate's own refusals, every spelling owned here.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// YAML did not parse as the config document.
    #[error("invalid reference source YAML: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    /// JSON did not parse as the config document.
    #[error("invalid reference source JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The document parsed but violates an invariant.
    #[error("invalid reference source config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = Error;

    fn validate(&self) -> Result<(), Error> {
        if self.path.is_empty() {
            return Err(Error::Invalid(
                "`path` is empty — one jsonl file is required".into(),
            ));
        }
        if stem_of(&self.path).is_none() {
            return Err(Error::Invalid(format!(
                "`{}` has no file stem to name the stream",
                self.path
            )));
        }
        Ok(())
    }
}

/// The stream name a path yields: its UTF-8 file stem, when it has one.
fn stem_of(path: &str) -> Option<String> {
    let stem = std::path::Path::new(path).file_stem()?.to_str()?;
    (!stem.is_empty()).then(|| stem.to_owned())
}

/// How far back the cursor's rewrite guard reaches: the hash covers the
/// last `min(bytes_read, TAIL_WINDOW)` consumed bytes. A rewrite that
/// preserves that window byte-for-byte legitimately resumes — the guard
/// answers "is this still the file I read", not "is every byte before
/// the cursor identical".
const TAIL_WINDOW: u64 = 4096;

/// Rows per pushed batch, which is also the checkpoint cadence: the
/// cursor is exact at every checkpoint (each one lands on a consumed
/// newline), but a checkpoint frame per LINE doubled wire traffic for
/// no extra resume precision a host ever used.
const ROWS_PER_BATCH: usize = 1024;

/// The persisted cursor, v1: `{"v":1,"bytes_read":<u64>,"tail_hash":
/// <hex>}`. The wire keys are frozen; a document with any other shape
/// (or a future `v`) is refused typed rather than read as zero.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorV1 {
    v: u32,
    bytes_read: u64,
    tail_hash: String,
}

/// The hash the cursor carries: the tail window of everything consumed,
/// hex-encoded.
fn tail_hash(bytes: &[u8], bytes_read: usize) -> String {
    let window = bytes_read.min(TAIL_WINDOW as usize);
    blake3::hash(&bytes[bytes_read - window..bytes_read])
        .to_hex()
        .to_string()
}

fn cursor_at(bytes: &[u8], bytes_read: usize) -> Cursor {
    Cursor::new(serde_json::json!({
        "v": 1,
        "bytes_read": bytes_read as u64,
        "tail_hash": tail_hash(bytes, bytes_read),
    }))
}

/// The connector: one file, one stream.
#[derive(Debug)]
pub struct Reference {
    path: String,
    stream: String,
}

#[async_trait]
impl SourceConnector for Reference {
    const NAME: &'static str = "io.rapidbyte.reference";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = Config;

    fn assemble(config: Config) -> Result<Self, Error> {
        // `validate` already refused a stem-less path on every Shell
        // entry; re-deriving here keeps `assemble` total for a caller
        // constructing around the gate.
        let stream = stem_of(&config.path).ok_or_else(|| {
            Error::Invalid(format!(
                "`{}` has no file stem to name the stream",
                config.path
            ))
        })?;
        Ok(Self {
            path: config.path,
            stream,
        })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::schema_of::<Config>())
    }

    async fn check(&self) -> Result<(), SourceError> {
        tokio::fs::metadata(&self.path)
            .await
            .map_err(|error| classify_io(&self.path, error))?;
        Ok(())
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        // No `cursor_field`: the cursor is positional (a byte offset),
        // not a record field — the same posture as the file source's
        // jsonl streams. Resume is still real: every read checkpoints.
        Ok(vec![StreamSpec::new(self.stream.as_str())])
    }

    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        if stream.name.as_str() != self.stream {
            return Err(SourceError::fatal(format!(
                "reference source: unknown stream `{}` — this source serves only `{}`, \
                 the file stem of `{}`",
                stream.name, self.stream, self.path
            )));
        }
        let bytes = tokio::fs::read(&self.path)
            .await
            .map_err(|error| classify_io(&self.path, error))?;
        let start = match &since {
            None => 0,
            Some(cursor) => self.resume_offset(cursor, &bytes)?,
        };

        let mut offset = start;
        let mut batch = Vec::new();
        let mut checkpointed = false;
        // Only newline-TERMINATED lines are consumed. A final line
        // without its newline is a row a writer may still be appending:
        // emitting it would commit a cursor at EOF mid-line, and the
        // resumed read would then split the finished row in two (or die
        // on its tail as invalid JSON). The cursor stays at the last
        // newline, and the read that sees the line completed picks it
        // up whole.
        while let Some(newline) = bytes[offset..].iter().position(|byte| *byte == b'\n') {
            let line = &bytes[offset..offset + newline];
            if !line.iter().all(u8::is_ascii_whitespace) {
                let row: serde_json::Value = serde_json::from_slice(line).map_err(|error| {
                    SourceError::fatal(format!(
                        "reference source: {}: invalid JSON on the line at byte {offset}: {error}",
                        self.path
                    ))
                })?;
                batch.push(row);
            }
            offset += newline + 1;
            // Checkpoint at batch boundaries, not per line: rows-so-far
            // are complete up to this byte offset — every checkpoint is
            // still a legal resume point — at a fraction of the wire
            // frames a per-line cadence cost.
            if batch.len() >= ROWS_PER_BATCH {
                if feed.rows(std::mem::take(&mut batch)).await.is_break() {
                    return Ok(());
                }
                if feed.checkpoint(cursor_at(&bytes, offset)).await.is_break() {
                    return Ok(());
                }
                checkpointed = true;
            }
        }
        if !batch.is_empty() {
            if feed.rows(std::mem::take(&mut batch)).await.is_break() {
                return Ok(());
            }
            if feed.checkpoint(cursor_at(&bytes, offset)).await.is_break() {
                return Ok(());
            }
            checkpointed = true;
        }
        // A read that consumed nothing (an empty file, a resume already
        // at its last newline, or only blank lines) still declares where
        // it stands, so the stream always certifies for resume and an
        // unchanged re-run commits the same cursor it started from.
        if !checkpointed && feed.checkpoint(cursor_at(&bytes, offset)).await.is_break() {
            return Ok(());
        }
        Ok(())
    }
}

impl Reference {
    /// Decode a resume cursor against the file's current bytes: refuse
    /// typed when the file shrank below the cursor OR when the bytes
    /// just before it no longer hash to what the cursor recorded — a
    /// rewrite-in-place, where a bare offset would silently emit the
    /// tail of unrelated new content as appended rows.
    fn resume_offset(&self, cursor: &Cursor, bytes: &[u8]) -> Result<usize, SourceError> {
        let len = bytes.len() as u64;
        let v1: CursorV1 = serde_json::from_value(cursor.as_value().clone()).map_err(|error| {
            SourceError::fatal(format!(
                "reference source: {}: unrecognized resume cursor {}: {error}",
                self.path,
                cursor.as_value()
            ))
        })?;
        if v1.v != 1 {
            return Err(SourceError::fatal(format!(
                "reference source: {}: cursor version {} is newer than this build reads (v1)",
                self.path, v1.v
            )));
        }
        if v1.bytes_read > len {
            return Err(SourceError::fatal(format!(
                "reference source: {} shrank below the cursor ({} > {len}): refusing to guess",
                self.path, v1.bytes_read
            )));
        }
        let bytes_read = v1.bytes_read as usize;
        if tail_hash(bytes, bytes_read) != v1.tail_hash {
            let window = bytes_read.min(TAIL_WINDOW as usize);
            return Err(SourceError::fatal(format!(
                "reference source: {}: the {window} bytes before the cursor no longer match \
                 its tail hash — the file was rewritten in place, refusing to resume",
                self.path
            )));
        }
        Ok(bytes_read)
    }
}

/// The classification rule: a path naming nothing, an unreadable path,
/// and a path naming a directory are all configurations that can never
/// pass (fatal); any other IO failure may (transient).
fn classify_io(path: &str, error: std::io::Error) -> SourceError {
    use std::io::ErrorKind;
    let message = format!("reference source: {path}: {error}");
    match error.kind() {
        ErrorKind::NotFound | ErrorKind::PermissionDenied | ErrorKind::IsADirectory => {
            SourceError::fatal(message)
        }
        _ => SourceError::transient(message),
    }
}

/// The canonical face: `Shell::from_yaml(text)?` / `Shell::new(config)?`
/// is a running SPI source in one call.
pub type Shell = rdlt_connector_sdk::source::Shell<Reference>;

#[cfg(test)]
mod tests {
    use super::classify_io;
    use std::io::{Error, ErrorKind};

    /// Every fatal arm is a misconfiguration retries cannot fix; the
    /// class is read off the rendered prefix because the classification
    /// IS the observable (`fatal source error: ` / `transient source
    /// error: `).
    #[test]
    fn misconfiguration_kinds_classify_fatal() {
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::PermissionDenied,
            ErrorKind::IsADirectory,
        ] {
            let classified = classify_io("f.jsonl", Error::from(kind));
            assert!(
                classified.to_string().starts_with("fatal source error: "),
                "{kind:?} must classify fatal, got: {classified}"
            );
        }
    }

    /// Kinds that genuinely may pass on retry stay transient.
    #[test]
    fn transient_kinds_stay_transient() {
        for kind in [
            ErrorKind::Interrupted,
            ErrorKind::TimedOut,
            ErrorKind::WouldBlock,
        ] {
            let classified = classify_io("f.jsonl", Error::from(kind));
            assert!(
                classified
                    .to_string()
                    .starts_with("transient source error: "),
                "{kind:?} must classify transient, got: {classified}"
            );
        }
    }
}
