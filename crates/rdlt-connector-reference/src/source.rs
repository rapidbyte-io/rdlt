//! The reference source: ONE jsonl file, resumed by byte offset.
//!
//! The stream is named by the file's stem; the persisted cursor (v1
//! wire keys `v`/`bytes_read`) is the count of consumed bytes, so a
//! re-run over an unchanged file reads zero rows, a grown file yields
//! only its tail, and a file that shrank below the cursor refuses
//! typed rather than guessing what the missing bytes were.

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
    Yaml(#[from] serde_yaml::Error),
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

/// The persisted cursor, v1: `{"v":1,"bytes_read":<u64>}`. The wire
/// keys are frozen; a document with any other shape (or a future `v`)
/// is refused typed rather than read as zero.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorV1 {
    v: u32,
    bytes_read: u64,
}

fn cursor_at(bytes_read: u64) -> Cursor {
    Cursor::new(serde_json::json!({"v": 1, "bytes_read": bytes_read}))
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
        let len = bytes.len() as u64;
        let start = match &since {
            None => 0,
            Some(cursor) => self.resume_offset(cursor, len)?,
        };

        let mut offset = start as usize;
        let mut checkpointed = false;
        while offset < bytes.len() {
            let rest = &bytes[offset..];
            let (line, next) = match rest.iter().position(|byte| *byte == b'\n') {
                Some(newline) => (&rest[..newline], offset + newline + 1),
                // A final line without its newline is still a complete
                // row; the cursor lands at EOF so an append that starts
                // mid-line would surface as invalid JSON, never as a
                // silently-glued row.
                None => (rest, bytes.len()),
            };
            if !line.iter().all(u8::is_ascii_whitespace) {
                let row: serde_json::Value = serde_json::from_slice(line).map_err(|error| {
                    SourceError::fatal(format!(
                        "reference source: {}: invalid JSON on the line at byte {offset}: {error}",
                        self.path
                    ))
                })?;
                if feed.rows([row]).await.is_break() {
                    return Ok(());
                }
            }
            offset = next;
            // Checkpoint per consumed line: rows-so-far are complete up
            // to this byte offset, which is what makes every prefix of
            // the file a legal resume point.
            if feed.checkpoint(cursor_at(offset as u64)).await.is_break() {
                return Ok(());
            }
            checkpointed = true;
        }
        // A read that consumed nothing (an empty file, or a resume that
        // was already at EOF) still declares where it stands, so the
        // stream always certifies for resume and an unchanged re-run
        // commits the same cursor it started from.
        if !checkpointed && feed.checkpoint(cursor_at(len)).await.is_break() {
            return Ok(());
        }
        Ok(())
    }
}

impl Reference {
    /// Decode a resume cursor against the file's current length.
    fn resume_offset(&self, cursor: &Cursor, len: u64) -> Result<u64, SourceError> {
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
        Ok(v1.bytes_read)
    }
}

/// One classification rule: a missing file is a configuration pointing
/// at nothing (fatal); any other IO failure may pass (transient).
fn classify_io(path: &str, error: std::io::Error) -> SourceError {
    let message = format!("reference source: {path}: {error}");
    if error.kind() == std::io::ErrorKind::NotFound {
        SourceError::fatal(message)
    } else {
        SourceError::transient(message)
    }
}

/// The canonical face: `Shell::from_yaml(text)?` / `Shell::new(config)?`
/// is a running SPI source in one call.
pub type Shell = rdlt_connector_sdk::source::Shell<Reference>;
