//! The sdk `SourceConnector`: one file, one stream. Only
//! newline-TERMINATED lines are ever consumed — a newline-less tail is
//! a row still being written, left for the read that sees its newline.

use async_trait::async_trait;
use rdlt_connector_sdk::config::schema_of;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::core::cursor::Cursor;
use rdlt_connector_sdk::spi::error::SourceError;
use rdlt_connector_sdk::spi::source::StreamSpec;

use super::{config, cursor};

/// Rows per pushed batch, which is also the checkpoint cadence: the
/// cursor is exact at every checkpoint (each one lands on a consumed
/// newline), but a checkpoint frame per LINE doubled wire traffic for
/// no extra resume precision a host ever used.
const ROWS_PER_BATCH: usize = 1024;

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
    type Config = config::Config;

    fn assemble(config: config::Config) -> Result<Self, config::Error> {
        // `validate` already refused a stem-less path on every shell
        // entry; re-deriving here keeps `assemble` total for a caller
        // constructing around the gate.
        let stream = config::stem_of(&config.path).ok_or_else(|| {
            config::Error::Invalid(format!(
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
        Some(schema_of::<config::Config>())
    }

    async fn check(&self) -> Result<(), SourceError> {
        let meta = tokio::fs::metadata(&self.path)
            .await
            .map_err(|error| classify_io(&self.path, error))?;
        // Existing is not enough: a directory (or any non-regular file)
        // at the path fails every read fatally, so a probe that passed
        // it would be optimism about a misconfiguration no retry fixes.
        if !meta.is_file() {
            return Err(SourceError::fatal(format!(
                "reference source: {}: not a regular file — `path` must name a jsonl file",
                self.path
            )));
        }
        Ok(())
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        // No `cursor_field`: the cursor is positional (a byte offset),
        // not a record field. Resume is still real: every read
        // checkpoints.
        Ok(vec![StreamSpec::new(self.stream.as_str())])
    }

    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        if stream.name.as_str() != self.stream {
            // The requested name is wire-authored for a served source —
            // quoted through the bounded diagnostic render, never raw.
            return Err(SourceError::fatal(format!(
                "reference source: unknown stream `{}` — this source serves only `{}`, \
                 the file stem of `{}`",
                rdlt_connector_sdk::spi::gate::render_diagnostic(stream.name.as_str(), 256),
                self.stream,
                self.path
            )));
        }
        let bytes = tokio::fs::read(&self.path)
            .await
            .map_err(|error| classify_io(&self.path, error))?;
        let start = match &since {
            None => 0,
            Some(cursor) => cursor::resume(&self.path, cursor, &bytes)?,
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
                if feed.checkpoint(cursor::at(&bytes, offset)).await.is_break() {
                    return Ok(());
                }
                checkpointed = true;
            }
        }
        if !batch.is_empty() {
            if feed.rows(std::mem::take(&mut batch)).await.is_break() {
                return Ok(());
            }
            if feed.checkpoint(cursor::at(&bytes, offset)).await.is_break() {
                return Ok(());
            }
            checkpointed = true;
        }
        // A read that consumed nothing (an empty file, a resume already
        // at its last newline, or only blank lines) still declares where
        // it stands, so the stream always certifies for resume and an
        // unchanged re-run commits the same cursor it started from.
        if !checkpointed && feed.checkpoint(cursor::at(&bytes, offset)).await.is_break() {
            return Ok(());
        }
        Ok(())
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
