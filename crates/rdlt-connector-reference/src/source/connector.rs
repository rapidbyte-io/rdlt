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

/// The per-line ceiling: one jsonl row is one document, and documents
/// ride the family's 8 MiB bound everywhere in this workspace. A longer
/// line refuses typed at the offset where it began. This bounds each
/// LINE; the INPUT bytes a batch accumulates are bounded by the byte
/// flush ([`MAX_BATCH_BYTES`]) plus at most one such line.
const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;

/// The per-batch byte flush bound, beside the row bound: parsed rows
/// are RETAINED until the batch flushes, and every row can ride a line
/// up to [`MAX_LINE_BYTES`], so the row count alone would retain up to
/// 1024 × 8 MiB of a perfectly legal file. Whichever bound crosses
/// first flushes.
///
/// What this bounds EXACTLY: the INPUT bytes one batch consumes — the
/// summed lengths of its lines, at most this budget plus the one line
/// that crossed it. What the batch then costs is a multiple of that:
/// parsed values carry their own per-value overhead, and the frame the
/// feed encodes is re-serialized (a compact number can render longer
/// than it arrived). Those multiples are bounded because this is, which
/// is the point; the figure to quote is the input bound, not a memory
/// total.
///
/// 8 MiB is chosen to sit AT the budget of the read channel a served
/// source pushes through, so an honest batch is one the channel admits
/// without leaning on its at-least-one-frame exception.
const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;

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
        self.gate_regular_file().await?;
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
        // The shape gate runs BEFORE any open: a FIFO parks the opener
        // until a writer appears, and a char device reads without end —
        // neither can be judged after the fact. The file is then
        // consumed LINE-STREAMING: each line rides the per-line
        // ceiling and the batch flushes on BYTES as well as rows, so
        // the input a batch accumulates is one budget plus one line
        // whatever the file's size or line sizes — an honest multi-GiB
        // jsonl streams where a whole-file read would have died on it.
        let len = self.gate_regular_file().await?;
        let read_io = |error: std::io::Error| classify_io(&self.path, error);
        let mut file = tokio::fs::File::open(&self.path).await.map_err(read_io)?;
        let (start, mut tail) = match &since {
            None => (0, cursor::Tail::start()),
            Some(resume_from) => cursor::resume(&self.path, resume_from, &mut file, len).await?,
        };

        use tokio::io::AsyncBufReadExt as _;
        let mut reader = tokio::io::BufReader::new(file);
        let mut offset = start;
        let mut line: Vec<u8> = Vec::new();
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        let mut checkpointed = false;
        // Only newline-TERMINATED lines are consumed. A final line
        // without its newline is a row a writer may still be appending:
        // emitting it would commit a cursor at EOF mid-line, and the
        // resumed read would then split the finished row in two (or die
        // on its tail as invalid JSON). The cursor stays at the last
        // newline, and the read that sees the line completed picks it
        // up whole.
        loop {
            let chunk = reader.fill_buf().await.map_err(read_io)?;
            if chunk.is_empty() {
                // EOF: whatever accumulated is the newline-less tail,
                // left for the read that sees it complete.
                break;
            }
            let Some(newline) = chunk.iter().position(|byte| *byte == b'\n') else {
                line.extend_from_slice(chunk);
                let taken = chunk.len();
                reader.consume(taken);
                if line.len() > MAX_LINE_BYTES {
                    return Err(SourceError::fatal(format!(
                        "reference source: {}: the line at byte {offset} is over the \
                         {MAX_LINE_BYTES}-byte line ceiling — one jsonl row rides the \
                         document family's 8 MiB bound",
                        self.path
                    )));
                }
                continue;
            };
            line.extend_from_slice(&chunk[..newline]);
            reader.consume(newline + 1);
            if line.len() > MAX_LINE_BYTES {
                return Err(SourceError::fatal(format!(
                    "reference source: {}: the line at byte {offset} is over the \
                     {MAX_LINE_BYTES}-byte line ceiling — one jsonl row rides the \
                     document family's 8 MiB bound",
                    self.path
                )));
            }
            if !line.iter().all(u8::is_ascii_whitespace) {
                let row: serde_json::Value = serde_json::from_slice(&line).map_err(|error| {
                    SourceError::fatal(format!(
                        "reference source: {}: invalid JSON on the line at byte {offset}: {error}",
                        self.path
                    ))
                })?;
                batch.push(row);
                batch_bytes += line.len();
            }
            // The rolling tail covers every consumed byte — the line
            // AND its newline — so a cursor minted at any boundary
            // hashes the same prefix the whole-file law always did.
            tail.push(&line);
            tail.push(b"\n");
            offset += line.len() as u64 + 1;
            line.clear();
            // Checkpoint at batch boundaries, not per line: rows-so-far
            // are complete up to this byte offset — every checkpoint is
            // still a legal resume point — at a fraction of the wire
            // frames a per-line cadence cost. The batch flushes on
            // WHICHEVER bound crosses first: the row count for ordinary
            // lines, the byte budget for large ones — a row bound alone
            // retained up to rows × the line ceiling.
            if batch.len() >= ROWS_PER_BATCH || batch_bytes >= MAX_BATCH_BYTES {
                batch_bytes = 0;
                if feed.rows(std::mem::take(&mut batch)).await.is_break() {
                    return Ok(());
                }
                if feed.checkpoint(cursor::at(&tail, offset)).await.is_break() {
                    return Ok(());
                }
                checkpointed = true;
            }
        }
        if !batch.is_empty() {
            if feed.rows(std::mem::take(&mut batch)).await.is_break() {
                return Ok(());
            }
            if feed.checkpoint(cursor::at(&tail, offset)).await.is_break() {
                return Ok(());
            }
            checkpointed = true;
        }
        // A read that consumed nothing (an empty file, a resume already
        // at its last newline, or only blank lines) still declares where
        // it stands, so the stream always certifies for resume and an
        // unchanged re-run commits the same cursor it started from.
        if !checkpointed && feed.checkpoint(cursor::at(&tail, offset)).await.is_break() {
            return Ok(());
        }
        Ok(())
    }
}

impl Reference {
    /// The one shape judgment `check` and the read both stand on — so
    /// they agree by construction: the configured path must name a
    /// regular file. A directory fails every read; a FIFO parks its
    /// opener until a writer appears; a char device reads without end.
    /// None can be judged after opening, so the metadata is the gate.
    /// The metadata-then-open window is the at-rest DIRECTORY writer's
    /// existing power (directory ownership is the trust boundary):
    /// replacing the file between the two calls needs write on the
    /// containing directory, and it grants what writing the file's
    /// CONTENTS cannot — a read that never terminates. The store's own
    /// read gate states the same boundary in the same words.
    /// Returns the file's current length — the read's resume bound.
    async fn gate_regular_file(&self) -> Result<u64, SourceError> {
        let meta = tokio::fs::metadata(&self.path)
            .await
            .map_err(|error| classify_io(&self.path, error))?;
        if !meta.is_file() {
            return Err(SourceError::fatal(format!(
                "reference source: {}: not a regular file — `path` must name a jsonl file",
                self.path
            )));
        }
        Ok(meta.len())
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
