//! The persisted cursor and its resume law. The cursor is the count of
//! consumed bytes plus a hash of the bytes just before it, so a re-run
//! over an unchanged file reads zero rows, a grown file yields only its
//! tail, and a file that shrank — or was rewritten in place — refuses
//! typed rather than emitting unrelated bytes as if they were appended
//! rows.

use rdlt_connector_sdk::spi::core::cursor::Cursor;
use rdlt_connector_sdk::spi::error::SourceError;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

/// How far back the cursor's rewrite guard reaches: the hash covers the
/// last `min(bytes_read, TAIL_WINDOW)` consumed bytes. A rewrite that
/// preserves that window byte-for-byte legitimately resumes — the guard
/// answers "is this still the file I read", not "is every byte before
/// the cursor identical".
const TAIL_WINDOW: u64 = 4096;

/// The persisted cursor, v1: `{"v":1,"bytes_read":<u64>,"tail_hash":
/// <hex>}`. The wire keys are frozen; a document with any other shape
/// (or a future `v`) is refused typed rather than read as zero.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct V1 {
    v: u32,
    bytes_read: u64,
    tail_hash: String,
}

/// The rolling tail of everything the read has consumed — the last
/// `min(consumed, TAIL_WINDOW)` bytes, maintained incrementally so the
/// streaming read can mint a cursor at any batch boundary without the
/// file's earlier bytes in memory. Newlines and skipped blank lines are
/// consumed bytes too: the hash covers the raw prefix, not the rows.
#[derive(Debug)]
pub(crate) struct Tail(Vec<u8>);

impl Tail {
    /// The tail of a read starting at offset zero: empty, like the
    /// consumed prefix.
    pub(crate) fn start() -> Self {
        Self(Vec::new())
    }

    /// Extend the tail with just-consumed bytes, keeping the window.
    pub(crate) fn push(&mut self, consumed: &[u8]) {
        let window = TAIL_WINDOW as usize;
        if consumed.len() >= window {
            self.0.clear();
            self.0
                .extend_from_slice(&consumed[consumed.len() - window..]);
            return;
        }
        self.0.extend_from_slice(consumed);
        if self.0.len() > window {
            self.0.drain(..self.0.len() - window);
        }
    }

    fn hash_hex(&self) -> String {
        blake3::hash(&self.0).to_hex().to_string()
    }
}

/// The cursor standing at `bytes_read`, its guard hash read off the
/// rolling tail.
pub(crate) fn at(tail: &Tail, bytes_read: u64) -> Cursor {
    Cursor::new(serde_json::json!({
        "v": 1,
        "bytes_read": bytes_read,
        "tail_hash": tail.hash_hex(),
    }))
}

/// Decode a resume cursor against the file's current shape: refuse
/// typed when the file shrank below the cursor OR when the bytes just
/// before it no longer hash to what the cursor recorded — a
/// rewrite-in-place, where a bare offset would silently emit the tail
/// of unrelated new content as appended rows. Only the guard window is
/// read (seek to the cursor minus the window), never the prefix, so
/// resuming a multi-GiB file stays cheap. On success the read's rolling
/// tail comes back seeded with the verified window.
pub(crate) async fn resume(
    path: &str,
    cursor: &Cursor,
    file: &mut tokio::fs::File,
    len: u64,
) -> Result<(u64, Tail), SourceError> {
    let v1: V1 = serde_json::from_value(cursor.as_value().clone()).map_err(|error| {
        // The cursor's own JSON is NOT echoed — a served cursor can
        // weigh megabytes and a direct driver's is unbounded; the serde
        // error names the shape problem, its text bounded because it
        // can embed value fragments.
        SourceError::fatal(format!(
            "reference source: {path}: unrecognized resume cursor: {}",
            rdlt_connector_sdk::spi::gate::render_diagnostic(&error.to_string(), 256)
        ))
    })?;
    if v1.v != 1 {
        return Err(SourceError::fatal(format!(
            "reference source: {path}: cursor version {} is newer than this build reads (v1)",
            v1.v
        )));
    }
    if v1.bytes_read > len {
        return Err(SourceError::fatal(format!(
            "reference source: {path} shrank below the cursor ({} > {len}): refusing to guess",
            v1.bytes_read
        )));
    }
    let window = v1.bytes_read.min(TAIL_WINDOW);
    // An IO failure reading the guard window is transient — EXCEPT the
    // one that means the file no longer holds what the cursor says it
    // does. The length was checked a moment ago; a short read here says
    // it shrank in between, which is the same fact the shrink refusal
    // above states and no retry can change. Classifying it transient
    // would spend a retry on a certainty and then report exhaustion in
    // place of the real cause.
    let read_io = |error: std::io::Error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            SourceError::fatal(format!(
                "reference source: {path} shrank below the cursor while its guard window \
                 was being read: refusing to guess"
            ))
        } else {
            SourceError::transient(format!("reference source: {path}: {error}"))
        }
    };
    file.seek(std::io::SeekFrom::Start(v1.bytes_read - window))
        .await
        .map_err(read_io)?;
    let mut tail = vec![0u8; window as usize];
    file.read_exact(&mut tail).await.map_err(read_io)?;
    if blake3::hash(&tail).to_hex().to_string() != v1.tail_hash {
        return Err(SourceError::fatal(format!(
            "reference source: {path}: the {window} bytes before the cursor no longer match \
             its tail hash — the file was rewritten in place, refusing to resume"
        )));
    }
    Ok((v1.bytes_read, Tail(tail)))
}
