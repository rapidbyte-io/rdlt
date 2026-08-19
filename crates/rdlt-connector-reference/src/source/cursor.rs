//! The persisted cursor and its resume law. The cursor is the count of
//! consumed bytes plus a hash of the bytes just before it, so a re-run
//! over an unchanged file reads zero rows, a grown file yields only its
//! tail, and a file that shrank — or was rewritten in place — refuses
//! typed rather than emitting unrelated bytes as if they were appended
//! rows.

use rdlt_connector_sdk::spi::core::cursor::Cursor;
use rdlt_connector_sdk::spi::error::SourceError;

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

/// The hash the cursor carries: the tail window of everything consumed,
/// hex-encoded.
fn hash(bytes: &[u8], bytes_read: usize) -> String {
    let window = bytes_read.min(TAIL_WINDOW as usize);
    blake3::hash(&bytes[bytes_read - window..bytes_read])
        .to_hex()
        .to_string()
}

/// The cursor standing at `bytes_read` into `bytes`.
pub(crate) fn at(bytes: &[u8], bytes_read: usize) -> Cursor {
    Cursor::new(serde_json::json!({
        "v": 1,
        "bytes_read": bytes_read as u64,
        "tail_hash": hash(bytes, bytes_read),
    }))
}

/// Decode a resume cursor against the file's current bytes: refuse
/// typed when the file shrank below the cursor OR when the bytes just
/// before it no longer hash to what the cursor recorded — a
/// rewrite-in-place, where a bare offset would silently emit the tail
/// of unrelated new content as appended rows.
pub(crate) fn resume(path: &str, cursor: &Cursor, bytes: &[u8]) -> Result<usize, SourceError> {
    let len = bytes.len() as u64;
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
    let bytes_read = v1.bytes_read as usize;
    if hash(bytes, bytes_read) != v1.tail_hash {
        let window = bytes_read.min(TAIL_WINDOW as usize);
        return Err(SourceError::fatal(format!(
            "reference source: {path}: the {window} bytes before the cursor no longer match \
             its tail hash — the file was rewritten in place, refusing to resume"
        )));
    }
    Ok(bytes_read)
}
