//! The shared Arrow-IPC stream-framing pre-pass: hold every DECLARED
//! length in a frame against the frame's actual bytes before any arrow
//! reader is allowed to allocate from them.
//!
//! This is ONE implementation for every wire decode seat (GLM round-5,
//! 5H1 — the class was fixed seat-by-seat three rounds running: the
//! client's read-decode in round 3, the WAL segment seat in wave 5, and
//! the certifier and serve-destination seats were still naked). Install
//! it before every `StreamReader`/`FileReader` construction over
//! connector-supplied bytes, and keep a `catch_unwind` belt beside it
//! for arrow's panic arms — the two failure modes are disjoint.
//!
//! The threat: arrow-ipc 58.3 trusts both declarations before verifying
//! them against the input: its stream reader `resize`s the metadata
//! buffer to the declared length (`Vec::resize` zero-fills every new
//! slot — a commit and memset of the full size) and allocates
//! `bodyLength` zeroed bytes for the body, in each case BEFORE
//! `read_exact` discovers the bytes are missing. So a ~30-byte frame
//! declaring ~2 GiB forces a 2 GiB allocate-and-memset in the host per
//! frame — not a panic, so no `catch_unwind` is a defense, and under a
//! memory limit it is an OOM kill. A negative `bodyLength` is the
//! sibling: cast to `usize` it wraps huge, and the failing allocation
//! ABORTS the process outright (`handle_alloc_error`), which no
//! containment absorbs. Checking every declaration against
//! `bytes.len()` first kills both vectors; wire frames arrive whole
//! (one gRPC field each), so a valid frame can never declare past its
//! own end.
//!
//! Compression note (4I3): arrow-ipc's `decompress_to_buffer` does an
//! unbounded `Vec::with_capacity` from a body-declared length — the same
//! class one layer down. It is unreachable today (no `ipc_compression`
//! feature and no lz4/zstd anywhere in the lockfile); if compression is
//! ever enabled, this walk must ALSO bound the decompressed length.

/// Walk one IPC stream's encapsulated-message framing — optional
/// continuation marker, an `i32` metadata length, the metadata bytes,
/// then the metadata's own declared `bodyLength` of body bytes — and
/// refuse any message whose DECLARED lengths exceed what the frame
/// actually carries. The reason strings name the offending declaration
/// verbatim; seats wrap them in their own refusal vocabulary.
///
/// All arithmetic is checked: a walk this refuses is malformed by
/// construction, never a panic. Truncation SHORT of a declaration (too
/// few bytes for a length word) is left for the reader's own EOF
/// handling — nothing oversized gets allocated on that path.
pub fn refuse_overdeclared_ipc_framing(bytes: &[u8]) -> Result<(), String> {
    const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
    let refuse = |what: &str, declared: u64| {
        format!(
            "a declared {what} length of {declared} bytes exceeds the {}-byte frame",
            bytes.len()
        )
    };
    let mut pos = 0usize;
    loop {
        let Some(word) = bytes.get(pos..pos + 4) else {
            return Ok(());
        };
        let word: [u8; 4] = word.try_into().expect("a 4-byte slice");
        let length_word = if word == CONTINUATION_MARKER {
            pos += 4;
            match bytes.get(pos..pos + 4) {
                Some(next) => next.try_into().expect("a 4-byte slice"),
                None => return Ok(()),
            }
        } else {
            word
        };
        pos += 4;
        let declared_meta = i32::from_le_bytes(length_word);
        if declared_meta == 0 {
            // The stream's end-of-stream marker.
            return Ok(());
        }
        let meta_len = usize::try_from(declared_meta)
            .map_err(|_| format!("a negative declared metadata length ({declared_meta})"))?;
        let meta_end = pos
            .checked_add(meta_len)
            .filter(|&end| end <= bytes.len())
            .ok_or_else(|| refuse("metadata", meta_len as u64))?;
        // The metadata really is present — now hold its own body
        // declaration to the same standard. The flatbuffer runs the
        // same verifier the reader itself would, so an unverifiable
        // message refuses here with the verifier's diagnostic.
        let message = arrow_ipc::root_as_message(&bytes[pos..meta_end])
            .map_err(|error| format!("unverifiable message metadata: {error}"))?;
        // A negative declaration renders SIGNED (4I4) — casting to u64
        // first would print the wrapped value, a diagnostic that lies
        // about what the frame actually declared.
        let body_len = u64::try_from(message.bodyLength())
            .map_err(|_| format!("a negative declared body length ({})", message.bodyLength()))?;
        pos = usize::try_from(body_len)
            .ok()
            .and_then(|body| meta_end.checked_add(body))
            .filter(|&end| end <= bytes.len())
            .ok_or_else(|| refuse("body", body_len))?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two declaration arms at their exact spellings — seats append
    /// these reasons to their own frozen prefixes, so the wording is
    /// the contract.
    #[test]
    fn overdeclared_lengths_refuse_with_the_shared_spellings() {
        let mut frame = vec![0xff, 0xff, 0xff, 0xff];
        frame.extend_from_slice(&0x7fff_fff0_i32.to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);
        assert_eq!(
            refuse_overdeclared_ipc_framing(&frame).expect_err("meta overdeclare refuses"),
            "a declared metadata length of 2147483632 bytes exceeds the 24-byte frame"
        );

        // A negative metadata length word.
        let mut frame = vec![0xff, 0xff, 0xff, 0xff];
        frame.extend_from_slice(&(-1_i32).to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);
        assert_eq!(
            refuse_overdeclared_ipc_framing(&frame).expect_err("negative meta refuses"),
            "a negative declared metadata length (-1)"
        );
    }

    /// The walk's honest-pass property: a real one-batch stream, an
    /// end-of-stream marker, and a schema-only stream all walk clean.
    #[test]
    fn honest_streams_walk_clean() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .expect("batch");
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), batch.schema_ref())
            .expect("writer");
        writer.write(&batch).expect("write");
        let bytes = writer.into_inner().expect("finish");
        refuse_overdeclared_ipc_framing(&bytes).expect("an honest one-batch stream walks clean");
    }
}
