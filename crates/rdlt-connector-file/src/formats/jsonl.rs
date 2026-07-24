//! JSONL reading: slab-sized pushes of complete lines through the raw-bytes perf
//! path, per-slab checkpoints, byte-offset resume.
//!
//! Slabs are read as raw bytes and split on `memchr` newlines — no per-line `String`,
//! no per-line UTF-8 validation (the JSON parse downstream validates), and the slab
//! moves into `Bytes` without a copy.

use bytes::Bytes;
use rdlt_connector::{RecordsOut, SourceError};

use crate::location::{ByteReader, Location};
use crate::source::cursor::{FileCursor, FileProgress, FileTask, TAIL_WINDOW};

/// Keep `tail` = the last `TAIL_WINDOW` bytes of everything consumed.
fn roll_tail(tail: &mut Vec<u8>, slab: &[u8]) {
    let window = TAIL_WINDOW as usize;
    if slab.len() >= window {
        tail.clear();
        tail.extend_from_slice(&slab[slab.len() - window..]);
    } else {
        tail.extend_from_slice(slab);
        if tail.len() > window {
            tail.drain(..tail.len() - window);
        }
    }
}

const SLAB_BYTES: usize = 8 << 20;

/// Read one file task, pushing slabs and checkpointing progress into `cursor`.
/// Returns Ok(false) if the host closed the channel (cancellation).
pub(crate) async fn read_task(
    location: &Location,
    task: &FileTask,
    validate: bool,
    cursor: &mut FileCursor,
    out: &mut RecordsOut,
) -> Result<bool, SourceError> {
    // Resume-offset integrity: re-read the recorded tail window and
    // compare BEFORE trusting the offset — a rewritten prefix fails loudly;
    // a genuine append verifies and continues from the same open reader.
    let verify = task.tail_check.as_ref().filter(|_| task.start > 0);
    let open_at = match verify {
        Some((window, _)) => task.start - window,
        None => task.start,
    };
    let mut file: ByteReader = location.open_from(&task.path, open_at).await?;
    // Rolling buffer of the last consumed bytes (hash goes into progress).
    let mut tail: Vec<u8> = Vec::new();
    if let Some((window, expected)) = verify {
        let mut got = vec![0u8; *window as usize];
        let n = file.read_full(&mut got).await.map_err(|e| {
            crate::location::classify_read_error(&format!("reading `{}`", task.path), e)
        })?;
        got.truncate(n);
        let matches = n as u64 == *window && blake3::hash(&got).to_hex().to_string() == *expected;
        if !matches {
            return Err(SourceError::fatal(format!(
                "file `{}` was rewritten before the resume offset (the content \
                 preceding byte {} changed since the last run); refusing to \
                 read a stale tail — clear it from the pipeline state or \
                 restore the file",
                task.path, task.start
            )));
        }
        tail = got;
    }
    // Snapshot size from the listing; the loop reads to end-of-stream, so a
    // file that grew since listing still loads whole (progress caps at the
    // observed end).
    let total_size = task.size;

    let mut offset = task.start;
    // Bytes after the last newline of the previous read — always < one line.
    let mut carry: Vec<u8> = Vec::new();
    // Whether the consumed range ends at a newline. A final line without one still
    // loads (files legitimately end that way), but the cursor remembers it: if the
    // file GROWS later, the recorded offset points mid-record and resume must fail
    // loudly instead of reading from the middle of a record.
    let mut ended_on_newline = true;

    loop {
        // One slab: the carried tail + fresh bytes. A single line longer than
        // the slab keeps growing the buffer until its newline (or EOF) arrives.
        let mut slab = std::mem::take(&mut carry);
        slab.reserve(SLAB_BYTES);
        let mut eof = false;
        loop {
            let filled = slab.len();
            slab.resize(filled + SLAB_BYTES, 0);
            let n = file.read_full(&mut slab[filled..]).await.map_err(|e| {
                crate::location::classify_read_error(&format!("reading `{}`", task.path), e)
            })?;
            slab.truncate(filled + n);
            if n == 0 {
                eof = true;
                break;
            }
            if memchr::memrchr(b'\n', &slab).is_some() {
                break;
            }
            // No newline yet: the line spans slabs — keep growing.
        }

        // Split at the last newline; the tail carries into the next round.
        let split = match memchr::memrchr(b'\n', &slab) {
            Some(nl) => nl + 1,
            None if eof => slab.len(), // unterminated final line: loads whole
            None => slab.len(),        // unreachable: non-EOF exits only on a newline
        };
        carry = slab.split_off(split);

        if slab.is_empty() {
            if eof && carry.is_empty() {
                break;
            }
            if eof {
                // EOF with only an unterminated fragment: push it as the final line.
                slab = std::mem::take(&mut carry);
            }
        }
        if slab.is_empty() {
            break;
        }

        if validate {
            validate_lines(&slab, offset, &task.path)?;
        }
        ended_on_newline = slab.last() == Some(&b'\n');
        offset += slab.len() as u64;
        roll_tail(&mut tail, &slab);

        // Zero-copy handoff: the Vec becomes the pushed Bytes.
        if out.raw_json(Bytes::from(slab)).await.is_err() {
            return Ok(false); // closed channel = cancellation
        }
        // Progress is durable-intent only once checkpointed: the checkpoint
        // covers exactly the rows pushed before it.
        cursor.record(
            &task.path,
            FileProgress {
                done: offset,
                size: total_size.max(offset),
                eol: ended_on_newline,
                mtime_ms: task.mtime_ms,
                etag: task.etag.clone(),
                tail_hash: Some(blake3::hash(&tail).to_hex().to_string()),
            },
        );
        if out.checkpoint(cursor.encode()).await.is_err() {
            return Ok(false);
        }
        if eof && carry.is_empty() {
            break;
        }
    }

    // File fully consumed: mark complete at its observed end position.
    cursor.record(
        &task.path,
        FileProgress {
            done: offset,
            size: offset.max(task.start),
            eol: ended_on_newline,
            mtime_ms: task.mtime_ms,
            etag: task.etag.clone(),
            tail_hash: Some(blake3::hash(&tail).to_hex().to_string()),
        },
    );
    if out.checkpoint(cursor.encode()).await.is_err() {
        return Ok(false);
    }
    Ok(true)
}

/// Skim-parse each line (no tree): malformed input fails HERE, naming the file
/// and the LINE-START byte offset, instead of later inside the engine.
fn validate_lines(slab: &[u8], slab_start: u64, path: &str) -> Result<(), SourceError> {
    let mut line_start = 0usize;
    for nl in memchr::memchr_iter(b'\n', slab).chain(std::iter::once(slab.len())) {
        if nl > line_start {
            let line = &slab[line_start..nl];
            if !line.iter().all(u8::is_ascii_whitespace)
                && let Err(e) = serde_json::from_slice::<serde::de::IgnoredAny>(line)
            {
                return Err(SourceError::fatal(format!(
                    "malformed JSON in `{path}` at byte offset {}: {e}",
                    slab_start + line_start as u64
                )));
            }
        }
        line_start = nl + 1;
        if line_start > slab.len() {
            break;
        }
    }
    Ok(())
}

/// Whole-file jsonl (compressed codecs, R5): decode through the codec,
/// same slab/line discipline, ONE completion checkpoint (no tail resume —
/// compressed streams are not seekable; a mid-file crash re-delivers the
/// file, exactly-once under keyed merge/dedup).
pub(crate) async fn read_task_whole(
    task: &FileTask,
    validate: bool,
    cursor: &mut FileCursor,
    out: &mut RecordsOut,
) -> Result<bool, SourceError> {
    use std::io::Read;
    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    let mut reader = super::open_decoded(read_path, super::codec_of(&task.path))?;
    let mut offset = 0u64; // decompressed bytes, for error context only
    let mut carry: Vec<u8> = Vec::new();
    loop {
        let mut slab = std::mem::take(&mut carry);
        slab.reserve(SLAB_BYTES);
        let mut eof = false;
        loop {
            let filled = slab.len();
            slab.resize(filled + SLAB_BYTES, 0);
            let mut n = 0;
            while n < SLAB_BYTES {
                match reader.read(&mut slab[filled + n..]) {
                    Ok(0) => break,
                    Ok(read) => n += read,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        return Err(SourceError::fatal(format!("reading `{}`: {e}", task.path)));
                    }
                }
            }
            slab.truncate(filled + n);
            if n == 0 {
                eof = true;
                break;
            }
            if memchr::memrchr(b'\n', &slab).is_some() {
                break;
            }
        }
        let split = match memchr::memrchr(b'\n', &slab) {
            Some(nl) => nl + 1,
            None => slab.len(),
        };
        carry = slab.split_off(split);
        if slab.is_empty() {
            if eof && carry.is_empty() {
                break;
            }
            if eof {
                slab = std::mem::take(&mut carry);
            }
        }
        if slab.is_empty() {
            break;
        }
        if validate {
            validate_lines(&slab, offset, &task.path)?;
        }
        offset += slab.len() as u64;
        if out.raw_json(Bytes::from(slab)).await.is_err() {
            return Ok(false); // closed channel = cancellation
        }
        if eof && carry.is_empty() {
            break;
        }
    }
    cursor.record(
        &task.path,
        FileProgress {
            done: task.size,
            size: task.size,
            eol: true,
            mtime_ms: task.mtime_ms,
            etag: task.etag.clone(),
            tail_hash: None, // whole-file units never tail-resume
        },
    );
    if out.checkpoint(cursor.encode()).await.is_err() {
        return Ok(false);
    }
    Ok(true)
}
