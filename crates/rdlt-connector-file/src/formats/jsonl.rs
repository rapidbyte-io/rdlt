//! JSONL reading: slab-sized pushes of complete lines through the raw-bytes perf
//! path, per-slab checkpoints, byte-offset resume.
//!
//! Feature 003 (FR-007): slabs are read as raw bytes and split on `memchr`
//! newlines — no per-line `String`, no per-line UTF-8 validation (the JSON
//! parse downstream validates), and the slab moves into `Bytes` without a copy.

use std::io::{Read, Seek, SeekFrom};

use bytes::Bytes;
use rdlt_connector::{RecordsOut, SourceError};

use crate::source::cursor::{FileCursor, FileProgress, FileTask};

const SLAB_BYTES: usize = 8 << 20;

/// Read one file task, pushing slabs and checkpointing progress into `cursor`.
/// Returns Ok(false) if the host closed the channel (cancellation).
pub(crate) async fn read_task(
    task: &FileTask,
    validate: bool,
    cursor: &mut FileCursor,
    out: &mut RecordsOut,
) -> Result<bool, SourceError> {
    let mut file = std::fs::File::open(&task.path)
        .map_err(|e| SourceError::fatal(format!("opening `{}`: {e}", task.path)))?;
    let total_size = file
        .metadata()
        .map_err(|e| SourceError::fatal(format!("stat `{}`: {e}", task.path)))?
        .len();
    if task.start > 0 {
        file.seek(SeekFrom::Start(task.start))
            .map_err(|e| SourceError::fatal(format!("seek `{}`: {e}", task.path)))?;
    }

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
            let n = read_full(&mut file, &mut slab[filled..])
                .map_err(|e| SourceError::fatal(format!("reading `{}`: {e}", task.path)))?;
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

        // Zero-copy handoff: the Vec becomes the pushed Bytes.
        if out.raw_json(Bytes::from(slab)).await.is_err() {
            return Ok(false); // clause S4: closed channel = cancellation
        }
        // Progress is durable-intent only once checkpointed (clause S2: the
        // checkpoint covers exactly the rows pushed before it).
        cursor.record(
            &task.path,
            FileProgress {
                done: offset,
                size: total_size.max(offset),
                eol: ended_on_newline,
                mtime_ms: task.mtime_ms,
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
        },
    );
    if out.checkpoint(cursor.encode()).await.is_err() {
        return Ok(false);
    }
    Ok(true)
}

/// `Read::read` until the buffer is full or EOF (plain files can short-read).
fn read_full(file: &mut std::fs::File, mut buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while !buf.is_empty() {
        match file.read(buf) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                buf = &mut buf[n..];
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
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
