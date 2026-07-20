//! JSONL reading: slab-sized pushes of complete lines through the raw-bytes perf
//! path, per-slab checkpoints, byte-offset resume.

use std::io::{BufRead, BufReader, Seek, SeekFrom};

use bytes::Bytes;
use rdlt_connector::{RecordsOut, SourceError};

use crate::cursor::{FileCursor, FileProgress, FileTask};

const SLAB_BYTES: usize = 8 << 20;

/// Read one file task, pushing slabs and checkpointing progress into `cursor`.
/// Returns Ok(false) if the host closed the channel (cancellation).
pub(crate) async fn read_task(
    task: &FileTask,
    validate: bool,
    cursor: &mut FileCursor,
    out: &mut RecordsOut,
) -> Result<bool, SourceError> {
    let file = std::fs::File::open(&task.path)
        .map_err(|e| SourceError::fatal(format!("opening `{}`: {e}", task.path)))?;
    let total_size = file
        .metadata()
        .map_err(|e| SourceError::fatal(format!("stat `{}`: {e}", task.path)))?
        .len();
    let mut reader = BufReader::with_capacity(SLAB_BYTES, file);
    if task.start > 0 {
        reader
            .seek(SeekFrom::Start(task.start))
            .map_err(|e| SourceError::fatal(format!("seek `{}`: {e}", task.path)))?;
    }

    let mut offset = task.start;
    let mut slab: Vec<u8> = Vec::with_capacity(SLAB_BYTES);
    let mut line = String::new();
    // Whether the consumed range ends at a newline. A final line without one still
    // loads (files legitimately end that way), but the cursor remembers it: if the
    // file GROWS later, the recorded offset points mid-record and resume must fail
    // loudly instead of reading from the middle of a record.
    let mut ended_on_newline = true;
    loop {
        // Fill a slab with complete lines.
        slab.clear();
        while slab.len() < SLAB_BYTES {
            line.clear();
            let n = reader.read_line(&mut line).map_err(|e| {
                SourceError::fatal(format!("reading `{}` at offset {offset}: {e}", task.path))
            })?;
            if n == 0 {
                break; // EOF
            }
            if validate && !line.trim().is_empty() {
                // Cheap skim-parse (no tree): malformed input fails HERE, naming the
                // file and byte offset, instead of later inside the engine.
                if let Err(e) = serde_json::from_str::<serde::de::IgnoredAny>(&line) {
                    return Err(SourceError::fatal(format!(
                        "malformed JSON in `{}` at byte offset {offset}: {e}",
                        task.path
                    )));
                }
            }
            slab.extend_from_slice(line.as_bytes());
            offset += n as u64;
            ended_on_newline = line.ends_with('\n');
        }
        if slab.is_empty() {
            break;
        }
        if out.raw_json(Bytes::copy_from_slice(&slab)).await.is_err() {
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
