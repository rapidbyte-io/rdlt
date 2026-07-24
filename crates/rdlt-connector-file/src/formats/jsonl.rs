//! JSONL reading: slab-sized pushes of complete lines through the raw-bytes perf
//! path, per-slab checkpoints, byte-offset resume.
//!
//! Slabs are read as raw bytes and split on `memchr` newlines — no per-line `String`,
//! no per-line UTF-8 validation (the JSON parse downstream validates), and the slab
//! moves into `Bytes` without a copy. The slab-assembly logic (fill until a newline,
//! carry the unterminated tail, emit the final line at EOF) lives ONCE in
//! [`SlabReader`]; the byte-offset-resume reader and the whole-file reader differ
//! only in their fill primitive (async object stream vs sync codec) and in what they
//! record per slab.

use bytes::Bytes;
use rdlt_connector::{RecordsOut, SourceError};

use super::SLAB_BYTES;
use crate::location::{ByteReader, Location};
use crate::source::cursor::{FileCursor, FileProgress, FileTask, TAIL_WINDOW};

/// Fills a caller-provided buffer as far as possible, returning the byte count
/// (0 = end of stream). One abstraction over the async object-stream reader and
/// the synchronous codec reader, so both share [`SlabReader`].
trait SlabFill {
    async fn fill(&mut self, buf: &mut [u8]) -> Result<usize, SourceError>;
}

/// Async fill from a [`ByteReader`] (local file or object-store GET). A
/// mid-object transport reset is carried through the io seam as a transient.
struct AsyncFill<'a> {
    reader: &'a mut ByteReader,
    path: &'a str,
}

impl SlabFill for AsyncFill<'_> {
    async fn fill(&mut self, buf: &mut [u8]) -> Result<usize, SourceError> {
        self.reader.read_full(buf).await.map_err(|e| {
            crate::location::classify_read_error(&format!("reading `{}`", self.path), e)
        })
    }
}

/// Synchronous fill from a decoded local reader (the compressed whole-file path;
/// compressed streams are not seekable, so this reader never tail-resumes).
struct SyncFill<'a> {
    reader: Box<dyn std::io::Read + Send>,
    path: &'a str,
}

impl SlabFill for SyncFill<'_> {
    async fn fill(&mut self, buf: &mut [u8]) -> Result<usize, SourceError> {
        use std::io::Read;
        let mut filled = 0;
        while filled < buf.len() {
            match self.reader.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(SourceError::fatal(format!("reading `{}`: {e}", self.path))),
            }
        }
        Ok(filled)
    }
}

/// Assembles slabs of COMPLETE lines from a byte source. Each `next` returns the
/// next slab ending at a newline (or, at EOF, the final unterminated line); the
/// bytes after the last newline carry into the following slab. `None` marks the
/// stream fully consumed. A single line longer than one slab grows the buffer
/// until its newline (or EOF) arrives.
struct SlabReader {
    carry: Vec<u8>,
    done: bool,
}

impl SlabReader {
    fn new() -> Self {
        Self {
            carry: Vec::new(),
            done: false,
        }
    }

    async fn next<F: SlabFill>(&mut self, fill: &mut F) -> Result<Option<Vec<u8>>, SourceError> {
        if self.done {
            return Ok(None);
        }
        let mut slab = std::mem::take(&mut self.carry);
        slab.reserve(SLAB_BYTES);
        let mut eof = false;
        loop {
            let filled = slab.len();
            slab.resize(filled + SLAB_BYTES, 0);
            let n = fill.fill(&mut slab[filled..]).await?;
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
        let split = memchr::memrchr(b'\n', &slab)
            .map(|nl| nl + 1)
            .unwrap_or(slab.len());
        self.carry = slab.split_off(split);
        if slab.is_empty() {
            if eof && self.carry.is_empty() {
                self.done = true;
                return Ok(None);
            }
            if eof {
                // EOF with only an unterminated fragment: emit it as the final line.
                slab = std::mem::take(&mut self.carry);
            }
        }
        if slab.is_empty() {
            self.done = true;
            return Ok(None);
        }
        if eof && self.carry.is_empty() {
            self.done = true;
        }
        Ok(Some(slab))
    }
}

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
    let total_size = task.size_units;

    let mut offset = task.start;
    // Whether the consumed range ends at a newline. A final line without one still
    // loads (files legitimately end that way), but the cursor remembers it: if the
    // file GROWS later, the recorded offset points mid-record and resume must fail
    // loudly instead of reading from the middle of a record.
    let mut ended_on_newline = true;

    let mut fill = AsyncFill {
        reader: &mut file,
        path: &task.path,
    };
    let mut slabs = SlabReader::new();
    while let Some(slab) = slabs.next(&mut fill).await? {
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
                done_units: offset,
                size_units: total_size.max(offset),
                ended_at_record_boundary: ended_on_newline,
                mtime_ms: task.mtime_ms,
                etag: task.etag.clone(),
                tail_hash: Some(blake3::hash(&tail).to_hex().to_string()),
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
            done_units: offset,
            size_units: offset.max(task.start),
            ended_at_record_boundary: ended_on_newline,
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
    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    let reader = super::open_decoded(read_path, super::codec_of(&task.path))?;
    let mut offset = 0u64; // decompressed bytes, for error context only

    let mut fill = SyncFill {
        reader,
        path: &task.path,
    };
    let mut slabs = SlabReader::new();
    while let Some(slab) = slabs.next(&mut fill).await? {
        if validate {
            validate_lines(&slab, offset, &task.path)?;
        }
        offset += slab.len() as u64;
        if out.raw_json(Bytes::from(slab)).await.is_err() {
            return Ok(false); // closed channel = cancellation
        }
    }
    cursor.record(
        &task.path,
        FileProgress {
            done_units: task.size_units,
            size_units: task.size_units,
            ended_at_record_boundary: true,
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
