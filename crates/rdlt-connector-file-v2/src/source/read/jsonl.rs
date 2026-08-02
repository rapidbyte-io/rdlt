//! JSONL reading: slab-sized pushes of complete lines down the
//! raw-bytes path, per-slab checkpoints, byte-offset resume behind the
//! tail-hash verification.
//!
//! Slabs are raw bytes split on memchr newlines — no per-line String,
//! no per-line UTF-8 pass (the downstream parse validates); a slab
//! moves into `Bytes` without a copy. Slab assembly (fill to a
//! newline, carry the unterminated tail, emit the final line at EOF)
//! lives once in [`SlabReader`]; the byte-resume reader and the
//! whole-file reader differ only in their fill primitive.

use std::ops::ControlFlow;

use bytes::Bytes;
use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;

use super::open_decoded;
use crate::format::{SLAB_BYTES, codec_of};
use crate::location::{ByteReader, Location, classify_read_error};
use crate::source::cursor::{FileCursor, FileProgress, FileTask, ResumeCheck, TAIL_WINDOW};

/// Fill a buffer as far as possible (0 = end of stream) — the one
/// abstraction over the async object reader and the sync codec reader.
trait SlabFill {
    async fn fill(&mut self, buf: &mut [u8]) -> Result<usize, SourceError>;
}

struct AsyncFill<'a> {
    reader: &'a mut ByteReader,
    path: &'a str,
}

impl SlabFill for AsyncFill<'_> {
    async fn fill(&mut self, buf: &mut [u8]) -> Result<usize, SourceError> {
        self.reader
            .read_full(buf)
            .await
            .map_err(|e| classify_read_error(&format!("reading `{}`", self.path), e))
    }
}

struct SyncFill<'a> {
    reader: Box<dyn std::io::Read + Send>,
    path: &'a str,
}

impl SlabFill for SyncFill<'_> {
    async fn fill(&mut self, buf: &mut [u8]) -> Result<usize, SourceError> {
        use std::io::Read as _;
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

/// Assembles slabs of COMPLETE lines: each `next` ends at a newline
/// (or, at EOF, the final unterminated line); the tail after the last
/// newline carries forward; a line longer than one slab grows the
/// buffer until its newline arrives; `None` means fully consumed.
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
        }
        let split = memchr::memrchr(b'\n', &slab)
            .map(|nl| nl + 1)
            .unwrap_or(slab.len());
        self.carry = slab.split_off(split);
        if slab.is_empty() && eof {
            if self.carry.is_empty() {
                self.done = true;
                return Ok(None);
            }
            // EOF holding only an unterminated fragment: it IS the
            // final line.
            slab = std::mem::take(&mut self.carry);
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

/// Read one byte-resume task. Returns Ok(false) on host cancellation.
pub(crate) async fn read_task(
    location: &Location,
    task: &FileTask,
    validate: bool,
    cursor: &mut FileCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    // A check of the wrong KIND is refused, not ignored: skipping it
    // would silently disarm verification for a resume this reader
    // cannot evaluate.
    let verify = match task.resume_check.as_ref() {
        Some(ResumeCheck::TailBytes { window, hash }) if task.start > 0 => Some((window, hash)),
        Some(ResumeCheck::TailBytes { .. }) | None => None,
        Some(ResumeCheck::RowGroupPrefix { .. }) => {
            return Err(SourceError::fatal(format!(
                "file `{}` recorded a row-group integrity value but is being read as a record \
                 stream; refusing to resume without a check this reader can evaluate — clear it \
                 from the pipeline state",
                task.path
            )));
        }
    };
    // The window is re-read on EVERY resume, verified or not: the tail
    // this run records must hash the full `min(done, TAIL_WINDOW)`
    // window the NEXT resume will re-derive, and only the bytes behind
    // the offset can complete it. Seeding only on the verified path
    // poisoned every unverified resume under 4 KiB of growth — the
    // next run refused a healthy append as "rewritten" (030 review).
    let window = match verify {
        Some((window, _)) => *window,
        None => task.start.min(crate::source::cursor::TAIL_WINDOW),
    };
    let mut file = location.open_from(&task.path, task.start - window).await?;
    let mut tail: Vec<u8> = vec![0u8; window as usize];
    let n = file
        .read_full(&mut tail)
        .await
        .map_err(|e| classify_read_error(&format!("reading `{}`", task.path), e))?;
    tail.truncate(n);
    if let Some((window, expected)) = verify {
        // Compare count AND hash before trusting the offset: a genuine
        // append verifies and continues; a rewritten prefix fails
        // loudly.
        let matches = n as u64 == *window && blake3::hash(&tail).to_hex().to_string() == *expected;
        if !matches {
            return Err(SourceError::fatal(format!(
                "file `{}` was rewritten before the resume offset (the content preceding byte \
                 {} changed since the last run); refusing to read a stale tail — clear it from \
                 the pipeline state or restore the file",
                task.path, task.start
            )));
        }
    }

    let total_size = task.size_units;
    let mut offset = task.start;
    // A final line without a newline still loads, but the cursor
    // remembers it: growth past a mid-record offset must fail loudly.
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

        if feed.raw_json(Bytes::from(slab)).await == ControlFlow::Break(()) {
            return Ok(false);
        }
        cursor.record(
            &task.path,
            FileProgress {
                done_units: offset,
                size_units: total_size.max(offset),
                ended_at_record_boundary: ended_on_newline,
                mtime_ms: task.mtime_ms,
                etag: task.etag.clone(),
                tail_hash: Some(blake3::hash(&tail).to_hex().to_string()),
                row_groups_hash: None,
            },
        );
        if feed.checkpoint(cursor.encode()).await == ControlFlow::Break(()) {
            return Ok(false);
        }
    }

    // EOF short of the listing size means the file SHRANK while it was
    // being read. Recording it complete would cap the size down and
    // let the next run skip the missing bytes silently (030 review,
    // docket S3) — refuse instead, like every other impossible history.
    if offset < total_size {
        return Err(SourceError::fatal(format!(
            "file `{}` ended at byte {offset} but its listing recorded {total_size} — the \
             file shrank while it was being read; clear it from the pipeline state or \
             restore the file",
            task.path
        )));
    }
    let completion = FileProgress {
        done_units: offset,
        size_units: offset,
        ended_at_record_boundary: ended_on_newline,
        mtime_ms: task.mtime_ms,
        etag: task.etag.clone(),
        tail_hash: Some(blake3::hash(&tail).to_hex().to_string()),
        row_groups_hash: None,
    };
    // Only when it says something new: the last per-slab record is
    // usually identical, and a load should not end on a redundant
    // checkpoint (docket S13).
    if cursor.files.get(&task.path) != Some(&completion) {
        cursor.record(&task.path, completion);
        if feed.checkpoint(cursor.encode()).await == ControlFlow::Break(()) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whole-file jsonl (the compressed codecs): decode, same slab and
/// line discipline, ONE completion checkpoint — compressed streams are
/// not seekable, so a mid-file crash re-delivers the file (exactly-once
/// is the keyed merge/dedup layer's job).
pub(crate) async fn read_task_whole(
    task: &FileTask,
    validate: bool,
    cursor: &mut FileCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    super::verify_local_snapshot(task)?;
    let reader = open_decoded(read_path, codec_of(&task.path))?;
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
        if feed.raw_json(Bytes::from(slab)).await == ControlFlow::Break(()) {
            return Ok(false);
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
            row_groups_hash: None,
        },
    );
    if feed.checkpoint(cursor.encode()).await == ControlFlow::Break(()) {
        return Ok(false);
    }
    Ok(true)
}

/// Skim-parse each line (no tree): malformed input fails HERE, naming
/// the file and the LINE-START byte offset, not later in the engine.
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

#[cfg(test)]
mod tests {
    use super::*;

    struct VecFill {
        chunks: std::collections::VecDeque<Vec<u8>>,
    }

    impl SlabFill for VecFill {
        async fn fill(&mut self, buf: &mut [u8]) -> Result<usize, SourceError> {
            let Some(mut chunk) = self.chunks.pop_front() else {
                return Ok(0);
            };
            let take = chunk.len().min(buf.len());
            buf[..take].copy_from_slice(&chunk[..take]);
            if take < chunk.len() {
                chunk.drain(..take);
                self.chunks.push_front(chunk);
            }
            Ok(take)
        }
    }

    fn fill_of(chunks: &[&[u8]]) -> VecFill {
        VecFill {
            chunks: chunks.iter().map(|c| c.to_vec()).collect(),
        }
    }

    /// The slab discipline: complete lines per slab, the unterminated
    /// tail carried, and the final fragment emitted at EOF.
    #[tokio::test]
    async fn slabs_end_at_newlines_and_the_final_fragment_is_emitted() {
        let mut fill = fill_of(&[b"{\"a\":1}\n{\"b\"", b":2}\n{\"c\":3}"]);
        let mut slabs = SlabReader::new();
        let one = slabs.next(&mut fill).await.expect("slab").expect("some");
        assert_eq!(one, b"{\"a\":1}\n");
        let two = slabs.next(&mut fill).await.expect("slab").expect("some");
        assert_eq!(two, b"{\"b\":2}\n");
        let three = slabs.next(&mut fill).await.expect("slab").expect("some");
        assert_eq!(
            three, b"{\"c\":3}",
            "the unterminated final line still loads"
        );
        assert!(slabs.next(&mut fill).await.expect("end").is_none());
    }

    /// The rolling tail keeps exactly the last window of consumed
    /// bytes, across slabs smaller and larger than the window.
    #[test]
    fn the_tail_rolls_to_exactly_the_window() {
        let mut tail = Vec::new();
        roll_tail(&mut tail, &[1u8; 10]);
        assert_eq!(tail.len(), 10);
        roll_tail(&mut tail, &vec![2u8; TAIL_WINDOW as usize - 4]);
        assert_eq!(tail.len(), TAIL_WINDOW as usize);
        assert_eq!(&tail[..4], &[1, 1, 1, 1], "the old bytes' remainder leads");
        roll_tail(&mut tail, &vec![3u8; TAIL_WINDOW as usize + 100]);
        assert_eq!(tail.len(), TAIL_WINDOW as usize);
        assert!(
            tail.iter().all(|&b| b == 3),
            "a big slab replaces the window"
        );
    }

    /// Validation names the file and the LINE-START offset of the
    /// malformed line — whitespace-only lines are not records.
    #[test]
    fn validation_names_the_line_start_offset() {
        let slab = b"{\"ok\":1}\n   \nnot json\n";
        let err = validate_lines(slab, 100, "f.jsonl").expect_err("malformed");
        assert!(
            format!("{err}").contains("malformed JSON in `f.jsonl` at byte offset 113"),
            "{err}"
        );
        validate_lines(b"{\"ok\":1}\n", 0, "f.jsonl").expect("clean slab passes");
    }
}
