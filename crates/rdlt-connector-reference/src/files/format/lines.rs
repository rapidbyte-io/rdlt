//! Lines of a file read one at a time, none held beyond its limit.

use std::io::{self, BufRead, Write};

use crate::rooted::Refusal;

/// What a line limit is called where one refuses a line.
pub(crate) const LINE_LIMIT: &str = "line bytes";

/// The lines of a reader, each at most a limit of bytes, its line ending apart.
#[derive(Debug)]
pub(crate) struct Lines<R> {
    reader: R,
    limit: u64,
}

impl<R: BufRead> Lines<R> {
    /// The lines of `reader`, each at most `limit` bytes.
    pub(crate) fn new(reader: R, limit: u64) -> Self {
        Self { reader, limit }
    }

    /// Reads the next line into `line`, emptied first, with its ending; `false` at the end.
    ///
    /// # Errors
    ///
    /// A [`Refusal::TooLarge`] for a line beyond the limit, refused once one byte beyond the
    /// limit was seen: never more of it is held.
    pub(crate) fn next(&mut self, line: &mut Vec<u8>) -> io::Result<bool> {
        line.clear();
        // A line of the limit may still be followed by a carriage return before its end.
        let most = usize::try_from(self.limit)
            .unwrap_or(usize::MAX)
            .saturating_add(1);
        loop {
            let buffered = self.reader.fill_buf()?;
            if buffered.is_empty() {
                break;
            }
            let end = buffered.iter().position(|byte| *byte == b'\n');
            let take = end.map_or(buffered.len(), |end| end + 1);
            let room = most.saturating_add(1).saturating_sub(line.len());
            line.extend_from_slice(&buffered[..take.min(room)]);
            self.reader.consume(take.min(room));
            // The line's end was taken, or more than a line may hold.
            if line.ends_with(b"\n") || line.len() > most {
                break;
            }
        }
        if content(line).len() > most - 1 {
            return Err(Refusal::TooLarge {
                name: LINE_LIMIT,
                limit: self.limit,
                actual: self.limit.saturating_add(1),
            }
            .into());
        }
        Ok(!line.is_empty())
    }
}

/// `line` without its ending: a line feed, and a carriage return before it.
pub(crate) fn content(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// Whether `line` holds a record: anything but white space.
pub(crate) fn holds_a_record(line: &[u8]) -> bool {
    !line.trim_ascii().is_empty()
}

/// A writer that refuses a line beyond a limit, so no line is written that [`Lines`] would not
/// read back.
#[derive(Debug)]
pub(crate) struct Bounded<W> {
    inner: W,
    limit: u64,
    /// Bytes of the line being written.
    line: u64,
    /// Bytes written.
    pub(crate) written: u64,
}

impl<W: Write> Bounded<W> {
    /// A writer into `inner` of lines of at most `limit` bytes each.
    pub(crate) fn new(inner: W, limit: u64) -> Self {
        Self {
            inner,
            limit,
            line: 0,
            written: 0,
        }
    }

    /// The writer written into.
    pub(crate) fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for Bounded<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for part in bytes.split_inclusive(|byte| *byte == b'\n') {
            let ended = part.ends_with(b"\n");
            let length = u64::try_from(part.len() - usize::from(ended)).unwrap_or(u64::MAX);
            self.line = self.line.saturating_add(length);
            if self.line > self.limit {
                return Err(Refusal::TooLarge {
                    name: LINE_LIMIT,
                    limit: self.limit,
                    actual: self.line,
                }
                .into());
            }
            if ended {
                self.line = 0;
            }
        }
        self.inner.write_all(bytes)?;
        self.written = self
            .written
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
