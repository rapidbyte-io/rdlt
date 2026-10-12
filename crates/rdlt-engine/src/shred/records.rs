//! Where the records of JSON pushes lie, grouped into chunks, without a range a record.
//!
//! Pushes are cut into chunks without walking their records: lines at the first line end past a
//! chunk's bytes, found by jumping there; an array at the first top-level comma past them, in one
//! walk of its structure that checks it is an array and counts its elements. A chunk keeps only
//! the span of each push it covers, and its job finds its records again as it parses them.

#[cfg(test)]
mod tests;

use std::ops::Range;

use bytes::Bytes;
use rdlt_connector::json::{Mark, Structure};

use super::ShredError;

/// Whole records of some pushes: the span of each push they lie in.
pub(super) struct Chunk {
    parts: Vec<Part>,
}

/// Consecutive records of one push.
struct Part {
    push: Bytes,
    form: Form,
    /// From the first record's first byte to the last record's last.
    span: Range<usize>,
    /// Of an array, how many elements the span holds; lines are counted where they are parsed.
    elements: usize,
}

/// How a push separates its records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Form {
    /// Objects on their own lines; blank lines hold none.
    Lines,
    /// The elements of one JSON array.
    Elements,
}

impl Chunk {
    /// Bytes of its spans, whitespace between records included.
    pub(super) fn bytes(&self) -> usize {
        self.parts.iter().map(|part| part.span.len()).sum()
    }

    /// The most records it holds: of lines, its line ends and one for each part; of an array,
    /// its elements.
    pub(super) fn count(&self) -> usize {
        self.parts
            .iter()
            .map(|part| match part.form {
                Form::Lines => {
                    memchr::memchr_iter(b'\n', &part.push[part.span.clone()]).count() + 1
                }
                Form::Elements => part.elements,
            })
            .sum()
    }

    /// The records, in order; a span that breaks as its records are found again ends them with
    /// an internal error, since the cut checked the array it was cut from.
    pub(super) fn records(&self) -> impl Iterator<Item = Result<&[u8], ShredError>> {
        self.parts.iter().flat_map(|part| Records {
            bytes: &part.push[part.span.clone()],
            form: part.form,
            at: 0,
        })
    }
}

/// The records of `pushes`, in order, cut into chunks of about `chunk_bytes` each.
///
/// A chunk ends at the first line end or top-level comma at least `chunk_bytes` bytes past its
/// start, whitespace counted, or at the end of a push past them. A push is a JSON array of
/// objects, or objects on their own lines; each record is checked when it is parsed.
///
/// # Errors
///
/// [`ShredError::Invalid`] for a push that opens an array and is not one.
pub(super) fn chunks(pushes: &[Bytes], chunk_bytes: usize) -> Result<Vec<Chunk>, ShredError> {
    // Each cut moves past at least a byte.
    let chunk_bytes = chunk_bytes.max(1);
    let mut cut = Cut {
        chunks: Vec::new(),
        parts: Vec::new(),
        chunk_bytes,
        left: chunk_bytes,
    };
    for push in pushes {
        match push.iter().position(|byte| !json_whitespace(*byte)) {
            Some(first) if push[first] == b'[' => cut.elements(push, first)?,
            _ => cut.lines(push),
        }
    }
    cut.close();
    Ok(cut.chunks)
}

/// The chunks cut so far, and the parts of the chunk still open.
struct Cut {
    chunks: Vec<Chunk>,
    parts: Vec<Part>,
    chunk_bytes: usize,
    /// Bytes the open chunk takes before a record end closes it.
    left: usize,
}

impl Cut {
    /// Cuts the lines of `push` at the first line end each chunk's bytes reach, found by jumping
    /// past those bytes unread.
    fn lines(&mut self, push: &Bytes) {
        let mut at = 0;
        while push.len() - at >= self.left {
            let from = at + self.left;
            let end =
                memchr::memchr(b'\n', &push[from..]).map_or(push.len(), |found| from + found + 1);
            self.add(push, Form::Lines, at..end, 0);
            self.close();
            at = end;
        }
        self.add(push, Form::Lines, at..push.len(), 0);
    }

    /// Cuts the array `push` opens at `open` at the first top-level comma each chunk's bytes
    /// reach, in one walk that refuses a push that is not an array and counts its elements.
    fn elements(&mut self, push: &Bytes, open: usize) -> Result<(), ShredError> {
        let invalid =
            |what: &str| ShredError::Invalid(format!("the push is not a JSON array: {what}"));
        let mut structure = Structure::new(push, open + 1);
        // Where the open chunk's part starts, where the element starts, and the part's elements.
        let (mut from, mut start, mut elements) = (open + 1, open + 1, 0);
        loop {
            let (end, closed) = match element_end(&mut structure) {
                End::Comma(end) => (end, false),
                End::Close(end) => (end, true),
                End::Unbalanced => return Err(invalid("unbalanced brackets")),
                End::Unended => return Err(invalid("it does not end")),
            };
            let empty = blank(&push[start..end]);
            if closed {
                // Only an array of no elements ends without one, and nothing follows the array.
                if empty && start > open + 1 {
                    return Err(invalid("an empty element"));
                }
                if !blank(&push[end + 1..]) {
                    return Err(invalid("more follows it"));
                }
                let full = end - from >= self.left;
                self.add(
                    push,
                    Form::Elements,
                    from..end,
                    elements + usize::from(!empty),
                );
                if full {
                    self.close();
                }
                return Ok(());
            }
            if empty {
                return Err(invalid("an empty element"));
            }
            elements += 1;
            if end - from >= self.left {
                self.add(push, Form::Elements, from..end, elements);
                self.close();
                (from, elements) = (end + 1, 0);
            }
            start = end + 1;
        }
    }

    /// Adds the records of `push` in `span`, without the whitespace around them, to the open
    /// chunk.
    fn add(&mut self, push: &Bytes, form: Form, span: Range<usize>, elements: usize) {
        self.left = self.left.saturating_sub(span.len());
        if let Some(span) = trimmed(push, span) {
            self.parts.push(Part {
                push: push.clone(),
                form,
                span,
                elements,
            });
        }
    }

    /// Closes the open chunk, keeping it where it holds a record.
    fn close(&mut self) {
        if !self.parts.is_empty() {
            let parts = std::mem::take(&mut self.parts);
            self.chunks.push(Chunk { parts });
        }
        self.left = self.chunk_bytes;
    }
}

/// Where an element of an array ends.
enum End {
    /// At a top-level comma, at its offset.
    Comma(usize),
    /// At the `]` that closes the array, at its offset.
    Close(usize),
    /// At a bracket that closes what the element never opened.
    Unbalanced,
    /// Nowhere: the text ends first.
    Unended,
}

/// Where the element `structure` stands at the start of ends, found without recursing however
/// deep it nests.
fn element_end(structure: &mut Structure<'_>) -> End {
    let mut depth = 0_usize;
    for mark in structure {
        match mark {
            Mark::Open(_) => depth += 1,
            Mark::Comma(at) if depth == 0 => return End::Comma(at),
            Mark::Close(at, true) if depth == 0 => return End::Close(at),
            Mark::Close(..) if depth == 0 => return End::Unbalanced,
            Mark::Close(..) => depth -= 1,
            Mark::Comma(_) | Mark::Text(_) => {}
        }
    }
    End::Unended
}

/// The records of a span a cut made from its first record's first byte to its last record's
/// last, found one at a time, keeping only where it stands; a record that does not read ends
/// them with an internal error.
struct Records<'a> {
    bytes: &'a [u8],
    form: Form,
    /// Where the next record is looked for.
    at: usize,
}

impl<'a> Iterator for Records<'a> {
    type Item = Result<&'a [u8], ShredError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = match self.form {
            Form::Lines => Ok(self.line()),
            Form::Elements => self.element(),
        };
        match next {
            Ok(record) => record.map(|range| Ok(&self.bytes[range])),
            Err(error) => {
                // Past the bytes, so nothing is read after the record that broke.
                self.at = self.bytes.len() + 1;
                Some(Err(error))
            }
        }
    }
}

impl Records<'_> {
    /// The next line that is not blank.
    fn line(&mut self) -> Option<Range<usize>> {
        while let Some(rest) = self.bytes.get(self.at..).filter(|rest| !rest.is_empty()) {
            let end = memchr::memchr(b'\n', rest).map_or(self.bytes.len(), |end| self.at + end);
            let line = trimmed(self.bytes, self.at..end);
            self.at = end + 1;
            if line.is_some() {
                return line;
            }
        }
        None
    }

    /// The next element, up to a top-level comma or the end of the span.
    fn element(&mut self) -> Result<Option<Range<usize>>, ShredError> {
        let broken = |what: &str| {
            ShredError::Internal(format!("a span the cut made does not read again: {what}"))
        };
        if self.at > self.bytes.len() {
            return Ok(None);
        }
        let start = self.at;
        let end = match element_end(&mut Structure::new(self.bytes, start)) {
            End::Comma(end) => end,
            End::Unended => self.bytes.len(),
            End::Close(_) | End::Unbalanced => return Err(broken("unbalanced brackets")),
        };
        self.at = end + 1;
        // A span begins and ends at a record, so an element with nothing in it is broken.
        let Some(element) = trimmed(self.bytes, start..end) else {
            return Err(broken("an empty element"));
        };
        Ok(Some(element))
    }
}

/// `range` of `bytes` without the JSON whitespace around it, unless nothing is left.
fn trimmed(bytes: &[u8], range: Range<usize>) -> Option<Range<usize>> {
    let within = bytes.get(range.clone())?;
    let start = within.iter().position(|byte| !json_whitespace(*byte))?;
    let end = within
        .iter()
        .rposition(|byte| !json_whitespace(*byte))
        .map_or(start, |last| last + 1);
    Some(range.start + start..range.start + end)
}

/// Whether `bytes` hold only JSON whitespace.
fn blank(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| json_whitespace(*byte))
}

/// Whether `byte` is whitespace in JSON: a space, tab, line feed or carriage return.
fn json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}
