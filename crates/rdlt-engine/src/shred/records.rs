//! Where the records of JSON pushes lie, grouped into chunks, without a range a record.
//!
//! A push is scanned once to find where its chunks end and how many records each holds; a chunk
//! keeps only the span of each push it covers, and finds its records again as it is parsed. A
//! push of very many very small records would otherwise take several times its bytes in ranges.

#[cfg(test)]
mod tests;

use std::ops::Range;

use bytes::Bytes;

use super::ShredError;

/// Whole records of some pushes: the span of each push they lie in.
pub(super) struct Chunk {
    parts: Vec<Part>,
    /// How many records the chunk holds.
    pub(super) rows: usize,
    /// How many records of the pushes come before the chunk's first.
    pub(super) before: usize,
    /// Bytes of the records, the whitespace around them left out.
    pub(super) bytes: usize,
}

/// Consecutive records of one push.
struct Part {
    push: Bytes,
    form: Form,
    /// From the first record's first byte to the last record's last.
    span: Range<usize>,
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
    /// The records, in order.
    pub(super) fn records(&self) -> impl Iterator<Item = &[u8]> {
        self.parts.iter().flat_map(|part| {
            let bytes = &part.push[part.span.clone()];
            Records::within(bytes, part.form).map(|record| &bytes[record])
        })
    }
}

/// The records of `pushes`, in order, grouped into chunks of about `chunk_bytes` each.
///
/// A push is a JSON array of objects, or objects on their own lines; each record is checked
/// when it is parsed.
///
/// # Errors
///
/// [`ShredError::Invalid`] for a push that opens an array and is not one.
pub(super) fn chunks(pushes: &[Bytes], chunk_bytes: usize) -> Result<Vec<Chunk>, ShredError> {
    let mut chunks = Vec::new();
    let mut chunk = Chunk::after(0);
    let mut size = 0;
    for push in pushes {
        let mut records = Records::of(push);
        let form = records.form;
        let mut span: Option<Range<usize>> = None;
        while let Some(record) = records.next_record()? {
            size += record.len();
            chunk.bytes += record.len();
            chunk.rows += 1;
            span = Some(span.map_or(record.start, |span| span.start)..record.end);
            if size >= chunk_bytes {
                chunk.part(push, form, span.take());
                let next = Chunk::after(chunk.before + chunk.rows);
                chunks.push(std::mem::replace(&mut chunk, next));
                size = 0;
            }
        }
        chunk.part(push, form, span);
    }
    if chunk.rows > 0 {
        chunks.push(chunk);
    }
    Ok(chunks)
}

impl Chunk {
    /// An empty chunk `before` records into the pushes.
    fn after(before: usize) -> Self {
        Self {
            parts: Vec::new(),
            rows: 0,
            before,
            bytes: 0,
        }
    }

    /// Adds the records of `push` in `span`, where there are any.
    fn part(&mut self, push: &Bytes, form: Form, span: Option<Range<usize>>) {
        if let Some(span) = span {
            self.parts.push(Part {
                push: push.clone(),
                form,
                span,
            });
        }
    }
}

/// Finds the records of some bytes one at a time, keeping only where it stands.
struct Records<'a> {
    bytes: &'a [u8],
    form: Form,
    /// Where the next record is looked for.
    at: usize,
    /// Whether the bytes are a whole push, whose array must close, or records cut from one.
    whole: bool,
    /// How many elements of an array were found.
    found: usize,
}

impl<'a> Records<'a> {
    /// The records of `push`, a whole push.
    fn of(push: &'a [u8]) -> Self {
        let first = push.iter().position(|byte| !json_whitespace(*byte));
        let array = first.filter(|first| push[*first] == b'[');
        Self {
            bytes: push,
            form: if array.is_some() {
                Form::Elements
            } else {
                Form::Lines
            },
            at: array.map_or(0, |open| open + 1),
            whole: true,
            found: 0,
        }
    }

    /// The records in `bytes`, which a scan of their push cut from its first record's first byte
    /// to its last record's last.
    fn within(bytes: &'a [u8], form: Form) -> impl Iterator<Item = Range<usize>> + 'a {
        let mut records = Self {
            bytes,
            form,
            at: 0,
            whole: false,
            found: 0,
        };
        // The scan that cut the bytes found every record in them.
        std::iter::from_fn(move || records.next_record().ok().flatten())
    }

    /// Where the next record lies, without the whitespace around it; `None` after the last.
    fn next_record(&mut self) -> Result<Option<Range<usize>>, ShredError> {
        match self.form {
            Form::Lines => Ok(self.line()),
            Form::Elements => self.element(),
        }
    }

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

    /// The next element of the array, found without recursing however deep it nests.
    fn element(&mut self) -> Result<Option<Range<usize>>, ShredError> {
        let invalid =
            |what: &str| ShredError::Invalid(format!("the push is not a JSON array: {what}"));
        if self.at > self.bytes.len() {
            return Ok(None);
        }
        let start = self.at;
        let (mut depth, mut in_string, mut escaped) = (0_usize, false, false);
        let mut end = None;
        for (index, &byte) in self.bytes.iter().enumerate().skip(start) {
            if in_string {
                match byte {
                    _ if escaped => escaped = false,
                    b'\\' => escaped = true,
                    b'"' => in_string = false,
                    _ => {}
                }
                continue;
            }
            match byte {
                b'"' => in_string = true,
                b'[' | b'{' => depth += 1,
                b']' if depth == 0 => {
                    end = Some((index, true));
                    break;
                }
                b']' | b'}' => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or_else(|| invalid("unbalanced brackets"))?;
                }
                b',' if depth == 0 => {
                    end = Some((index, false));
                    break;
                }
                _ => {}
            }
        }
        let (end, closed) = match end {
            Some(end) => end,
            None if self.whole => return Err(invalid("it does not end")),
            None => (self.bytes.len(), true),
        };
        let element = trimmed(self.bytes, start..end);
        if closed {
            // Only an array of no elements ends without one, and nothing follows the array.
            self.at = self.bytes.len() + 1;
            if element.is_none() && self.found > 0 {
                return Err(invalid("an empty element"));
            }
            if self.whole && trimmed(self.bytes, end + 1..self.bytes.len()).is_some() {
                return Err(invalid("more follows it"));
            }
        } else {
            self.at = end + 1;
            if element.is_none() {
                return Err(invalid("an empty element"));
            }
        }
        self.found += 1;
        Ok(element)
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

/// Whether `byte` is whitespace in JSON: a space, tab, line feed or carriage return.
fn json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}
