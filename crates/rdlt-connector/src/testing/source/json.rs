//! The records of a JSON push, found without parsing the push whole.
//!
//! A push is the rows of its arrays, and each value beside an array a row of its own. Its
//! records are found by their brackets and quotes alone, which builds nothing, so a push is
//! charged for its rows before any is parsed; each record is then parsed by itself.

#[cfg(test)]
mod tests;

use std::ops::Range;

use bytes::Bytes;

use crate::testing::Violation;
use crate::testing::limits::{RECORD_BYTES, YIELD_BYTES};

/// The spans of the records of `text`, in order.
pub(in crate::testing) struct Records<'a> {
    text: &'a [u8],
    at: usize,
    /// Whether the scan is inside an array of rows.
    rows: bool,
}

impl<'a> Records<'a> {
    pub(in crate::testing) fn new(text: &'a [u8]) -> Self {
        Self {
            text,
            at: 0,
            rows: false,
        }
    }

    /// The end of the record that begins at `self.at`: of a container, where its brackets
    /// close; of a string, its closing quote; of anything else, the next separator.
    fn end(&self) -> usize {
        let text = &self.text[self.at..];
        let (mut depth, mut quoted, mut escaped) = (0_usize, false, false);
        for (index, byte) in text.iter().enumerate() {
            match byte {
                _ if escaped => escaped = false,
                b'\\' if quoted => escaped = true,
                b'"' => {
                    quoted = !quoted;
                    if !quoted && depth == 0 {
                        return self.at + index + 1;
                    }
                }
                _ if quoted => {}
                b'{' | b'[' => depth += 1,
                b'}' | b']' if depth > 0 => {
                    depth -= 1;
                    if depth == 0 {
                        return self.at + index + 1;
                    }
                }
                b'}' | b']' | b',' | b' ' | b'\t' | b'\n' | b'\r' if depth == 0 => {
                    return self.at + index;
                }
                _ => {}
            }
        }
        self.text.len()
    }
}

impl Iterator for Records<'_> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Range<usize>> {
        loop {
            let byte = *self.text.get(self.at)?;
            match byte {
                b' ' | b'\t' | b'\n' | b'\r' | b',' => self.at += 1,
                b'[' if !self.rows => {
                    self.rows = true;
                    self.at += 1;
                }
                b']' | b'}' => {
                    self.rows = false;
                    self.at += 1;
                }
                _ => {
                    let record = self.at..self.end();
                    self.at = record.end;
                    return Some(record);
                }
            }
        }
    }
}

/// How many records `text` holds, each within [`RECORD_BYTES`]; a violation, of a clause not
/// observed, for a push holding a longer one.
///
/// It yields to the runtime every [`YIELD_BYTES`], so what bounds the caller can end it.
pub(in crate::testing) async fn counted(text: &[u8]) -> Result<usize, Violation> {
    let (mut records, mut yielded) = (0_usize, 0_usize);
    for record in Records::new(text) {
        if record.len() > RECORD_BYTES {
            return Err(Violation::unobserved(format_args!(
                "the source pushes a JSON record of {} bytes, more than the {RECORD_BYTES} a \
                 clause parses: certify it with smaller records",
                record.len()
            )));
        }
        records += 1;
        if record.end - yielded >= YIELD_BYTES {
            yielded = record.end;
            tokio::task::yield_now().await;
        }
    }
    Ok(records)
}

/// `text` as one array of its records that parse, each written as its value alone: equal rows
/// are then equal bytes, whatever their formatting.
///
/// Each record is parsed by itself, so no more than one is ever held as a tree, and it yields
/// to the runtime every [`YIELD_BYTES`].
pub(in crate::testing) async fn canonical(text: &[u8]) -> Bytes {
    let mut rows = Vec::with_capacity(text.len().saturating_add(2));
    rows.push(b'[');
    let mut yielded = 0;
    for record in Records::new(text) {
        let end = record.end;
        if let Ok(row) = serde_json::from_slice::<serde_json::Value>(&text[record]) {
            if rows.len() > 1 {
                rows.push(b',');
            }
            // Writing a value into memory does not fail.
            serde_json::to_writer(&mut rows, &row).ok();
        }
        if end - yielded >= YIELD_BYTES {
            yielded = end;
            tokio::task::yield_now().await;
        }
    }
    rows.push(b']');
    Bytes::from(rows)
}
