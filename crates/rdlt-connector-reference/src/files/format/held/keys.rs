//! The keys of a line's record, found without reading its values.
//!
//! A line is passed over once, byte by byte, keeping count of how deep in brackets it is and
//! whether it is inside a string. Nothing is decoded but the keys, and nothing nests a call, so
//! a record nested however deep is passed over as its decoder then reads it.

use std::borrow::Cow;

use arrow_schema::ArrowError;

/// Where a pass over a record stands.
#[derive(Default)]
struct Pass<'l> {
    /// Brackets open.
    depth: usize,
    /// Where the string being passed over starts, and whether its next byte is escaped.
    string: Option<(usize, bool)>,
    /// The record's key whose value has not started.
    key: Option<&'l [u8]>,
    /// Whether a key's colon was passed and its value has not started.
    valued: bool,
    /// Whether the record has ended.
    ended: bool,
    keys: Vec<&'l [u8]>,
}

/// The keys of the record `line` holds under which it holds a value that is not null, as they
/// are written between their quotes.
///
/// # Errors
///
/// A line that does not start a record or does not end it, as its decoder would refuse it; and
/// as a parse error, apart from those, a line that holds anything after its record: one line is
/// one row, as every writer of these files writes it.
pub(super) fn valued(line: &[u8]) -> Result<Vec<Cow<'_, str>>, ArrowError> {
    let mut pass = Pass::default();
    for (at, byte) in line.iter().enumerate() {
        pass.step(line, at, *byte)?;
    }
    if !pass.ended {
        return Err(refused("its record does not end"));
    }
    pass.keys.into_iter().map(text).collect()
}

impl<'l> Pass<'l> {
    fn step(&mut self, line: &'l [u8], at: usize, byte: u8) -> Result<(), ArrowError> {
        if let Some((start, escaped)) = self.string {
            match byte {
                _ if escaped => self.string = Some((start, false)),
                b'\\' => self.string = Some((start, true)),
                b'"' => {
                    self.string = None;
                    if self.depth == 1 && !self.valued {
                        self.key = Some(&line[start + 1..at]);
                    }
                }
                _ => {}
            }
            return Ok(());
        }
        if byte.is_ascii_whitespace() {
            return Ok(());
        }
        if self.ended {
            let message = "a line holds more than one record".to_owned();
            return Err(ArrowError::ParseError(message));
        }
        match (self.depth, byte) {
            (0, open) if open != b'{' => return Err(refused("it starts no record")),
            (1, b':') => self.valued = true,
            (1, b',') => self.valued = false,
            (_, b'}' | b']') => {
                self.depth -= 1;
                self.ended = self.depth == 0;
            }
            (depth, start) => {
                // A value starts: under a key of the record, it is the key's.
                if let (1, true, Some(key)) = (depth, self.valued, self.key.take())
                    && start != b'n'
                {
                    self.keys.push(key);
                }
                match start {
                    b'{' | b'[' => self.depth += 1,
                    b'"' => self.string = Some((at, false)),
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

/// A key as the text it names: its escapes read, where it is written with any.
fn text(key: &[u8]) -> Result<Cow<'_, str>, ArrowError> {
    let unread = || refused("a key is no text");
    if !key.contains(&b'\\') {
        return std::str::from_utf8(key)
            .map(Cow::Borrowed)
            .map_err(|_| unread());
    }
    let mut quoted = Vec::with_capacity(key.len() + 2);
    quoted.push(b'"');
    quoted.extend_from_slice(key);
    quoted.push(b'"');
    serde_json::from_slice::<String>(&quoted)
        .map(Cow::Owned)
        .map_err(|_| unread())
}

/// The refusal of a line that is no record.
fn refused(why: &str) -> ArrowError {
    ArrowError::JsonError(format!("a line is no record: {why}"))
}
