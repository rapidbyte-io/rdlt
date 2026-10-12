//! Where a JSON text's values begin and end, found by its brackets, commas and quotes alone,
//! without parsing it, so the text is never read deeper than its structure.

#[cfg(test)]
mod tests;

use std::ops::Range;

/// One mark of a JSON text's structure outside its strings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mark {
    /// `[` or `{`, at its offset.
    Open(usize),
    /// `]` (true) or `}` (false), at its offset.
    Close(usize, bool),
    /// `,`, at its offset.
    Comma(usize),
    /// A string, from its opening quote to just past its closing one; a string that never
    /// closes runs to the end of the text.
    Text(Range<usize>),
}

/// The marks of a JSON text from an offset on, in order: each bracket and comma outside its
/// strings, and each string whole.
#[derive(Clone, Debug)]
pub struct Structure<'a> {
    text: &'a [u8],
    at: usize,
}

impl<'a> Structure<'a> {
    /// The marks of `text` from `at` on.
    #[must_use]
    pub fn new(text: &'a [u8], at: usize) -> Self {
        Self { text, at }
    }

    /// Where the walk stands: just past the last mark it gave.
    #[must_use]
    pub fn at(&self) -> usize {
        self.at
    }
}

impl Iterator for Structure<'_> {
    type Item = Mark;

    fn next(&mut self) -> Option<Mark> {
        let rest = self.text.get(self.at..)?;
        let found = rest
            .iter()
            .position(|byte| matches!(byte, b'"' | b'[' | b']' | b'{' | b'}' | b','))?;
        let at = self.at + found;
        self.at = at + 1;
        Some(match self.text[at] {
            b'"' => {
                self.at = string_end(self.text, at + 1);
                Mark::Text(at..self.at)
            }
            b'[' | b'{' => Mark::Open(at),
            b']' => Mark::Close(at, true),
            b'}' => Mark::Close(at, false),
            _ => Mark::Comma(at),
        })
    }
}

/// Just past the quote that closes the string whose contents start at `from`, or the text's end
/// where none does.
fn string_end(text: &[u8], mut from: usize) -> usize {
    while let Some(found) = text
        .get(from..)
        .and_then(|rest| memchr::memchr2(b'"', b'\\', rest))
    {
        let at = from + found;
        if text[at] == b'"' {
            return at + 1;
        }
        from = at + 2;
    }
    text.len()
}
