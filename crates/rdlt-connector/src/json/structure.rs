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
    /// Just past the last mark it gave: where the next is looked for.
    at: usize,
}

impl<'a> Structure<'a> {
    /// The marks of `text` from `at` on.
    #[must_use]
    pub fn new(text: &'a [u8], at: usize) -> Self {
        Self { text, at }
    }
}

impl Iterator for Structure<'_> {
    type Item = Mark;

    fn next(&mut self) -> Option<Mark> {
        let rest = self.text.get(self.at..)?;
        let found = rest.iter().position(|byte| MARKS[usize::from(*byte)])?;
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

/// Whether a byte outside a string is a mark: a quote, a bracket or a comma.
const MARKS: [bool; 256] = {
    let mut marks = [false; 256];
    let mut at = 0;
    while at < 6 {
        marks[b"\"[]{},"[at] as usize] = true;
        at += 1;
    }
    marks
};

/// Just past the quote that closes the string whose contents start at `from`, or the text's end
/// where none does.
fn string_end(text: &[u8], mut from: usize) -> usize {
    while let Some(byte) = text.get(from) {
        match byte {
            b'"' => return from + 1,
            b'\\' => from += 2,
            _ => from += 1,
        }
    }
    text.len()
}
