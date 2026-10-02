//! Text a connector sent, as a host keeps and shows it: nothing in it is obeyed by a terminal
//! or hidden from a reader, and it has a bounded length.

use std::fmt::{self, Write as _};

#[cfg(test)]
mod tests;

/// What ends a text that was cut at its limit.
pub const CUT: &str = " [cut]";

/// `text` as a reader may be shown it, in `bytes` of UTF-8 at most.
///
/// Every character that acts on a terminal, reorders or hides what is around it, or shows as
/// nothing is written as an escape that names it, `\u{202e}`; a run of spaces is one space;
/// and a text that does not fit ends in [`CUT`]. Showing what was shown changes nothing, so
/// every place that receives a connector's text may apply it. No more of `text` is formatted
/// than is kept.
pub fn shown(text: impl fmt::Display, bytes: usize) -> String {
    let mut showing = Showing {
        text: String::new(),
        bytes,
        fits_mark: 0,
        spaced: false,
        cut: false,
    };
    // An error here is the cut, which the mark records.
    write!(showing, "{text}").ok();
    if showing.cut {
        showing.text.truncate(showing.fits_mark);
        let kept = showing.text.trim_end_matches(' ').len();
        showing.text.truncate(kept);
        showing.text.push_str(CUT);
    }
    showing.text
}

/// Text being shown: what is kept of it so far.
struct Showing {
    text: String,
    /// Bytes the text may take.
    bytes: usize,
    /// How much of the text leaves room for the mark of a cut.
    fits_mark: usize,
    /// Whether the last character kept is a space.
    spaced: bool,
    cut: bool,
}

impl Showing {
    /// Keeps `piece` whole, or cuts the text.
    fn keep(&mut self, piece: &str) -> fmt::Result {
        if self.text.len() + piece.len() > self.bytes {
            self.cut = true;
            return Err(fmt::Error);
        }
        self.text.push_str(piece);
        if self.text.len() + CUT.len() <= self.bytes {
            self.fits_mark = self.text.len();
        }
        Ok(())
    }
}

impl fmt::Write for Showing {
    fn write_str(&mut self, piece: &str) -> fmt::Result {
        if self.cut {
            return Err(fmt::Error);
        }
        let mut buffer = [0; 4];
        for c in piece.chars() {
            if c == ' ' {
                if !self.spaced {
                    self.keep(" ")?;
                }
                self.spaced = true;
                continue;
            }
            self.spaced = false;
            if deceives(c) {
                let mut escape = String::new();
                escape.extend(c.escape_default());
                self.keep(&escape)?;
            } else {
                self.keep(c.encode_utf8(&mut buffer))?;
            }
        }
        Ok(())
    }
}

/// Whether `c` could deceive a reader shown it raw: a control, a format character, a
/// separator of lines, a space that is not U+0020, a character that shows as nothing, or
/// one with no meaning of its own.
///
/// Identifiers refuse such characters, so two that read alike are alike.
pub fn deceives(c: char) -> bool {
    c.is_control() || blank(c) || format(c) || ignorable(c) || unmeaning(c)
}

/// The separators of lines and paragraphs, the spaces other than U+0020, and the letters
/// that show as blank.
fn blank(c: char) -> bool {
    matches!(
        c,
        '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{2800}'
                | '\u{3000}'
    )
}

/// The format characters: the marks, embeddings, overrides and isolates that reorder text,
/// the joiners and spaces of no width, and the marks that shape or annotate what is around
/// them.
fn format(c: char) -> bool {
    matches!(
        c,
        '\u{ad}'
            | '\u{600}'..='\u{605}'
            | '\u{61c}'
            | '\u{6dd}'
            | '\u{70f}'
            | '\u{890}'..='\u{891}'
            | '\u{8e2}'
            | '\u{180e}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{206f}'
            | '\u{feff}'
            | '\u{fff9}'..='\u{fffb}'
            | '\u{110bd}'
            | '\u{110cd}'
            | '\u{13430}'..='\u{1343f}'
            | '\u{1bca0}'..='\u{1bca3}'
            | '\u{1d173}'..='\u{1d17a}'
    )
}

/// The characters a renderer ignores by default: fillers, variation selectors and tags.
fn ignorable(c: char) -> bool {
    matches!(
        c,
        '\u{34f}'
            | '\u{115f}'..='\u{1160}'
            | '\u{17b4}'..='\u{17b5}'
            | '\u{180b}'..='\u{180f}'
            | '\u{3164}'
            | '\u{fe00}'..='\u{fe0f}'
            | '\u{ffa0}'
            | '\u{fff0}'..='\u{fff8}'
            | '\u{e0000}'..='\u{e0fff}'
    )
}

/// The characters with no meaning a reader shares: those of private use, and those that
/// are no character.
fn unmeaning(c: char) -> bool {
    matches!(
        c,
        '\u{e000}'..='\u{f8ff}'
            | '\u{fdd0}'..='\u{fdef}'
            | '\u{f0000}'..='\u{10ffff}'
    ) || u32::from(c) & 0xfffe == 0xfffe
}
