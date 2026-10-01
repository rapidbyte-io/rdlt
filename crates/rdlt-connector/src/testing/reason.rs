//! Reasons: the text a report gives for a clause that did not pass, bounded where it is made.

use std::fmt::{self, Write as _};

use super::limits::{REASON_BYTES, SHOWN_ROWS};

#[cfg(test)]
mod tests;

/// What ends a reason that was cut at its limit.
pub(super) const CUT: &str = " [cut]";

/// Why a clause failed, does not apply or was not observed: at most [`REASON_BYTES`] of text,
/// ending in a mark where more was cut.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Reason(String);

impl Reason {
    /// The reason `text` gives, cut at [`REASON_BYTES`]: no more of `text` is formatted than is
    /// kept.
    pub fn new(text: impl fmt::Display) -> Self {
        let mut clipped = Clipped {
            text: String::new(),
            room: REASON_BYTES - CUT.len(),
            cut: false,
        };
        // An error here is the cut, which the mark records.
        write!(clipped, "{text}").ok();
        if clipped.cut {
            clipped.text.push_str(CUT);
        }
        Self(clipped.text)
    }

    /// The reason's text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Reason {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for Reason {
    fn from(text: String) -> Self {
        Self::new(text)
    }
}

impl From<&str> for Reason {
    fn from(text: &str) -> Self {
        Self::new(text)
    }
}

/// Text that keeps what fits its room, whole characters only, and refuses the rest.
struct Clipped {
    text: String,
    room: usize,
    cut: bool,
}

impl fmt::Write for Clipped {
    fn write_str(&mut self, piece: &str) -> fmt::Result {
        if self.cut {
            return Err(fmt::Error);
        }
        if piece.len() <= self.room {
            self.text.push_str(piece);
            self.room -= piece.len();
            return Ok(());
        }
        let mut kept = self.room;
        while !piece.is_char_boundary(kept) {
            kept -= 1;
        }
        self.text.push_str(&piece[..kept]);
        self.room = 0;
        self.cut = true;
        Err(fmt::Error)
    }
}

/// Rows a connector sent, shown in a reason: how many there are, and the first [`SHOWN_ROWS`].
pub(super) struct Listed<'a, T>(pub(super) &'a [T]);

impl<T: fmt::Debug> fmt::Display for Listed<'_, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shown = &self.0[..self.0.len().min(SHOWN_ROWS)];
        write!(formatter, "{} rows", self.0.len())?;
        if shown.len() < self.0.len() {
            write!(formatter, ", the first {}", shown.len())?;
        }
        write!(formatter, ": {shown:?}")
    }
}
