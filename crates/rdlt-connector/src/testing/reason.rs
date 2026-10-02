//! Reasons: the text a report gives for a clause that did not pass, bounded where it is made.

use std::fmt;

use super::limits::{REASON_BYTES, SHOWN_ROWS};

#[cfg(test)]
mod tests;

/// Why a clause failed, does not apply or was not observed: at most [`REASON_BYTES`] of text
/// shown as [`text::shown`](crate::text::shown) shows it, whatever a connector put in it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Reason(String);

impl Reason {
    /// The reason `text` gives, cut at [`REASON_BYTES`]: no more of `text` is formatted than is
    /// kept.
    pub fn new(text: impl fmt::Display) -> Self {
        Self(crate::text::shown(text, REASON_BYTES))
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
