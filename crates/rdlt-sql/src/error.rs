//! Why a SQL transform's text is refused.

#[cfg(test)]
mod tests;

use rdlt_engine::{Code, codes};

/// What a [`SqlError`] refuses.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlErrorKind {
    /// The text is longer than [`SQL_BYTES`](crate::SQL_BYTES).
    TooLong,
    /// The text is not one statement, nests deeper than [`SQL_DEPTH`](crate::SQL_DEPTH), or
    /// nests a `CASE` within more conditions than
    /// [`SQL_NESTED_CONDITIONS`](crate::SQL_NESTED_CONDITIONS), or a condition deeper than
    /// [`SQL_CONDITION_DEPTH`](crate::SQL_CONDITION_DEPTH).
    Syntax,
    /// The query is not a projection of `input`, optionally filtered.
    Shape,
    /// The query calls a function the list lacks, or calls one with a clause or arguments the
    /// list does not bound.
    Function,
    /// A literal is longer than [`SQL_LITERAL_BYTES`](crate::SQL_LITERAL_BYTES).
    Literal,
    /// A regular expression or a `LIKE` pattern is not a literal, or a regular expression is not
    /// one or compiles to more than [`REGEX_BYTES`](crate::REGEX_BYTES).
    Regex,
    /// An output column that is no bare column has no name.
    Unnamed,
    /// Parsing the text panicked in a library it was parsed by, a bug there.
    Failed,
}

/// A SQL transform's text refused, naming what in it is refused and never a value it holds.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct SqlError {
    kind: SqlErrorKind,
    message: String,
}

impl SqlError {
    pub(crate) fn new(kind: SqlErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// What is refused.
    pub fn kind(&self) -> SqlErrorKind {
        self.kind
    }

    /// The engine's code for the refusal: [`codes::TRANSFORM_INVALID`].
    pub fn code(&self) -> Code {
        codes::TRANSFORM_INVALID
    }
}
