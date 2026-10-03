//! Why a JSON push cannot be shredded, and the code each refusal carries.

use crate::limits::MAX_CELLS;

/// Why a JSON push cannot be shredded.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ShredError {
    /// The push is not JSON.
    #[error("the push is not valid JSON: {0}")]
    Invalid(String),
    /// A record is not an object.
    #[error("a record is not a JSON object")]
    NotObject,
    /// A value nests deeper than the limit.
    #[error(
        "a value nests deeper than {} levels",
        rdlt_connector::limits::MAX_NESTING_DEPTH
    )]
    TooDeep,
    /// An object repeats a key, which is shown cut to a limit.
    #[error("an object repeats the key {0:?}")]
    DuplicateKey(String),
    /// The records hold more columns than the limit, the second.
    #[error("the records hold {0} columns or more, over the limit of {1}")]
    TooManyColumns(u64, u64),
    /// The records would shred into more cells than the limit.
    #[error("the records would shred into {0} cells, over the limit of {MAX_CELLS}")]
    TooManyCells(u64),
    /// A number's exponent, shown cut to a limit, has more digits than its value's canonical
    /// text holds.
    #[error(
        "the number {0} has an exponent of more than {digits} digits",
        digits = crate::json::EXPONENT_DIGITS
    )]
    Exponent(String),
    /// The records' columns take more to observe than their chunks' text pays for, beyond what
    /// a flush may hold past it, the bytes.
    #[error(
        "the records' columns take more to observe than their text pays for, by more than {0} \
         bytes"
    )]
    ColumnsBeyondText(u64),
    /// A list holds more items than a column can.
    #[error("a list column holds more items than one batch can")]
    TooLarge,
    /// A bug in the shredder.
    #[error("shredding: {0}")]
    Internal(String),
}

impl ShredError {
    /// The machine code of the error.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "json_invalid",
            Self::NotObject => "json_not_object",
            Self::TooDeep
            | Self::Exponent(_)
            | Self::ColumnsBeyondText(_)
            | Self::TooManyColumns(..)
            | Self::TooManyCells(_)
            | Self::TooLarge => "limit_exceeded",
            Self::DuplicateKey(_) => "json_duplicate_key",
            Self::Internal(_) => "shred_internal",
        }
    }
}
