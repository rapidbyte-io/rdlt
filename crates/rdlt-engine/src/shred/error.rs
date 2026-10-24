//! Why a JSON push cannot be shredded, the code each refusal carries, and a refused record's
//! number among the pushes.

use crate::error::{Code, codes};
use crate::limits::MAX_CELLS;

/// Why a JSON push cannot be shredded.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ShredError {
    /// The push is not JSON.
    #[error("the push is not valid JSON: {0}")]
    Invalid(String),
    /// A record is not JSON: what broke, without quoting the record.
    #[error("the push is not valid JSON: record {}: {what}", index + 1)]
    Record {
        /// The record's place among the pushes, from zero.
        index: usize,
        /// What broke and where in the record.
        what: String,
    },
    /// A record is not an object.
    #[error("a record is not a JSON object")]
    NotObject,
    /// A value nests deeper than the limit.
    #[error(
        "a value nests deeper than {} levels",
        rdlt_wire::limits::NESTING_DEPTH
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
    /// The engine's code for the error.
    pub(crate) fn code(&self) -> Code {
        match self {
            Self::Invalid(_) | Self::Record { .. } => codes::JSON_INVALID,
            Self::NotObject => codes::JSON_NOT_OBJECT,
            Self::TooDeep
            | Self::Exponent(_)
            | Self::ColumnsBeyondText(_)
            | Self::TooManyColumns(..)
            | Self::TooManyCells(_)
            | Self::TooLarge => codes::LIMIT_EXCEEDED,
            Self::DuplicateKey(_) => codes::JSON_DUPLICATE_KEY,
            Self::Internal(_) => codes::SHRED_INTERNAL,
        }
    }
}

/// The results of jobs over consecutive chunks, in order, or the first error, a refused record
/// numbered among the pushes by the `rows` of the results before it.
pub(super) fn numbered<T>(
    results: impl IntoIterator<Item = Result<T, ShredError>>,
    rows: impl Fn(&T) -> usize,
) -> Result<Vec<T>, ShredError> {
    let mut before = 0_usize;
    results
        .into_iter()
        .map(|result| match result {
            Ok(done) => {
                before = before.saturating_add(rows(&done));
                Ok(done)
            }
            Err(ShredError::Record { index, what }) => Err(ShredError::Record {
                index: index.saturating_add(before),
                what,
            }),
            Err(error) => Err(error),
        })
        .collect()
}
