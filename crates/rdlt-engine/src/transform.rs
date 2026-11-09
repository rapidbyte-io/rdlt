//! Reshaping a stream's batches in flight, before the engine fits them to their table.

mod bound;
mod mask;
mod op;
pub(crate) mod stage;
#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{Schema, SchemaRef};

pub use self::bound::{Bound, allocated_beyond, column_bytes};
pub use self::mask::Mask;
pub use self::op::{Literal, Op};

/// A pure function of a batch that reshapes it, and may filter its rows, before the engine fits
/// it to its table.
///
/// A transform works row by row: what it makes of a row depends on that row alone, so the stage
/// may cut a batch and its parts give what the whole does. It reads no time or randomness and
/// keeps no state from one batch to the next, so a retry, a replan or a resume replays it
/// exactly. A transform that declares a [`Bound`] is held to it; one that declares none is
/// trusted code, its output measured after it runs, and trusted to pass the stream's keys,
/// change columns and change time on with their values, of which the stage checks only that
/// each passes with its name and type.
///
/// ```
/// use std::sync::Arc;
///
/// use arrow_array::RecordBatch;
/// use arrow_schema::{Schema, SchemaRef};
/// use rdlt_engine::{Transform, TransformError};
///
/// /// Passes every batch on as it is.
/// struct Unchanged;
///
/// impl Transform for Unchanged {
///     fn name(&self) -> &str {
///         "unchanged"
///     }
///
///     fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
///         Ok(Arc::new(input.clone()))
///     }
///
///     fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
///         Ok(batch)
///     }
/// }
///
/// assert_eq!(Unchanged.name(), "unchanged");
/// ```
pub trait Transform: Send + Sync + 'static {
    /// A short name for errors and `Debug`.
    fn name(&self) -> &str;

    /// The output schema for `input`, or why `input` is refused.
    ///
    /// # Errors
    ///
    /// A [`TransformError`] where the transform cannot take `input`.
    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError>;

    /// Whether the transform filters rows: only then is `keep` called.
    fn filters(&self) -> bool {
        false
    }

    /// Which rows to keep, one value a row; a null keeps nothing.
    ///
    /// # Errors
    ///
    /// A [`TransformError`] where the filter cannot be evaluated on `batch`.
    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        Ok(BooleanArray::new(
            BooleanBuffer::new_set(batch.num_rows()),
            None,
        ))
    }

    /// The batch reshaped: the same rows, in the same order, matching `output`.
    ///
    /// # Errors
    ///
    /// A [`TransformError`] where the batch cannot be reshaped.
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError>;

    /// A bound on what `keep` and `apply` allocate for batches of `input`, for transforms the
    /// budget charges before they run.
    fn bound(&self, _input: &Schema) -> Option<Bound> {
        None
    }

    /// For each output column of batches of `input`, the input columns, by ordinal, it is
    /// computed from: what lets a change batch's unchanged flags pass the transform.
    fn lineage(&self, _input: &Schema) -> Option<Vec<Vec<usize>>> {
        None
    }

    /// The input columns, by ordinal, that `keep` reads of batches of `input`: what lets a
    /// filter judge an update whose other columns the source left unchanged; none for any.
    fn keep_reads(&self, _input: &Schema) -> Option<Vec<usize>> {
        None
    }

    /// For each output column of batches of `input`, the input column, by ordinal, it passes on
    /// unchanged, where it does: what holds a declarative transform to the keys, change columns
    /// and change time it must pass as they arrive; none for trusted code, which is not held.
    fn passes(&self, _input: &Schema) -> Option<Vec<Option<usize>>> {
        None
    }
}

/// Why a transform refused an input schema or failed on a batch: text the engine shows, bounded,
/// and never trusts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransformError {
    message: String,
    column: Option<String>,
    cause: Option<Cause>,
}

impl TransformError {
    /// The error `message` describes.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            column: None,
            cause: None,
        }
    }

    /// The error, naming the column it is about.
    #[must_use]
    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        self.column = Some(column.into());
        self
    }

    /// The error, keeping `cause`, the text of what it came from, as its source.
    #[must_use]
    pub fn with_cause(mut self, cause: impl Into<String>) -> Self {
        self.cause = Some(Cause(cause.into()));
        self
    }

    /// What went wrong.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The column the error is about, if it names one.
    pub fn column(&self) -> Option<&str> {
        self.column.as_deref()
    }
}

impl fmt::Display for TransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.column {
            Some(column) => write!(f, "column {column}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for TransformError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(|cause| cause as _)
    }
}

/// What a [`TransformError`] came from, as text, so the error stays `Clone` and `Eq`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
struct Cause(String);

/// One step of a stream's transforms: an op, compared by value, or an embedder's transform,
/// compared by identity.
#[derive(Clone)]
pub enum TransformStep {
    /// A declarative op.
    Op(Op),
    /// An embedder's transform, equal only to itself.
    Code(Arc<dyn Transform>),
}

impl TransformStep {
    /// The step as the transform the engine runs.
    pub fn as_transform(&self) -> &dyn Transform {
        match self {
            Self::Op(op) => op,
            Self::Code(code) => code.as_ref(),
        }
    }
}

impl PartialEq for TransformStep {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Op(op), Self::Op(other)) => op == other,
            (Self::Code(code), Self::Code(other)) => Arc::ptr_eq(code, other),
            (Self::Op(_), Self::Code(_)) | (Self::Code(_), Self::Op(_)) => false,
        }
    }
}

impl Eq for TransformStep {}

impl fmt::Debug for TransformStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Op(op) => f.debug_tuple("Op").field(op).finish(),
            Self::Code(code) => f.debug_tuple("Code").field(&code.name()).finish(),
        }
    }
}

impl From<Op> for TransformStep {
    fn from(op: Op) -> Self {
        Self::Op(op)
    }
}

impl From<Arc<dyn Transform>> for TransformStep {
    fn from(code: Arc<dyn Transform>) -> Self {
        Self::Code(code)
    }
}
