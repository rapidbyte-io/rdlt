//! Why the stage refused a schema or a batch, and the error that fails its stream.

use std::fmt;

use rdlt_connector::StreamName;

use crate::error::{Code, Error, ErrorKind, codes};
use crate::transform::TransformError;

/// Why the stage refused a schema or a batch.
#[derive(Debug)]
pub(crate) enum Refusal {
    /// A transform refused its input's schema.
    Schema {
        transform: String,
        error: TransformError,
    },
    /// The transforms do not pass a column the stream keys or times by on as it arrives.
    Kept { column: String },
    /// A transform declaring what it passes makes a column under the name of one the stream
    /// keys or times by, or of a change column, rather than pass that column as it arrives.
    Claimed { transform: String, column: String },
    /// An op names a column the stream's declared schema lacks as the op meets it.
    Absent { transform: String, column: String },
    /// A transform failed on a batch.
    Failed {
        transform: String,
        error: TransformError,
    },
    /// A transform's output broke its contract; `bounded` where the transform declares a bound,
    /// which makes it the engine's to keep.
    Contract {
        transform: String,
        bounded: bool,
        breach: Breach,
    },
    /// A transform allocated beyond the bound it declares.
    Bound {
        transform: String,
        allocated: u64,
        bound: u64,
    },
    /// One row takes more than what one request leaves the stage, as a transform asks or, where
    /// it names none, as the stage's own work does: moving its flags, decoding its encoded
    /// columns.
    Row {
        transform: Option<String>,
        asked: u64,
        free: u64,
    },
    /// A flush's batches, as a transform left them or, where it names none, as moving their
    /// flags did, take more than one request holds.
    Flush {
        transform: Option<String>,
        asked: u64,
        free: u64,
    },
    /// A batch's updates flag columns unchanged, met by a transform declaring no lineage.
    Partial { transform: String },
    /// A batch's updates flag unchanged a column a filter may read, whose value the source did
    /// not send.
    Unsent { transform: String },
    /// An insert or update flags unchanged a column a transform computes an output column from
    /// beside one it changed, so the output would be computed from a placeholder.
    Mixed { transform: String },
    /// A bug in the stage.
    Internal(String),
}

/// How a transform's output broke its contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Breach {
    /// It returned other rows than its input's.
    Rows { input: usize, output: usize },
    /// Its filter answered another count of values than rows.
    Keep { rows: usize, answered: usize },
    /// It returned a batch unlike the schema it declares.
    Schema,
    /// Its lineage does not give one entry an output column, each naming its input's columns.
    Lineage,
    /// The columns its filter says it reads are not all its input's.
    Reads,
    /// What it says it passes does not give one entry an output column, each naming its
    /// input's columns.
    Passes,
}

impl fmt::Display for Breach {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rows { input, output } => write!(f, "it returned {output} rows for {input}"),
            Self::Keep { rows, answered } => {
                write!(f, "its filter answered {answered} values for {rows} rows")
            }
            Self::Schema => f.write_str("it returned a batch unlike the schema it declares"),
            Self::Lineage => f.write_str("its lineage does not match its input and output"),
            Self::Reads => f.write_str("its filter says it reads columns its input lacks"),
            Self::Passes => {
                f.write_str("what it says it passes does not match its input and output")
            }
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema { transform, .. } => write!(f, "transform {transform} refuses its input"),
            Self::Kept { column } => write!(
                f,
                "its transforms do not pass on column {column}, which keys or times the stream, \
                 as it arrives"
            ),
            Self::Claimed { transform, column } => write!(
                f,
                "transform {transform} makes column {column}, which keys or times the stream or \
                 carries its changes, rather than pass it on as it arrives"
            ),
            Self::Absent { transform, column } => write!(
                f,
                "op {transform} names column {column}, which its declared schema lacks"
            ),
            Self::Failed { transform, .. } => write!(f, "transform {transform} failed on a batch"),
            Self::Contract {
                transform, breach, ..
            } => write!(f, "transform {transform}: {breach}"),
            Self::Bound {
                transform,
                allocated,
                bound,
            } => write!(
                f,
                "transform {transform} allocated {allocated} bytes, beyond the {bound} it declares"
            ),
            Self::Row {
                transform,
                asked,
                free,
            } => row(f, transform.as_deref(), *asked, *free),
            Self::Flush {
                transform,
                asked,
                free,
            } => flush(f, transform.as_deref(), *asked, *free),
            Self::Partial { transform } => write!(
                f,
                "updates leave columns unchanged, and transform {transform} says nothing of which \
                 columns it reads"
            ),
            Self::Unsent { transform } => write!(
                f,
                "updates leave columns unchanged that the filter of transform {transform} may \
                 read, whose values the source did not send"
            ),
            Self::Mixed { transform } => write!(
                f,
                "transform {transform} computes a column from one an update left unchanged and \
                 one it changed, whose value the source did not send"
            ),
            Self::Internal(message) => f.write_str(message),
        }
    }
}

/// Writes what one row takes, `asked` bytes as `transform` asks them or, where that is none, as
/// the stage's own work does, beyond the `free` bytes a request left.
fn row(f: &mut fmt::Formatter<'_>, transform: Option<&str>, asked: u64, free: u64) -> fmt::Result {
    match transform {
        Some(transform) => write!(f, "transform {transform} takes {asked} bytes for one row")?,
        None => write!(
            f,
            "moving one row's unchanged flags or decoding its encoded columns takes {asked} bytes"
        )?,
    }
    write!(f, ", beyond the {free} {LEFT}")
}

/// Writes what a flush's batches take, `asked` bytes as `transform` left them or, where that is
/// none, as moving their unchanged flags did, beyond the `free` bytes a request left.
fn flush(
    f: &mut fmt::Formatter<'_>,
    transform: Option<&str>,
    asked: u64,
    free: u64,
) -> fmt::Result {
    match transform {
        Some(transform) => write!(f, "a flush's batches, as transform {transform} left them")?,
        None => f.write_str("a flush's batches, their unchanged flags moved")?,
    }
    write!(f, ", take {asked} bytes, beyond the {free} {LEFT}")
}

/// What is left of the memory budget's request the stage holds.
const LEFT: &str = "left of one request of the memory budget";

/// The error `refusal` fails `stream` with.
pub(crate) fn refused(stream: &StreamName, refusal: Refusal) -> Error {
    let (kind, code) = classed(&refusal);
    let error = Error::new(kind, format!("stream {stream}: {refusal}"));
    let error = match code {
        Some(code) => error.with_code(code),
        None => error,
    };
    let error = match refusal {
        Refusal::Schema { error: cause, .. } | Refusal::Failed { error: cause, .. } => {
            error.with_source(cause)
        }
        Refusal::Kept { .. }
        | Refusal::Claimed { .. }
        | Refusal::Absent { .. }
        | Refusal::Contract { .. }
        | Refusal::Bound { .. }
        | Refusal::Row { .. }
        | Refusal::Flush { .. }
        | Refusal::Partial { .. }
        | Refusal::Unsent { .. }
        | Refusal::Mixed { .. }
        | Refusal::Internal(_) => error,
    };
    error.with_stream(stream)
}

/// The kind and code of the error `refusal` fails its stream with: a contract a transform that
/// declares a bound breaks is the engine's bug, and rows too large for the budget where no
/// transform made them are as lowering's own are, the source's.
fn classed(refusal: &Refusal) -> (ErrorKind, Option<Code>) {
    let (kind, code) = match refusal {
        Refusal::Schema { .. } | Refusal::Kept { .. } | Refusal::Absent { .. } => {
            (ErrorKind::Config, codes::TRANSFORM_SCHEMA)
        }
        Refusal::Claimed { .. } => (ErrorKind::Config, codes::TRANSFORM_INVALID),
        Refusal::Failed { .. } => (ErrorKind::Transform, codes::TRANSFORM_FAILED),
        Refusal::Contract { bounded: true, .. } => (ErrorKind::Internal, codes::TRANSFORM_CONTRACT),
        Refusal::Contract { bounded: false, .. } => {
            (ErrorKind::Transform, codes::TRANSFORM_CONTRACT)
        }
        Refusal::Bound { .. } => (ErrorKind::Internal, codes::TRANSFORM_BOUND),
        Refusal::Row {
            transform: Some(_), ..
        }
        | Refusal::Flush {
            transform: Some(_), ..
        } => (ErrorKind::Transform, codes::ROW_EXCEEDS_BUDGET),
        Refusal::Row {
            transform: None, ..
        }
        | Refusal::Flush {
            transform: None, ..
        } => (ErrorKind::Source, codes::ROW_EXCEEDS_BUDGET),
        Refusal::Partial { .. } | Refusal::Unsent { .. } | Refusal::Mixed { .. } => {
            (ErrorKind::Transform, codes::TRANSFORM_PARTIAL)
        }
        Refusal::Internal(_) => return (ErrorKind::Internal, None),
    };
    (kind, Some(code))
}
