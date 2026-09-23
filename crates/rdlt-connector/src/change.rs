//! Change batches: Arrow batches whose rows are inserts, updates and deletes.

#[cfg(test)]
mod tests;

use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;

use crate::error::{ConnectorError, Result};

/// The column holding each row's [`ChangeOp`] as an `Int8`.
pub const OP_COLUMN: &str = "_rdlt_op";

/// The column holding each row's source position: `FixedSizeBinary(16)`, big-endian, compared bytewise.
pub const SEQ_COLUMN: &str = "_rdlt_seq";

/// The optional column flagging columns an update left unchanged: a bitmap over field ordinals.
pub const UNCHANGED_COLUMN: &str = "_rdlt_unchanged";

/// What a change row does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChangeOp {
    /// A new row.
    Insert,
    /// A new version of an existing row.
    Update,
    /// A removed row; only key columns need values.
    Delete,
}

impl ChangeOp {
    /// The op's value in [`OP_COLUMN`].
    pub fn code(self) -> i8 {
        match self {
            Self::Insert => 0,
            Self::Update => 1,
            Self::Delete => 2,
        }
    }

    /// The op an [`OP_COLUMN`] value names.
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Insert),
            1 => Some(Self::Update),
            2 => Some(Self::Delete),
            _ => None,
        }
    }
}

/// Checks that `batch` carries valid op and sequence columns, and a valid unchanged column if any.
pub fn validate_change_batch(batch: &RecordBatch) -> Result<()> {
    let invalid = |reason: String| {
        ConnectorError::data(format!("change batch: {reason}")).with_code("change_batch")
    };
    let ops = batch
        .column_by_name(OP_COLUMN)
        .ok_or_else(|| invalid(format!("no {OP_COLUMN} column")))?;
    if ops.data_type() != &DataType::Int8 {
        return Err(invalid(format!(
            "{OP_COLUMN} is {}, not Int8",
            ops.data_type()
        )));
    }
    if ops.null_count() > 0 {
        return Err(invalid(format!("{OP_COLUMN} has nulls")));
    }
    if let Some(code) = ops
        .as_primitive::<Int8Type>()
        .values()
        .iter()
        .find(|code| ChangeOp::from_code(**code).is_none())
    {
        return Err(invalid(format!(
            "{OP_COLUMN} holds {code}, which is not an op"
        )));
    }
    let seq = batch
        .column_by_name(SEQ_COLUMN)
        .ok_or_else(|| invalid(format!("no {SEQ_COLUMN} column")))?;
    if seq.data_type() != &DataType::FixedSizeBinary(16) {
        return Err(invalid(format!(
            "{SEQ_COLUMN} is {}, not FixedSizeBinary(16)",
            seq.data_type()
        )));
    }
    if seq.null_count() > 0 {
        return Err(invalid(format!("{SEQ_COLUMN} has nulls")));
    }
    if let Some(unchanged) = batch.column_by_name(UNCHANGED_COLUMN)
        && unchanged.data_type() != &DataType::Binary
    {
        return Err(invalid(format!(
            "{UNCHANGED_COLUMN} is {}, not Binary",
            unchanged.data_type()
        )));
    }
    Ok(())
}
