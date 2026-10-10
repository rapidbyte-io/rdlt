//! Change batches: Arrow batches whose rows are inserts, updates and deletes.

#[cfg(test)]
mod tests;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_schema::DataType;

use crate::error::{ConnectorError, Result};

/// The column holding each row's [`ChangeOp`] as an `Int8`.
pub const OP_COLUMN: &str = "_rdlt_op";

/// The column holding each row's source position: `FixedSizeBinary(16)`, big-endian, compared bytewise.
pub const SEQ_COLUMN: &str = "_rdlt_seq";

/// The optional column flagging columns an update left unchanged: a bitmap over field ordinals.
pub const UNCHANGED_COLUMN: &str = "_rdlt_unchanged";

/// One row's [`UNCHANGED_COLUMN`] value: a bitmap whose bit `i % 8` of byte `i / 8` flags the
/// field at ordinal `i`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnchangedFlags<'a>(&'a [u8]);

impl<'a> UnchangedFlags<'a> {
    /// The flags `bitmap` holds.
    pub fn new(bitmap: &'a [u8]) -> Self {
        Self(bitmap)
    }

    /// Whether the field at `ordinal` is flagged unchanged; no field past the bitmap is.
    pub fn contains(self, ordinal: usize) -> bool {
        self.0
            .get(ordinal / 8)
            .is_some_and(|byte| byte & (1 << (ordinal % 8)) != 0)
    }

    /// The ordinals flagged unchanged, in ascending order, read a set bit at a time.
    ///
    /// Every byte is read: a bitmap a source sent is walked with [`Self::ordinals_below`].
    pub fn ordinals(self) -> impl Iterator<Item = usize> + 'a {
        self.0
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte != 0)
            .flat_map(|(index, byte)| {
                let byte = *byte;
                (0..8)
                    .filter(move |bit| byte & (1 << bit) != 0)
                    .map(move |bit| index * 8 + bit)
            })
    }

    /// The ordinals below `fields` flagged unchanged, in ascending order: only the bytes holding
    /// them are read, however long the bitmap is.
    pub fn ordinals_below(self, fields: usize) -> impl Iterator<Item = usize> + 'a {
        let held = &self.0[..self.0.len().min(fields.div_ceil(8))];
        Self(held)
            .ordinals()
            .take_while(move |ordinal| *ordinal < fields)
    }
}

/// `flags`, bitmaps over some fields' ordinals, over others: the field at ordinal `i` is flagged
/// at `to[i]`, or dropped where that is `None` or `i` is past `to`.
///
/// A null row stays null; a row with flags stays a row, empty where none of its flags is kept,
/// and as long as its last kept flag needs. Only the bytes holding ordinals `to` maps are read,
/// however long a bitmap is.
pub fn remap_unchanged(flags: &BinaryArray, to: &[Option<usize>]) -> BinaryArray {
    let mut remapped = BinaryBuilder::with_capacity(flags.len(), flags.len());
    let mut out: Vec<u8> = Vec::new();
    for bitmap in flags {
        let Some(bitmap) = bitmap else {
            remapped.append_null();
            continue;
        };
        out.clear();
        for ordinal in UnchangedFlags::new(bitmap).ordinals_below(to.len()) {
            let Some(target) = to[ordinal] else {
                continue;
            };
            out.resize(out.len().max(target / 8 + 1), 0);
            out[target / 8] |= 1 << (target % 8);
        }
        remapped.append_value(&out);
    }
    remapped.finish()
}

/// What a change row does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChangeOp {
    /// A new row.
    Insert,
    /// A new version of an existing row.
    Update,
    /// A removed row; only key columns need values.
    Delete,
    /// Every row of the table the source truncated before this position; no column needs a
    /// value.
    Truncate,
}

impl ChangeOp {
    /// The op's value in [`OP_COLUMN`].
    pub fn code(self) -> i8 {
        match self {
            Self::Insert => 0,
            Self::Update => 1,
            Self::Delete => 2,
            Self::Truncate => 3,
        }
    }

    /// The op an [`OP_COLUMN`] value names.
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Insert),
            1 => Some(Self::Update),
            2 => Some(Self::Delete),
            3 => Some(Self::Truncate),
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
