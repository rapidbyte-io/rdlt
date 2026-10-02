//! A change stream's rows: split into their data and their change columns as each piece is
//! lowered, so nothing of a unit is copied before the piece that holds it has reserved it.

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int8Type;
use arrow_array::{BooleanArray, RecordBatch};
use rdlt_connector::cost::Stored;
use rdlt_connector::{ChangeOp, OP_COLUMN, StreamName};

use super::super::ChangeMode;
use crate::error::Error;
use crate::plan::{DeleteMode, OnTruncate};
use crate::table::{ChangeRows, data_ordinals};

/// Bytes: what splitting a change row holds beside its data: its op, its position as bytes and
/// its flags, each with its offset.
pub(super) const CHANGE_ROW: u64 = 64;

/// How many rows a change stream ignores.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Ignored {
    pub(super) deletes: u64,
    pub(super) truncates: u64,
}

impl Ignored {
    /// The rows ignored, of either kind.
    pub(super) fn rows(self) -> u64 {
        self.deletes + self.truncates
    }
}

/// Whether a stream read as `mode` ignores a change of `op`.
fn ignores(mode: ChangeMode, op: Option<ChangeOp>) -> bool {
    match op {
        Some(ChangeOp::Delete) => mode.merge && mode.deletes == DeleteMode::Ignore,
        Some(ChangeOp::Truncate) => mode.merge && mode.truncates == OnTruncate::Ignore,
        _ => false,
    }
}

/// The rows of `parts`, a change stream's batches, the stream ignores: counted from their ops
/// where they lie.
pub(super) fn ignored(mode: ChangeMode, parts: &[RecordBatch]) -> Ignored {
    let mut ignored = Ignored::default();
    for part in parts {
        let Some(ops) = part.column_by_name(OP_COLUMN) else {
            continue;
        };
        let Some(ops) = ops.as_primitive_opt::<Int8Type>() else {
            continue;
        };
        for op in ops.values() {
            match ChangeOp::from_code(*op) {
                op @ Some(ChangeOp::Delete) if ignores(mode, op) => ignored.deletes += 1,
                op @ Some(ChangeOp::Truncate) if ignores(mode, op) => ignored.truncates += 1,
                _ => {}
            }
        }
    }
    ignored
}

/// `stored`, how a table stores each data column of `batch`, a change batch, by the batch's own
/// columns: nothing for its change columns.
pub(super) fn aligned(batch: &RecordBatch, stored: &[Option<Stored>]) -> Vec<Option<Stored>> {
    data_ordinals(batch)
        .into_iter()
        .map(|data| data.and_then(|data| stored.get(data).cloned().flatten()))
        .collect()
}

/// The data columns of `batch`, a change batch, none of them copied.
pub(super) fn data(batch: &RecordBatch) -> Result<RecordBatch, Error> {
    ChangeRows::data(batch).map_err(|error| Error::internal(format!("splitting changes: {error}")))
}

/// `parts`, a piece of a change stream's unit, as one batch of data and its change columns,
/// without the deletes and truncates the stream ignores.
///
/// A merge's rows flagging columns unchanged are refused where the destination cannot keep a
/// column's value.
pub(super) fn split_changes(
    stream: &StreamName,
    mode: ChangeMode,
    parts: &[RecordBatch],
) -> Result<(RecordBatch, ChangeRows), Error> {
    let failed =
        |error: arrow_schema::ArrowError| Error::internal(format!("splitting changes: {error}"));
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts).map_err(failed)?;
    let (data, changes) = ChangeRows::split(&batch).map_err(failed)?;
    // A log stores each row's flags as data; only a merge keeps a column's value.
    let flagged = if mode.merge && !mode.partial_updates {
        changes.flagged()
    } else {
        Vec::new()
    };
    if !flagged.is_empty() {
        let schema = data.schema();
        let names: Vec<&str> = flagged
            .iter()
            .filter_map(|ordinal| {
                schema
                    .fields()
                    .get(*ordinal)
                    .map(|field| field.name().as_str())
            })
            .collect();
        return Err(Error::config(format!(
            "stream {stream}: updates leave columns {} unchanged, which the destination cannot \
             keep",
            names.join(", ")
        ))
        .with_code("partial_updates_unsupported")
        .with_stream(stream));
    }
    let keep: BooleanArray = (0..changes.op.len())
        .map(|row| Some(!ignores(mode, changes.op(row))))
        .collect();
    if keep.true_count() == keep.len() {
        return Ok((data, changes));
    }
    let data = arrow_select::filter::filter_record_batch(&data, &keep).map_err(failed)?;
    Ok((data, changes.filter(&keep).map_err(failed)?))
}
