//! A change stream's unit split into its data and its change columns.

use arrow_array::{BooleanArray, RecordBatch};
use rdlt_connector::ChangeOp;

use super::super::{ChangeMode, OpenSegment, PartitionJob};
use crate::error::Error;
use crate::plan::{DeleteMode, OnTruncate};
use crate::table::ChangeRows;

/// `parts`, a change stream's batches of one schema, as one batch of data and its change
/// columns, without the deletes and truncates the stream ignores, which `open` counts.
///
/// A merge's rows flagging columns unchanged are refused where the destination cannot keep a
/// column's value.
pub(super) fn split_changes(
    job: &PartitionJob,
    mode: ChangeMode,
    open: &mut OpenSegment,
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
            "stream {}: updates leave columns {} unchanged, which the destination cannot keep",
            job.stream,
            names.join(", ")
        ))
        .with_code("partial_updates_unsupported")
        .with_stream(&job.stream));
    }
    let ignores = |op| match op {
        Some(ChangeOp::Delete) => mode.merge && mode.deletes == DeleteMode::Ignore,
        Some(ChangeOp::Truncate) => mode.merge && mode.truncates == OnTruncate::Ignore,
        _ => false,
    };
    let keep: BooleanArray = (0..changes.op.len())
        .map(|row| Some(!ignores(changes.op(row))))
        .collect();
    if keep.true_count() == keep.len() {
        return Ok((data, changes));
    }
    for row in (0..changes.op.len()).filter(|row| !keep.value(*row)) {
        match changes.op(row) {
            Some(ChangeOp::Delete) => open.deletes_ignored += 1,
            _ => open.truncates_ignored += 1,
        }
    }
    let data = arrow_select::filter::filter_record_batch(&data, &keep).map_err(failed)?;
    Ok((data, changes.filter(&keep).map_err(failed)?))
}
