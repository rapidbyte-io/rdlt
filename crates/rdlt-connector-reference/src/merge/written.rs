//! What a written batch of a merge table must hold before any of its rows is staged or merged.

use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use rdlt_connector::{ChangeColumns, ChangeOp, Deletion, MergeKey};

use super::refused::{
    DELETION_UNTIMED, FLAG_ON_KEY, FLAG_ON_MISSING_COLUMN, FLAGS_INVALID, MERGE_KEY_INVALID,
    OP_INVALID, SEQUENCE_MISSING, refused,
};
use super::retype::retyped;

/// Refuses `batch`, written for a table merged by `key` whose stored columns are `stored` where
/// they are known, unless every row has a sequence and, for a change stream's, an op, a deletion
/// time where a history table's deletes are soft, and unchanged flags that name stored columns
/// other than the key and the sequence.
pub(crate) fn admitted(
    batch: &RecordBatch,
    stored: Option<&SchemaRef>,
    key: &MergeKey,
) -> Result<(), ArrowError> {
    let Some(seqs) = batch.column_by_name(&key.seq) else {
        let message = format!("the rows have no sequence column {}", key.seq);
        return Err(refused(MERGE_KEY_INVALID, message));
    };
    if seqs.null_count() != 0 {
        return Err(refused(SEQUENCE_MISSING, "a row has no sequence"));
    }
    // A child table is keyed by its root's id, the first of its key's columns.
    let keyed = match &key.root {
        Some(_) => key.columns.get(..1).unwrap_or_default(),
        None => &key.columns[..],
    };
    if let Some(missing) = keyed
        .iter()
        .find(|column| batch.column_by_name(column).is_none())
    {
        let message = format!("the rows have no key column {missing}");
        return Err(refused(MERGE_KEY_INVALID, message));
    }
    let Some(changes) = &key.changes else {
        return Ok(());
    };
    let ops = ops(batch, changes)?;
    if let (Some(_), Deletion::Soft { at }) = (&key.history, &changes.deletion) {
        let times = batch.column_by_name(at);
        let untimed = ops.iter().enumerate().any(|(row, op)| {
            matches!(op, ChangeOp::Delete | ChangeOp::Truncate)
                && times.is_none_or(|times| times.is_null(row))
        });
        if untimed {
            let message = "a delete or a truncate of a history table says no deletion time";
            return Err(refused(DELETION_UNTIMED, message));
        }
    }
    flags(batch, stored, key, changes)
}

/// The op of every row of `batch`, a written batch, as the column `changes` names holds it.
pub(super) fn ops(
    batch: &RecordBatch,
    changes: &ChangeColumns,
) -> Result<Vec<ChangeOp>, ArrowError> {
    let Some(ops) = batch.column_by_name(&changes.op) else {
        let message = format!("the rows have no op column {}", changes.op);
        return Err(refused(OP_INVALID, message));
    };
    let ops = retyped(ops, &DataType::Int8)
        .map_err(|_| refused(OP_INVALID, "a change stream's ops are no bytes"))?;
    ops.as_primitive::<Int8Type>()
        .iter()
        .map(|op| {
            op.and_then(ChangeOp::from_code)
                .ok_or_else(|| refused(OP_INVALID, format!("{op:?} is no change stream's op")))
        })
        .collect()
}

/// Refuses `batch` where a row flags unchanged a field that is its key, its sequence, or no
/// column its table stores.
fn flags(
    batch: &RecordBatch,
    stored: Option<&SchemaRef>,
    key: &MergeKey,
    changes: &ChangeColumns,
) -> Result<(), ArrowError> {
    let Some(flags) = changes
        .unchanged
        .as_deref()
        .and_then(|column| batch.column_by_name(column))
    else {
        return Ok(());
    };
    let flags = retyped(flags, &DataType::Binary)
        .map_err(|_| refused(FLAGS_INVALID, "unchanged flags are no bitmap of bytes"))?;
    let flags = flags.as_binary::<i32>();
    let schema = batch.schema();
    // What a flag on each of the batch's fields is refused as, worked out once for every row.
    let directive = |name: &str| name == &*changes.op || changes.unchanged.as_deref() == Some(name);
    let refusals: Vec<Option<&'static str>> = schema
        .fields()
        .iter()
        .map(|field| {
            let name = field.name().as_str();
            if *key.seq == *name || key.columns.iter().any(|column| **column == *name) {
                Some(FLAG_ON_KEY)
            } else if directive(name)
                || stored.is_some_and(|stored| stored.field_with_name(name).is_err())
            {
                Some(FLAG_ON_MISSING_COLUMN)
            } else {
                None
            }
        })
        .collect();
    for row in (0..flags.len()).filter(|row| flags.is_valid(*row)) {
        for (byte, bits) in flags
            .value(row)
            .iter()
            .enumerate()
            .filter(|(_, bits)| **bits != 0)
        {
            for bit in (0..8).filter(|bit| bits & (1 << bit) != 0) {
                let field = byte * 8 + bit;
                if let Some(code) = refusals.get(field).copied().flatten() {
                    let name = schema.field(field).name();
                    let message = format!("a change flags {name} unchanged");
                    return Err(refused(code, message));
                }
            }
        }
    }
    Ok(())
}
