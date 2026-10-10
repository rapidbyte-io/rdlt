//! Merging a change stream's rows: each row applies in sequence order, only when its sequence is
//! greater than the published row's, as an insert, update, delete or truncate.
//!
//! The work is in proportion to the rows given: a row no change touches is one reference, a
//! truncate is recorded and met by each row once, and a row's unchanged flags are read through a
//! map made once for its batch.

mod table;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, ChangeOp, Deletion, MergeKey, UnchangedFlags};

use super::refused::{FLAG_ON_KEY, SEQUENCE_MISSING, refused};
use super::retype::retyped;
use super::sparse::{At, Nulls};
use super::tombstones::{self, Tombstones};
use super::written::ops;
use super::{converter, source_keys, source_seqs, sources};
use table::{Applied, Change, Columns, Table};

/// The schema a change stream's table stores: `schema` without the columns `changes` names that
/// only written batches carry.
pub(crate) fn stored(schema: &SchemaRef, changes: &ChangeColumns) -> SchemaRef {
    let directive = |name: &str| {
        name == &*changes.op
            || changes
                .unchanged
                .as_deref()
                .is_some_and(|flags| flags == name)
    };
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|field| !directive(field.name()))
        .cloned()
        .collect();
    Arc::new(Schema::new(fields))
}

/// The published rows of a change stream's table, and its tombstones, once `incoming` applies,
/// row by row in sequence order, to `published` and `buried`, its tombstones.
pub(crate) fn merge_changes(
    schema: &SchemaRef,
    published: &[RecordBatch],
    buried: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
    changes: &ChangeColumns,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>), ArrowError> {
    let schema = stored(schema, changes);
    let converter = converter(&schema, key)?;
    let mut nulls = Nulls::default();
    let (sources, held) = sources(&schema, published, incoming)?;
    let keys = source_keys(&sources, &converter, key, &mut nulls)?;
    let seqs = source_seqs(&sources, key, &mut nulls)?;
    let flags = incoming
        .iter()
        .map(|batch| Flags::of(batch, &schema, key, changes))
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = changed_rows(incoming, held, &seqs, changes)?;
    // A truncate removes what is sequenced before it, and a key's change at its own sequence
    // is not before it: the truncate applies first, wherever it was written. A stable sort
    // keeps the other rows of one sequence in the order they were written.
    rows.sort_by(|left, right| {
        let keyed = |change: &Change| change.op != ChangeOp::Truncate;
        (&left.seq, keyed(left)).cmp(&(&right.seq, keyed(right)))
    });
    let columns = Columns {
        seq: schema.index_of(&key.seq)?,
        at: match &changes.deletion {
            Deletion::Hard => None,
            Deletion::Soft { at } => Some(schema.index_of(at)?),
        },
    };
    let hard = columns.at.is_none();
    let mut table = Table::load(&sources, held, (&keys, &seqs), columns);
    let tombstone_schema = tombstones::schema(&schema, key)?;
    let mut tombstones = Tombstones::load(buried, &tombstone_schema, &converter, key)?;
    for change in rows {
        if change.op == ChangeOp::Truncate {
            if tombstones.admits(None, &change.seq) {
                table.truncate(&change);
                if hard {
                    tombstones.raise(change.seq);
                }
            }
            continue;
        }
        let at = change.at;
        let row_key = keys[at.source].row(at.row).as_ref().to_vec();
        if !tombstones.admits(Some(&row_key), &change.seq) {
            continue;
        }
        let unchanged = match &flags[at.source - held] {
            Some(flags) => flags.mask(at.row)?,
            None => None,
        };
        let seq = change.seq.clone();
        match table.apply(row_key.clone(), change, unchanged.as_deref()) {
            Applied::Removed => tombstones.bury(row_key, seq, at),
            Applied::Held => tombstones.lift(&row_key),
            Applied::Nothing => {}
        }
    }
    let merged = table.assemble()?;
    let buried = tombstones.assemble(&tombstone_schema, &sources, key, &mut nulls)?;
    Ok((merged, buried))
}

/// Every row of `incoming`, the sources from `first` on, with its op and its sequence, which
/// `seqs` holds for each source.
fn changed_rows(
    incoming: &[RecordBatch],
    first: usize,
    seqs: &[ArrayRef],
    changes: &ChangeColumns,
) -> Result<Vec<Change>, ArrowError> {
    let mut rows = Vec::new();
    for (index, raw) in incoming.iter().enumerate() {
        let source = first + index;
        let seqs = seqs[source].as_binary::<i32>();
        for (row, op) in ops(raw, changes)?.into_iter().enumerate() {
            if seqs.is_null(row) {
                return Err(refused(SEQUENCE_MISSING, "a change has no sequence"));
            }
            rows.push(Change {
                at: At { source, row },
                op,
                seq: seqs.value(row).to_vec(),
            });
        }
    }
    Ok(rows)
}

/// The unchanged flags of one written batch: each row's bitmap over the batch's fields, and the
/// stored column each field is, worked out once for the batch.
struct Flags {
    bitmaps: ArrayRef,
    /// The stored column of each of the batch's fields, none for a field the table does not
    /// store.
    stored: Vec<Option<usize>>,
    /// Whether each stored column is a key column or the sequence, which every change sets.
    set_always: Vec<bool>,
}

impl Flags {
    /// The flags of `raw`, a written batch of a table of `schema` merged by `key`, where its rows
    /// carry any.
    fn of(
        raw: &RecordBatch,
        schema: &SchemaRef,
        key: &MergeKey,
        changes: &ChangeColumns,
    ) -> Result<Option<Self>, ArrowError> {
        let Some(bitmaps) = changes
            .unchanged
            .as_deref()
            .and_then(|column| raw.column_by_name(column))
        else {
            return Ok(None);
        };
        let stored = raw
            .schema_ref()
            .fields()
            .iter()
            .map(|field| schema.index_of(field.name()).ok())
            .collect();
        let set_always = schema
            .fields()
            .iter()
            .map(|field| {
                let name = field.name().as_str();
                *key.seq == *name || key.columns.iter().any(|column| **column == *name)
            })
            .collect();
        Ok(Some(Self {
            bitmaps: retyped(bitmaps, &DataType::Binary)?,
            stored,
            set_always,
        }))
    }

    /// Which stored columns row `row` flags unchanged; none where it flags none.
    ///
    /// A flag on a key column or the sequence, which a change always sets, is an error.
    fn mask(&self, row: usize) -> Result<Option<Vec<bool>>, ArrowError> {
        let bitmaps = self.bitmaps.as_binary::<i32>();
        if bitmaps.is_null(row) {
            return Ok(None);
        }
        let flags = UnchangedFlags::new(bitmaps.value(row));
        let mut mask = vec![false; self.set_always.len()];
        let mut flagged = false;
        for (ordinal, stored) in self.stored.iter().enumerate() {
            let (true, Some(column)) = (flags.contains(ordinal), stored) else {
                continue;
            };
            if self.set_always[*column] {
                let message = "a change flags its key or sequence column unchanged";
                return Err(refused(FLAG_ON_KEY, message));
            }
            mask[*column] = true;
            flagged = true;
        }
        Ok(flagged.then_some(mask))
    }
}
