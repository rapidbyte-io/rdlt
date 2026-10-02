//! What a change stream's table remembers of rows it removed outright, so that a change sequenced
//! before their removal, arriving again later, never brings them back.
//!
//! A hard delete leaves the key's tombstone: its sequence. A hard truncate leaves a bound: rows
//! sequenced before it are gone, whichever key they hold. Soft deletes keep their rows, whose
//! sequences guard them, and need neither. Tombstones are stored as rows of the key's columns and
//! the sequence; the bound's names no key.
//!
//! A tombstone stays until a later change of its key lifts it or the bound passes it: a source
//! may send any earlier change of a key again, however long after, so none is dropped for its
//! age.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch, new_null_array};
use arrow_row::RowConverter;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use rdlt_connector::MergeKey;

use super::key_columns;
use super::retype::{compared, retyped};
use super::sparse::{At, Nulls, Sources};

/// Where a tombstone's key values come from.
#[derive(Clone, Copy, Debug)]
enum KeyCell {
    /// Row of the stored tombstones.
    Stored(usize),
    /// A row of the merge's sources.
    Incoming(At),
}

/// One removed key: the sequence that removed it, and where its key values come from.
#[derive(Debug)]
struct Stone {
    seq: Vec<u8>,
    key: KeyCell,
}

/// A table's tombstones while its changes apply.
#[derive(Debug)]
pub(crate) struct Tombstones {
    stored: RecordBatch,
    by_key: BTreeMap<Vec<u8>, Stone>,
    /// The sequence of the latest hard truncate: every row sequenced before it is gone.
    bound: Option<Vec<u8>>,
    /// Whether the changes left the tombstones other than as they were stored.
    changed: bool,
}

/// The schema of `schema`'s tombstones, for a table merged by `key`: its key columns, nullable
/// since the bound names no key, then its sequence.
pub(crate) fn schema(schema: &SchemaRef, key: &MergeKey) -> Result<SchemaRef, ArrowError> {
    let mut fields: Vec<Field> = key
        .columns
        .iter()
        .map(|column| {
            schema
                .field_with_name(column)
                .map(|field| field.clone().with_nullable(true))
        })
        .collect::<Result<_, _>>()?;
    fields.push(Field::new(key.seq.as_ref(), DataType::Binary, false));
    Ok(Arc::new(Schema::new(fields)))
}

/// The stored tombstones `batches` as one batch of `schema`, the tombstone schema: its few
/// columns found by name and converted to its types.
fn stored_under(batches: &[RecordBatch], schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let mut under = batches
        .iter()
        .map(|batch| {
            let columns = schema
                .fields()
                .iter()
                .map(|field| match batch.column_by_name(field.name()) {
                    Some(column) => retyped(column, field.data_type()),
                    None => Ok(new_null_array(field.data_type(), batch.num_rows())),
                })
                .collect::<Result<Vec<ArrayRef>, _>>()?;
            RecordBatch::try_new(Arc::clone(schema), columns)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if under.len() == 1 {
        // One batch is itself: nothing of it is copied.
        return Ok(under.swap_remove(0));
    }
    arrow_select::concat::concat_batches(schema, &under)
}

impl Tombstones {
    /// The tombstones `stored`, as `schema`, the tombstone schema, keyed as `converter` encodes
    /// keys.
    pub(crate) fn load(
        stored: &[RecordBatch],
        schema: &SchemaRef,
        converter: &RowConverter,
        key: &MergeKey,
    ) -> Result<Self, ArrowError> {
        let stored = stored_under(stored, schema)?;
        let columns = key_columns(&stored, key)?;
        let keys = converter.convert_columns(&columns)?;
        let seqs = compared(stored.column(key.columns.len()))?;
        let seqs = seqs.as_binary::<i32>();
        let mut tombstones = Self {
            stored: stored.clone(),
            by_key: BTreeMap::new(),
            bound: None,
            changed: false,
        };
        for row in 0..stored.num_rows() {
            let seq = seqs.value(row).to_vec();
            if columns.iter().all(|column| column.is_null(row)) {
                // A bound above another, as two commits' truncates leave, is the table's.
                tombstones.bound = tombstones.bound.max(Some(seq));
            } else {
                let key = KeyCell::Stored(row);
                tombstones
                    .by_key
                    .insert(keys.row(row).as_ref().to_vec(), Stone { seq, key });
            }
        }
        Ok(tombstones)
    }

    /// Whether a change of `key`, none for a truncate, sequenced at `seq` may apply: it is not
    /// sequenced before the bound, nor at or before its key's tombstone.
    pub(crate) fn admits(&self, key: Option<&[u8]>, seq: &[u8]) -> bool {
        let bounded = self.bound.as_deref().is_some_and(|bound| seq < bound);
        let buried = key
            .and_then(|key| self.by_key.get(key))
            .is_some_and(|stone| stone.seq.as_slice() >= seq);
        !bounded && !buried
    }

    /// Records that `key`, whose values the row `at` of the merge's sources holds, was removed
    /// at `seq`.
    pub(crate) fn bury(&mut self, key: Vec<u8>, seq: Vec<u8>, at: At) {
        let key_cell = KeyCell::Incoming(at);
        self.by_key.insert(key, Stone { seq, key: key_cell });
        self.changed = true;
    }

    /// Forgets `key`'s tombstone: a row sequenced after it holds the key now.
    pub(crate) fn lift(&mut self, key: &[u8]) {
        self.changed |= self.by_key.remove(key).is_some();
    }

    /// Records a hard truncate at `seq`: the bound rises to it, which covers the tombstones
    /// before it; they go when the tombstones are assembled, once for every truncate of a merge.
    pub(crate) fn raise(&mut self, seq: Vec<u8>) {
        if self.bound.as_ref().is_some_and(|bound| *bound >= seq) {
            return;
        }
        self.bound = Some(seq);
        self.changed = true;
    }

    /// The tombstones as batches of `schema`, the tombstone schema, their key values taken from
    /// the stored tombstones or `sources`, the merge's; the stored batch itself where the changes
    /// left them as they were.
    pub(crate) fn assemble(
        &self,
        schema: &SchemaRef,
        sources: &Sources,
        key: &MergeKey,
        nulls: &mut Nulls,
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        if !self.changed {
            return Ok(super::held([self.stored.clone()]));
        }
        let covered = |stone: &&Stone| self.bound.as_ref().is_none_or(|bound| stone.seq >= *bound);
        let stones: Vec<&Stone> = self.by_key.values().filter(covered).collect();
        let bound = usize::from(self.bound.is_some());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(key.columns.len() + 1);
        for (index, column) in key.columns.iter().enumerate() {
            let field = schema.field(index);
            let null = new_null_array(field.data_type(), 1);
            let place = sources.schema().index_of(column)?;
            // The stored tombstones first, then each source the stones' keys come from, once.
            let mut values: Vec<ArrayRef> = vec![Arc::clone(self.stored.column(index))];
            let mut places: BTreeMap<usize, usize> = BTreeMap::new();
            let mut indices: Vec<(usize, usize)> = Vec::with_capacity(stones.len() + bound);
            for stone in &stones {
                indices.push(match stone.key {
                    KeyCell::Stored(row) => (0, row),
                    KeyCell::Incoming(at) => {
                        let place = *places.entry(at.source).or_insert_with(|| {
                            values.push(sources.dense(at.source, place, nulls));
                            values.len() - 1
                        });
                        (place, at.row)
                    }
                });
            }
            values.push(null);
            indices.extend(std::iter::repeat_n((values.len() - 1, 0), bound));
            let values: Vec<&dyn Array> = values.iter().map(AsRef::as_ref).collect();
            columns.push(arrow_select::interleave::interleave(&values, &indices)?);
        }
        let seqs: BinaryArray = stones
            .iter()
            .map(|stone| Some(stone.seq.as_slice()))
            .chain(self.bound.iter().map(|bound| Some(bound.as_slice())))
            .collect();
        columns.push(Arc::new(seqs));
        let options =
            arrow_array::RecordBatchOptions::new().with_row_count(Some(stones.len() + bound));
        let assembled = RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)?;
        Ok(super::held([assembled]))
    }
}
