//! A change stream's table while its changes apply: each key's row by where its cells come from,
//! and the truncates met so far, applied to a row when it is next touched.

use std::collections::BTreeMap;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_row::RowConverter;
use arrow_schema::{ArrowError, SchemaRef};
use rdlt_connector::{ChangeOp, MergeKey};

use super::super::aligned::{Nulls, interleaved};
use super::super::{binary, key_columns};
use arrow_array::cast::AsArray;

/// A row of a source batch: the published batch is source 0, incoming batch `i` source `i + 1`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct At {
    pub(super) source: usize,
    pub(super) row: usize,
}

/// Where a merged row's cells come from, in the schema's order.
enum Cells {
    /// Every cell is that of one source row: a row no change of the merge composed.
    Whole(At),
    /// Each cell from its own source row, or null.
    Mixed(Vec<Option<At>>),
}

impl Cells {
    fn cell(&self, column: usize) -> Option<At> {
        match self {
            Self::Whole(at) => Some(*at),
            Self::Mixed(cells) => cells[column],
        }
    }
}

/// A merged row: its sequence, its cells, and how many of the table's truncates it has met.
struct Merged {
    seq: Vec<u8>,
    cells: Cells,
    met: usize,
}

/// A truncate the changes applied: its sequence and its row.
struct Truncate {
    seq: Vec<u8>,
    at: At,
}

/// Where a change stream's table keeps its sequence and deletion time, and how many columns it
/// has.
pub(super) struct Columns {
    pub(super) seq: usize,
    pub(super) at: Option<usize>,
    pub(super) count: usize,
}

/// What applying a change did to its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Applied {
    /// It removed the key's row outright.
    Removed,
    /// A row it wrote holds the key.
    Held,
    /// Neither.
    Nothing,
}

/// One incoming row, and what it does.
pub(super) struct Change {
    pub(super) at: At,
    pub(super) op: ChangeOp,
    pub(super) seq: Vec<u8>,
}

/// The rows of a table, by key, in the order they were first published.
pub(super) struct Table<'a> {
    rows: Vec<Option<Merged>>,
    by_key: BTreeMap<Vec<u8>, usize>,
    /// The truncates applied so far, in sequence order, no two of one sequence.
    truncates: Vec<Truncate>,
    /// The positions in `truncates` of those that say when they deleted.
    timed: Vec<usize>,
    columns: Columns,
    published: &'a RecordBatch,
    aligned: &'a [RecordBatch],
}

impl<'a> Table<'a> {
    /// The table holding `published`, its rows keyed as `converter` encodes `key`, to which rows
    /// of `aligned`, the incoming batches under the table's schema, apply.
    pub(super) fn load(
        published: &'a RecordBatch,
        aligned: &'a [RecordBatch],
        converter: &RowConverter,
        key: &MergeKey,
        columns: Columns,
    ) -> Result<Self, ArrowError> {
        let mut table = Self {
            rows: Vec::with_capacity(published.num_rows()),
            by_key: BTreeMap::new(),
            truncates: Vec::new(),
            timed: Vec::new(),
            columns,
            published,
            aligned,
        };
        let keys = converter.convert_columns(&key_columns(published, key)?)?;
        let seqs = binary(published, &key.seq)?;
        let seqs = seqs.as_binary::<i32>();
        for row in 0..published.num_rows() {
            let merged = Merged {
                seq: seqs.value(row).to_vec(),
                cells: Cells::Whole(At { source: 0, row }),
                met: 0,
            };
            table.put(keys.row(row).as_ref().to_vec(), merged);
        }
        Ok(table)
    }

    fn column(&self, source: usize, column: usize) -> &ArrayRef {
        match source {
            0 => self.published.column(column),
            incoming => self.aligned[incoming - 1].column(column),
        }
    }

    /// Whether `cell` of `column` is null.
    fn is_null(&self, cell: Option<At>, column: usize) -> bool {
        cell.is_none_or(|at| self.column(at.source, column).is_null(at.row))
    }

    fn put(&mut self, key: Vec<u8>, merged: Merged) {
        if let Some(index) = self.by_key.get(&key) {
            self.rows[*index] = Some(merged);
        } else {
            self.by_key.insert(key, self.rows.len());
            self.rows.push(Some(merged));
        }
    }

    /// Records the truncate `change`: every row sequenced before it is removed, or marked
    /// deleted, when it next meets the table's truncates.
    ///
    /// Truncates arrive in sequence order, and a second of one sequence finds no row before it
    /// that the first left, so it is not kept.
    pub(super) fn truncate(&mut self, change: &Change) {
        if self
            .truncates
            .last()
            .is_some_and(|last| last.seq == change.seq)
        {
            return;
        }
        let timed = self
            .columns
            .at
            .is_some_and(|at| !self.is_null(Some(change.at), at));
        if timed {
            self.timed.push(self.truncates.len());
        }
        self.truncates.push(Truncate {
            seq: change.seq.clone(),
            at: change.at,
        });
    }

    /// Applies to the row at `index` the truncates it has not met: the first sequenced past it
    /// removes it, or where deletes are soft marks it deleted then, and the last leaves it its
    /// sequence.
    fn meet(&mut self, index: usize) {
        let Some(merged) = self.rows[index].as_ref() else {
            return;
        };
        let pending = &self.truncates[merged.met..];
        let first = merged.met + pending.partition_point(|truncate| truncate.seq <= merged.seq);
        let (met, Some(last)) = (self.truncates.len(), self.truncates.last()) else {
            return;
        };
        if first == met {
            self.rows[index].as_mut().expect("the row was found").met = met;
            return;
        }
        let Some(at) = self.columns.at else {
            self.rows[index] = None;
            return;
        };
        // The row keeps when it was deleted; else the first truncate that says when does.
        let timed = self.timed.partition_point(|position| *position < first);
        let deleted_by = self
            .timed
            .get(timed)
            .map_or(last.at, |position| self.truncates[*position].at);
        let (seq, by) = (last.seq.clone(), last.at);
        let undeleted = self.is_null(merged.cells.cell(at), at);
        let merged = self.rows[index].as_mut().expect("the row was found");
        let cells = mixed(&mut merged.cells, self.columns.count);
        cells[self.columns.seq] = Some(by);
        if undeleted {
            cells[at] = Some(deleted_by);
        }
        merged.seq = seq;
        merged.met = met;
    }

    /// Applies `change` to the row with `key`, when it is sequenced past it: an insert or
    /// update keeping the columns `unchanged` marks, or a delete; returns what it did.
    pub(super) fn apply(
        &mut self,
        key: Vec<u8>,
        change: Change,
        unchanged: Option<&[bool]>,
    ) -> Applied {
        let index = self.by_key.get(&key).copied();
        if let Some(index) = index {
            self.meet(index);
        }
        let current = index.and_then(|index| self.rows[index].as_ref());
        if current.is_some_and(|current| current.seq >= change.seq) {
            return Applied::Nothing;
        }
        let met = self.truncates.len();
        if change.op == ChangeOp::Delete {
            return match (self.columns.at, index.filter(|_| current.is_some())) {
                (None, _) => {
                    if let Some(index) = self.by_key.remove(&key) {
                        self.rows[index] = None;
                    }
                    Applied::Removed
                }
                (Some(at), Some(index)) => {
                    let undeleted = self.is_null(current.and_then(|row| row.cells.cell(at)), at);
                    let kept = self.rows[index].as_mut().expect("the row is current");
                    let cells = mixed(&mut kept.cells, self.columns.count);
                    cells[self.columns.seq] = Some(change.at);
                    if undeleted {
                        cells[at] = Some(change.at);
                    }
                    kept.seq = change.seq;
                    Applied::Nothing
                }
                (Some(_), None) => Applied::Nothing,
            };
        }
        let cells = match unchanged {
            None => Cells::Whole(change.at),
            Some(unchanged) => Cells::Mixed(
                unchanged
                    .iter()
                    .enumerate()
                    .map(|(column, kept)| match (kept, current) {
                        (false, _) => Some(change.at),
                        (true, Some(current)) => current.cells.cell(column),
                        (true, None) => None,
                    })
                    .collect(),
            ),
        };
        let merged = Merged {
            seq: change.seq,
            cells,
            met,
        };
        self.put(key, merged);
        Applied::Held
    }

    /// The rows the table holds once every row has met every truncate, as one batch of `schema`,
    /// each cell taken from where it comes from.
    pub(super) fn assemble(
        mut self,
        schema: &SchemaRef,
        nulls: &mut Nulls,
    ) -> Result<RecordBatch, ArrowError> {
        for index in 0..self.rows.len() {
            self.meet(index);
        }
        let rows: Vec<&Merged> = self.rows.iter().flatten().collect();
        let absent = self.aligned.len() + 1;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for (column, field) in schema.fields().iter().enumerate() {
            let null = nulls.of(field.data_type(), 1);
            let mut sources: Vec<&dyn Array> = vec![self.published.column(column).as_ref()];
            sources.extend(
                self.aligned
                    .iter()
                    .map(|batch| batch.column(column).as_ref()),
            );
            sources.push(null.as_ref());
            let indices: Vec<(usize, usize)> = rows
                .iter()
                .map(|merged| match merged.cells.cell(column) {
                    Some(at) => (at.source, at.row),
                    None => (absent, 0),
                })
                .collect();
            columns.push(interleaved(&sources, &indices, field.data_type(), nulls)?);
        }
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len()));
        RecordBatch::try_new_with_options(std::sync::Arc::clone(schema), columns, &options)
    }
}

/// `cells` as cells each of its own source, which a row a delete or a truncate marks needs.
fn mixed(cells: &mut Cells, count: usize) -> &mut Vec<Option<At>> {
    if let Cells::Whole(at) = *cells {
        *cells = Cells::Mixed(vec![Some(at); count]);
    }
    match cells {
        Cells::Mixed(cells) => cells,
        Cells::Whole(_) => unreachable!("the cells were made mixed above"),
    }
}
