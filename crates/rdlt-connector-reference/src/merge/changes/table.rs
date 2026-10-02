//! A change stream's table while its changes apply: each key's row by where its cells come from,
//! and the truncates met so far, applied to a row when it is next touched.

use std::collections::BTreeMap;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_row::Rows;
use arrow_schema::ArrowError;
use rdlt_connector::ChangeOp;

use super::super::met::before;
use super::super::sparse::{At, Base, Pick, Sources, assemble};

/// Where a merged row's cells come from, in the schema's order, but for those set apart.
enum Cells {
    /// Every cell is that of one source row: a row no change of the merge composed.
    Whole(At),
    /// Each cell from its own source row, or null: a row an update flagging columns composed.
    Mixed(Vec<Option<At>>),
}

/// A merged row: its sequence, its cells, how many of the table's truncates it has met, and
/// whether one of them, marking it, gave it its sequence.
struct Merged {
    seq: Vec<u8>,
    cells: Cells,
    /// The cells a deletion marking the row gave it in place of its own, by ascending column:
    /// its sequence and its deletion time, which is all a marked row costs, whatever its width.
    marks: Vec<(usize, Option<At>)>,
    met: usize,
    /// A row a truncate marked carries the truncate's sequence, and a change of its key at that
    /// sequence is not before the truncate: it applies.
    truncated: bool,
}

impl Merged {
    /// The row the cell at `column` comes from; none for a null.
    fn cell(&self, column: usize) -> Option<At> {
        if let Some((_, at)) = self.marks.iter().find(|(marked, _)| *marked == column) {
            return *at;
        }
        match &self.cells {
            Cells::Whole(at) => Some(*at),
            Cells::Mixed(cells) => cells[column],
        }
    }

    /// Gives the row the cell of `at` in `column`, in place of its own.
    fn mark(&mut self, column: usize, at: At) {
        if let Some(mark) = self.marks.iter_mut().find(|(marked, _)| *marked == column) {
            mark.1 = Some(at);
        } else {
            self.marks.push((column, Some(at)));
            self.marks.sort_by_key(|(marked, _)| *marked);
        }
    }
}

/// A truncate the changes applied: its sequence and its row.
struct Truncate {
    seq: Vec<u8>,
    at: At,
}

/// Where a change stream's table keeps its sequence and deletion time.
pub(super) struct Columns {
    pub(super) seq: usize,
    pub(super) at: Option<usize>,
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
    sources: &'a Sources,
}

impl<'a> Table<'a> {
    /// The table holding the first `published` of `sources`, its rows keyed by `keys` and
    /// sequenced by `seqs`, each source's, to which rows of the other sources apply.
    pub(super) fn load(
        sources: &'a Sources,
        published: usize,
        (keys, seqs): (&[Rows], &[ArrayRef]),
        columns: Columns,
    ) -> Self {
        let mut table = Self {
            rows: Vec::new(),
            by_key: BTreeMap::new(),
            truncates: Vec::new(),
            timed: Vec::new(),
            columns,
            sources,
        };
        for source in 0..published {
            let seqs = seqs[source].as_binary::<i32>();
            for row in 0..sources.rows(source) {
                let merged = Merged {
                    seq: seqs.value(row).to_vec(),
                    cells: Cells::Whole(At { source, row }),
                    marks: Vec::new(),
                    met: 0,
                    truncated: false,
                };
                table.put(keys[source].row(row).as_ref().to_vec(), merged);
            }
        }
        table
    }

    /// Whether `cell` of `column` is null.
    fn is_null(&self, cell: Option<At>, column: usize) -> bool {
        cell.is_none_or(|at| self.sources.is_null(at, column))
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
        let (met, Some(last)) = (self.truncates.len(), self.truncates.last()) else {
            return;
        };
        let Some(merged) = self.rows[index].as_ref() else {
            return;
        };
        let pending = &self.truncates[merged.met..];
        let first =
            merged.met + pending.partition_point(|truncate| before(&truncate.seq, &merged.seq));
        // The row keeps when it was deleted; else the first truncate that says when does.
        let timed = self.timed.partition_point(|position| *position < first);
        let deleted_by = self
            .timed
            .get(timed)
            .map_or(last.at, |position| self.truncates[*position].at);
        let (seq, by) = (last.seq.clone(), last.at);
        let undeleted = self
            .columns
            .at
            .is_some_and(|at| self.is_null(merged.cell(at), at));
        let (seq_column, at_column) = (self.columns.seq, self.columns.at);
        let slot = &mut self.rows[index];
        let Some(merged) = slot.as_mut() else {
            return;
        };
        if first == met {
            merged.met = met;
            return;
        }
        let Some(at) = at_column else {
            *slot = None;
            return;
        };
        merged.mark(seq_column, by);
        if undeleted {
            merged.mark(at, deleted_by);
        }
        merged.seq = seq;
        merged.met = met;
        merged.truncated = true;
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
        let behind = |current: &Merged| match current.seq.cmp(&change.seq) {
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => !current.truncated,
            std::cmp::Ordering::Greater => true,
        };
        if current.is_some_and(behind) {
            return Applied::Nothing;
        }
        if change.op == ChangeOp::Delete {
            return self.delete(&key, index, &change);
        }
        let cells = match unchanged {
            None => Cells::Whole(change.at),
            Some(unchanged) => Cells::Mixed(
                unchanged
                    .iter()
                    .enumerate()
                    .map(|(column, kept)| match (kept, current) {
                        (false, _) => Some(change.at),
                        (true, Some(current)) => current.cell(column),
                        (true, None) => None,
                    })
                    .collect(),
            ),
        };
        let merged = Merged {
            seq: change.seq,
            cells,
            marks: Vec::new(),
            met: self.truncates.len(),
            truncated: false,
        };
        self.put(key, merged);
        Applied::Held
    }

    /// Applies the delete `change` to the row at `index`, its key's, where there is one: it is
    /// removed, or where deletes are soft marked deleted then, unless it was already.
    fn delete(&mut self, key: &[u8], index: Option<usize>, change: &Change) -> Applied {
        let Some(at) = self.columns.at else {
            if let Some(index) = self.by_key.remove(key) {
                self.rows[index] = None;
            }
            return Applied::Removed;
        };
        let seq_column = self.columns.seq;
        let Some(index) = index else {
            return Applied::Nothing;
        };
        let undeleted = self.rows[index]
            .as_ref()
            .is_some_and(|row| self.is_null(row.cell(at), at));
        if let Some(kept) = self.rows[index].as_mut() {
            kept.mark(seq_column, change.at);
            if undeleted {
                kept.mark(at, change.at);
            }
            kept.seq.clone_from(&change.seq);
            kept.truncated = false;
        }
        Applied::Nothing
    }

    /// The rows the table holds once every row has met every truncate, as batches of the
    /// columns they hold, each cell taken from where it comes from.
    pub(super) fn assemble(mut self) -> Result<Vec<RecordBatch>, ArrowError> {
        for index in 0..self.rows.len() {
            self.meet(index);
        }
        let picks = self.rows.iter().flatten().map(|merged| Pick {
            base: match &merged.cells {
                Cells::Whole(at) => Base::Row(*at),
                Cells::Mixed(cells) => Base::Cells(cells),
            },
            over: &merged.marks,
        });
        assemble(self.sources, picks)
    }
}
