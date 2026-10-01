//! A history table's merge, as `HistoryColumns` says: each key's versions, one
//! closed where the next begins, a change equal to the live version changing nothing.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, BooleanArray, new_null_array};
use arrow_schema::DataType;
use rdlt_connector::{ChangeOp, Deletion, HistoryColumns, MergeKey};
use rdlt_testkit::canon::Canon;

use super::cells::{Stored, compose, text};
use super::tombstones::Tombstones;

/// One history table's merge: its key and history columns.
struct Versioning<'a> {
    key: &'a MergeKey,
    history: &'a HistoryColumns,
}

/// Merges a history stream's `incoming` rows into `published`, every version of each key, and
/// the `tombstones` of a change stream's hard deletes and truncates, as `key` directs.
pub(crate) fn merge_history(
    published: &mut Vec<Stored>,
    tombstones: &mut Tombstones,
    mut incoming: Vec<Stored>,
    key: &MergeKey,
    history: &HistoryColumns,
) {
    let versioning = Versioning { key, history };
    // Sequences are 16 bytes, so their texts order as the bytes do.
    incoming.sort_by_key(|row| text(row, &key.seq));
    for row in incoming {
        versioning.apply(published, tombstones, &row);
    }
}

impl Versioning<'_> {
    fn key_of(&self, row: &Stored) -> String {
        let values: Vec<String> = self
            .key
            .columns
            .iter()
            .map(|column| text(row, column))
            .collect();
        values.join("\u{1}")
    }

    fn op(&self, row: &Stored) -> ChangeOp {
        let changes = self.key.changes.as_ref();
        changes
            .and_then(|changes| row.row.column_by_name(&changes.op))
            .and_then(|ops| arrow_cast::cast(ops, &DataType::Int8).ok())
            .and_then(|ops| ChangeOp::from_code(ops.as_primitive::<Int8Type>().value(0)))
            .unwrap_or(ChangeOp::Update)
    }

    /// The column soft deletes record their time in, where deletes are soft.
    fn at(&self) -> Option<&str> {
        match self.key.changes.as_ref().map(|changes| &changes.deletion) {
            Some(Deletion::Soft { at }) => Some(at),
            _ => None,
        }
    }

    fn current(&self, row: &Stored) -> bool {
        row.cells.get(&*self.history.is_current) == Some(&Canon::Bool(true))
    }

    fn deleted(&self, row: &Stored) -> bool {
        self.at()
            .is_some_and(|at| !matches!(row.cells.get(at), None | Some(Canon::Null)))
    }

    fn apply(&self, published: &mut Vec<Stored>, tombstones: &mut Tombstones, row: &Stored) {
        let seq = text(row, &self.key.seq);
        if self.op(row) == ChangeOp::Truncate {
            if !tombstones.admits(None, &seq) {
                return;
            }
            let before: Vec<usize> = (0..published.len())
                .filter(|index| {
                    let version = &published[*index];
                    self.current(version) && text(version, &self.key.seq) < seq
                })
                .collect();
            for index in before {
                self.remove(published, index, row);
            }
            if self.at().is_none() {
                tombstones.raise(seq);
            }
            return;
        }
        let row_key = self.key_of(row);
        let versions =
            || (0..published.len()).filter(|index| self.key_of(&published[*index]) == row_key);
        if self.key.changes.is_some() {
            let newest = versions()
                .map(|index| text(&published[index], &self.key.seq))
                .max();
            let past = newest.is_none_or(|newest| newest < seq);
            if !past || !tombstones.admits(Some(&row_key), &seq) {
                return;
            }
        }
        let current = versions().find(|index| self.current(&published[*index]));
        if self.op(row) == ChangeOp::Delete {
            if let Some(index) = current {
                self.remove(published, index, row);
            }
            if self.at().is_none() {
                tombstones.bury(row_key, seq);
            }
            return;
        }
        if let Some(index) = current {
            let live = &published[index];
            let hash = &*self.history.row_hash;
            if !self.deleted(live) && text(live, hash) == text(row, hash) {
                return;
            }
            published[index] = self.closed(live, row);
        }
        published.push(self.version(row, None));
    }

    /// Removes the version at `index` as `row`, a delete or truncate, says: closes it, and where
    /// deletes are soft opens a deleted version keeping its data, unless it is deleted already.
    fn remove(&self, published: &mut Vec<Stored>, index: usize, row: &Stored) {
        let version = published[index].clone();
        if self.deleted(&version) {
            return;
        }
        published[index] = self.closed(&version, row);
        if let Some(at) = self.at() {
            published.push(self.version(&version, Some((row, at))));
        }
    }

    /// `version` closed where `row` begins.
    fn closed(&self, version: &Stored, row: &Stored) -> Stored {
        let from = &*self.history.valid_from;
        let parts = version
            .row
            .schema()
            .fields()
            .iter()
            .zip(version.row.columns())
            .map(|(field, column)| {
                let name = field.name().as_str();
                let (column, cell): (ArrayRef, Canon) = if name == &*self.history.valid_to {
                    let begins = row
                        .row
                        .column_by_name(from)
                        .expect("rows say when they begin");
                    let begins =
                        arrow_cast::cast(begins, field.data_type()).expect("validity casts");
                    (begins, row.cells.get(from).cloned().unwrap_or(Canon::Null))
                } else if name == &*self.history.is_current {
                    (
                        Arc::new(BooleanArray::from(vec![false])),
                        Canon::Bool(false),
                    )
                } else {
                    let cell = version.cells.get(name).cloned().unwrap_or(Canon::Null);
                    (Arc::clone(column), cell)
                };
                (field.as_ref().clone(), column, cell)
            })
            .collect();
        compose(parts)
    }

    /// `row`'s stored columns as a current version; with `deleting`, `row` is the version kept,
    /// taking the deleting row's sequence, beginning and deletion time.
    fn version(&self, row: &Stored, deleting: Option<(&Stored, &str)>) -> Stored {
        let op = self.key.changes.as_ref().map(|changes| &*changes.op);
        let taken = [&*self.key.seq, &*self.history.valid_from];
        let parts = row
            .row
            .schema()
            .fields()
            .iter()
            .zip(row.row.columns())
            .filter(|(field, _)| Some(field.name().as_str()) != op)
            .map(|(field, column)| {
                let name = field.name().as_str();
                let source = deleting
                    .filter(|(_, at)| taken.contains(&name) || name == *at)
                    .map_or(row, |(deleting, _)| deleting);
                let (column, cell): (ArrayRef, Canon) = if name == &*self.history.valid_to {
                    (new_null_array(field.data_type(), 1), Canon::Null)
                } else if name == &*self.history.is_current {
                    (Arc::new(BooleanArray::from(vec![true])), Canon::Bool(true))
                } else {
                    let column = source.row.column_by_name(name).map_or_else(
                        || Arc::clone(column),
                        |taken| arrow_cast::cast(taken, field.data_type()).expect("a stored value"),
                    );
                    (
                        column,
                        source.cells.get(name).cloned().unwrap_or(Canon::Null),
                    )
                };
                (field.as_ref().clone(), column, cell)
            })
            .collect();
        compose(parts)
    }
}
