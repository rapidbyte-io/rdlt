//! A history table's versions, the rows they come from, and how a change acts on them.

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef};

use super::super::sparse::At;

/// A row a version's values come from, of the merge's sources.
pub(super) type Row = At;

/// One version of a key, by the rows its values come from.
#[derive(Clone, Copy, Debug)]
pub(super) struct Version {
    /// The row holding its data, key and hash.
    pub(super) data: Row,
    /// The row that published it, holding its sequence and deletion time: `data`, or the delete
    /// of a version a soft delete kept.
    pub(super) opened: Row,
    /// When it begins.
    pub(super) began: i64,
    /// When it ends; none while it is open.
    pub(super) ended: Option<i64>,
    pub(super) current: bool,
    /// Whether the merge opened or closed it: a version it left is its published row whole.
    pub(super) touched: bool,
}

/// A key's newest version's row, which guards a change stream's key, its current version, how
/// many of the table's truncates it has met, and whether one of them opened its newest version.
#[derive(Debug)]
pub(super) struct Key {
    pub(super) newest: Row,
    pub(super) current: Option<usize>,
    /// The latest instant the key's versions hold: when one began or ended.
    pub(super) floor: Option<i64>,
    pub(super) met: usize,
    /// A version a truncate opened carries the truncate's sequence, and a change of its key at
    /// that sequence is not before the truncate: it applies.
    pub(super) truncated: bool,
}

/// The sequences, hashes, validity and deletion times of each of the merge's sources.
pub(super) struct Guards {
    pub(super) seqs: Vec<ArrayRef>,
    pub(super) hashes: Vec<ArrayRef>,
    /// When each row begins and ends, as 64-bit integers.
    pub(super) begins: Vec<ArrayRef>,
    pub(super) ends: Vec<ArrayRef>,
    /// Where deletes are soft, the deletion times.
    pub(super) at: Option<Vec<ArrayRef>>,
}

impl Guards {
    pub(super) fn seq(&self, row: Row) -> &[u8] {
        self.seqs[row.source].as_binary::<i32>().value(row.row)
    }

    pub(super) fn hash(&self, row: Row) -> Option<&[u8]> {
        let hashes = self.hashes[row.source].as_binary::<i32>();
        hashes.is_valid(row.row).then(|| hashes.value(row.row))
    }

    /// When `row` begins.
    pub(super) fn begins(&self, row: Row) -> i64 {
        let begins = self.begins[row.source].as_primitive::<Int64Type>();
        if begins.is_valid(row.row) {
            begins.value(row.row)
        } else {
            i64::MIN
        }
    }

    /// When `row` ends, where it does.
    pub(super) fn ends(&self, row: Row) -> Option<i64> {
        let ends = self.ends[row.source].as_primitive::<Int64Type>();
        ends.is_valid(row.row).then(|| ends.value(row.row))
    }

    /// Whether `row` deleted what it holds.
    pub(super) fn deleted(&self, row: Row) -> bool {
        self.at
            .as_ref()
            .is_some_and(|at| at[row.source].is_valid(row.row))
    }
}

/// A history table's versions, and the rows they come from.
pub(super) struct Versions {
    pub(super) list: Vec<Version>,
    pub(super) sources: Guards,
}

impl Versions {
    /// When `by`, a change of `key` that acts, begins: when it says, or the latest instant the
    /// key held before it, if that is later; which is then the key's latest.
    pub(super) fn begun(&self, key: &mut Key, by: Row) -> i64 {
        let says = self.sources.begins(by);
        let began = key.floor.map_or(says, |floor| floor.max(says));
        key.floor = Some(began);
        began
    }

    /// Closes the version at `index` at `ended`.
    pub(super) fn close(&mut self, index: usize, ended: i64) {
        let version = &mut self.list[index];
        version.ended = Some(ended);
        version.current = false;
        version.touched = true;
    }

    /// Publishes a current version of `data` that `opened` published as `key`'s, beginning at
    /// `began`.
    pub(super) fn open(&mut self, key: &mut Key, (data, opened): (Row, Row), began: i64) {
        key.current = Some(self.list.len());
        self.list.push(Version {
            data,
            opened,
            began,
            ended: None,
            current: true,
            touched: true,
        });
        // A change stream opens a version only past its key's newest; a plain table never asks.
        key.newest = opened;
    }

    /// Applies the upsert `by` to `key`: unless its hash is the current version's, which is not
    /// deleted, it closes that version and becomes the current one.
    pub(super) fn upsert(&mut self, key: &mut Key, by: Row) {
        if let Some(index) = key.current {
            let version = self.list[index];
            let sources = &self.sources;
            if !sources.deleted(version.opened) && sources.hash(version.data) == sources.hash(by) {
                return;
            }
            let began = self.begun(key, by);
            self.close(index, began);
            self.open(key, (by, by), began);
        } else {
            let began = self.begun(key, by);
            self.open(key, (by, by), began);
        }
    }

    /// Applies the delete `by` to `key`'s current version: closes it, and where deletes are
    /// soft, keeps its data in a deleted current version, unless it is deleted already.
    pub(super) fn remove(&mut self, key: &mut Key, by: Row) {
        let Some(index) = key.current else {
            return;
        };
        let version = self.list[index];
        if self.sources.at.is_none() {
            let began = self.begun(key, by);
            self.close(index, began);
            key.current = None;
        } else if !self.sources.deleted(version.opened) {
            let began = self.begun(key, by);
            self.close(index, began);
            self.open(key, (version.data, by), began);
        }
    }
}
