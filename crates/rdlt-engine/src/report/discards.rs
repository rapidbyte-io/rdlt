//! What a schema policy discarded, by table, column and reason: counted as lowering finds it,
//! under the names the plans share, and reported, its names shown, with each commit and run.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;

use crate::error::shown;
use crate::limits::REPORTED_LISTED;

/// Why a schema policy discarded a value, or a row holding one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscardReason {
    /// The value would have changed its table's schema.
    SchemaChange,
    /// Its column cannot hold the value whatever its schema: a time beyond its unit, a decimal
    /// beyond its precision, a change time no version can begin at.
    OutOfRange,
}

/// What a schema policy discarded of one incoming column of a table, for one reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Discard {
    /// The table, by its name in the destination.
    pub table: String,
    /// The incoming column.
    pub column: String,
    /// Why.
    pub reason: DiscardReason,
    /// Rows dropped for holding one of its values: a row that held several counts under each.
    pub rows: u64,
    /// Its values nulled.
    pub values: u64,
}

/// What a stream's schema policy discarded, by table, column and reason: the first
/// [`REPORTED_LISTED`](crate::REPORTED_LISTED) named, the rows and values of the rest counted.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Discards {
    /// The tables' columns and reasons named, in the order they were first discarded.
    pub listed: Vec<Discard>,
    /// Rows dropped for the columns not named.
    pub unlisted_rows: u64,
    /// Values of the columns not named, nulled.
    pub unlisted_values: u64,
}

impl Discards {
    /// Adds what `other` counts: each of its columns to the same one named here, or named once
    /// there is room, or counted.
    pub(crate) fn absorb(&mut self, other: &Self) {
        for discard in &other.listed {
            let same = |listed: &Discard| {
                (&listed.table, &listed.column, listed.reason)
                    == (&discard.table, &discard.column, discard.reason)
            };
            match self.listed.iter().position(same) {
                Some(place) => {
                    let listed = &mut self.listed[place];
                    listed.rows = listed.rows.saturating_add(discard.rows);
                    listed.values = listed.values.saturating_add(discard.values);
                }
                None if self.listed.len() < REPORTED_LISTED => self.listed.push(discard.clone()),
                None => self.unlist(discard.rows, discard.values),
            }
        }
        self.unlist(other.unlisted_rows, other.unlisted_values);
    }

    fn unlist(&mut self, rows: u64, values: u64) {
        self.unlisted_rows = self.unlisted_rows.saturating_add(rows);
        self.unlisted_values = self.unlisted_values.saturating_add(values);
    }
}

/// A table's incoming column and why its values were discarded.
type Key = (Arc<str>, Arc<str>, DiscardReason);

/// What a segment's schema policy discarded, by table, column and reason, under the names the
/// plans share: at most [`REPORTED_LISTED`] kept, the rows and values of the rest counted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ColumnDiscards {
    kept: BTreeMap<Key, (u64, u64)>,
    unlisted: (u64, u64),
}

impl ColumnDiscards {
    /// Counts `rows` dropped and `values` nulled of `table`'s incoming `column`, for `reason`.
    pub(crate) fn add(
        &mut self,
        (table, column): (&Arc<str>, &Arc<str>),
        reason: DiscardReason,
        (rows, values): (u64, u64),
    ) {
        let key = (Arc::clone(table), Arc::clone(column), reason);
        let counts = if self.kept.len() < REPORTED_LISTED || self.kept.contains_key(&key) {
            self.kept.entry(key).or_default()
        } else {
            &mut self.unlisted
        };
        counts.0 = counts.0.saturating_add(rows);
        counts.1 = counts.1.saturating_add(values);
    }

    /// Adds what `other` counts.
    pub(crate) fn absorb(&mut self, other: Self) {
        for ((table, column, reason), counts) in other.kept {
            self.add((&table, &column), reason, counts);
        }
        self.unlisted.0 = self.unlisted.0.saturating_add(other.unlisted.0);
        self.unlisted.1 = self.unlisted.1.saturating_add(other.unlisted.1);
    }

    /// Whether it counts nothing: what it counts past the columns it keeps it counts only once
    /// it keeps as many as it may.
    pub(crate) fn is_empty(&self) -> bool {
        self.kept.is_empty()
    }

    /// What it counts, as a report names it.
    pub(crate) fn reported(&self) -> Discards {
        let listed = self
            .kept
            .iter()
            .map(|((table, column, reason), counts)| Discard {
                table: shown(table),
                column: shown(column),
                reason: *reason,
                rows: counts.0,
                values: counts.1,
            });
        Discards {
            listed: listed.collect(),
            unlisted_rows: self.unlisted.0,
            unlisted_values: self.unlisted.1,
        }
    }
}
