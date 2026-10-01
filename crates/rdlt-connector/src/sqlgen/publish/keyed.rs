//! Merging staged rows into a table by key: the published rows of the staged keys replaced by the
//! staged row of each key with the greatest sequence.

#[cfg(test)]
mod tests;

use super::super::{Column, Sql, SqlDialect, SqlPlanner};
use super::Staged;
use crate::commit::SegmentSet;
use crate::destination::{Deletion, MergeKey, TableRef};
use crate::error::{ConnectorError, Result};
use crate::id::{Epoch, PipelineId};

/// The rows one commit publishes for one table: what `pipeline` staged at `epoch` in `segments`.
pub(super) struct Of<'a> {
    pub(super) staged: &'a Staged,
    pub(super) pipeline: &'a PipelineId,
    pub(super) epoch: Epoch,
    pub(super) segments: &'a SegmentSet,
}

/// The error for a merge key naming no column: a `Data` error coded `merge_key_invalid`.
pub(in crate::sqlgen) fn keyless(table: &str) -> ConnectorError {
    ConnectorError::data(format!("table {table} is merged by a key of no column"))
        .with_code("merge_key_invalid")
}

/// Refuses `key` unless `columns`, the columns of the table `table`, hold every column it names
/// there: its key columns, of which it has at least one, its sequence, where deletes are soft
/// their deletion time, and a history table's history columns.
///
/// Every merge is planned from a key checked so, since a name that is no column is, to some
/// databases, a text: a `Data` error coded `merge_key_invalid`.
pub(in crate::sqlgen) fn holds_key(table: &str, key: &MergeKey, columns: &[Column]) -> Result<()> {
    if key.columns.is_empty() {
        return Err(keyless(table));
    }
    // A child table is keyed by its root id alone, its first key column.
    let keys = match &key.root {
        Some(_) => &key.columns[..1],
        None => &key.columns[..],
    };
    let at = key
        .changes
        .as_ref()
        .and_then(|changes| match &changes.deletion {
            Deletion::Soft { at } => Some(at),
            Deletion::Hard => None,
        });
    let history = key.history.iter().flat_map(|history| {
        [
            &history.valid_from,
            &history.valid_to,
            &history.is_current,
            &history.row_hash,
        ]
    });
    let named = keys.iter().chain([&key.seq]).chain(at).chain(history);
    for name in named {
        if !columns.iter().any(|column| *column.name == **name) {
            return Err(ConnectorError::data(format!(
                "table {table} has no column {name}, which its merge key names"
            ))
            .with_code("merge_key_invalid"));
        }
    }
    Ok(())
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// Refuses a writer of `table` unless `columns`, the columns the table has, hold every column
    /// its merge key names there, as [`SqlPlanner::publish`] refuses its commit: a `Data` error
    /// coded `merge_key_invalid`; a table that does not merge has no key to hold.
    pub fn merges(&self, table: &TableRef, columns: &[Column]) -> Result<()> {
        match &table.merge {
            Some(key) => holds_key(&table.name, key, columns),
            None => Ok(()),
        }
    }

    /// The statements merging the rows `of` names into `target` by `key`, from `staging`, both of
    /// the `columns` named `names`: the published rows of their keys deleted, then the row of
    /// each key with the greatest sequence inserted.
    ///
    /// Keys are compared a column at a time, as every database does, rather than as row values,
    /// and the rows a merge replaces are found through [`SqlPlanner::key_indexes`].
    pub(super) fn merged<'a>(
        &'a self,
        [target, staging, names]: [&str; 3],
        key: &MergeKey,
        columns: &[Column],
        of: &Of<'_>,
    ) -> [Sql<'a, D>; 2] {
        let keys: Vec<String> = key.columns.iter().map(|c| self.quote(c)).collect();
        let same: Vec<String> = keys
            .iter()
            .map(|key| format!("_rdlt_s.{key} = {target}.{key}"))
            .collect();
        // The staged keys' first column finds the rows through the key's index, then every
        // column is matched.
        let first = &keys[0];
        let keys = keys.join(", ");
        let mut replaced = self.sql();
        replaced.push(&format!(
            "DELETE FROM {target} WHERE {target}.{first} IN (SELECT _rdlt_s.{first} FROM {staging} \
             _rdlt_s WHERE "
        ));
        self.rows_of(&mut replaced, of.staged, of.pipeline, of.epoch, of.segments);
        replaced.push(&format!(
            ") AND EXISTS (SELECT 1 FROM {staging} _rdlt_s WHERE "
        ));
        self.rows_of(&mut replaced, of.staged, of.pipeline, of.epoch, of.segments);
        replaced.push(&format!(" AND {})", same.join(" AND ")));
        let rank = self.quote(&unused("_rdlt_rank", columns));
        let mut insert = self.sql();
        insert.push(&format!(
            "INSERT INTO {target} ({names}) SELECT {names} FROM (SELECT {names}, \
             ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {} DESC) AS {rank} \
             FROM {staging} WHERE ",
            self.quote(&key.seq)
        ));
        self.rows_of(&mut insert, of.staged, of.pipeline, of.epoch, of.segments);
        insert.push(&format!(") _rdlt_ranked WHERE {rank} = 1"));
        [replaced, insert]
    }
}

/// `name`, or `name` followed by underscores, whichever no column of `columns` is named, compared
/// without case as databases may compare identifiers.
pub(super) fn unused(name: &str, columns: &[Column]) -> String {
    let mut candidate = name.to_owned();
    while columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(&candidate))
    {
        candidate.push('_');
    }
    candidate
}
