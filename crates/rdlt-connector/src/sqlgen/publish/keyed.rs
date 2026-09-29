//! Merging staged rows into a table by key: the published rows of the staged keys replaced by the
//! staged row of each key with the greatest sequence.

#[cfg(test)]
mod tests;

use super::super::{Column, Sql, SqlDialect, SqlPlanner};
use super::Staged;
use crate::commit::SegmentSet;
use crate::destination::MergeKey;
use crate::id::{Epoch, PipelineId};

/// The rows one commit publishes for one table: what `pipeline` staged at `epoch` in `segments`.
pub(super) struct Of<'a> {
    pub(super) staged: &'a Staged,
    pub(super) pipeline: &'a PipelineId,
    pub(super) epoch: Epoch,
    pub(super) segments: &'a SegmentSet,
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statements merging the rows `of` names into `target` by `key`, from `staging`, both of
    /// the `columns` named `names`: the published rows of their keys deleted, then the row of
    /// each key with the greatest sequence inserted.
    ///
    /// Keys are compared a column at a time, as every database does, rather than as row values.
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
        let keys = keys.join(", ");
        let mut replaced = self.sql();
        replaced.push(&format!(
            "DELETE FROM {target} WHERE EXISTS (SELECT 1 FROM {staging} _rdlt_s WHERE "
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
