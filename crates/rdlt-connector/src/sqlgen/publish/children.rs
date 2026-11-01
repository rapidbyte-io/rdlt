//! The child tables of a merge table: each indexed by its root id, and published after its root's
//! staged rows decide which of its children replace those published.

use super::super::{Owned, Sql, SqlDialect, SqlPlanner, Statement};
use super::keyed::Of;
use super::keyless;
use crate::destination::{MergeKey, RootKey, TableRef};
use crate::error::{ConnectorError, Result};

impl<D: SqlDialect> SqlPlanner<D> {
    /// The name of the index of the child table `target` by its root id.
    pub(in crate::sqlgen) fn root_index_name(&self, target: &str) -> String {
        self.fitted(format!("_rdlt_root__{target}"))
    }

    /// The statement indexing `table`, which `owned` names, a child table of a merge table, by
    /// its root id, where it is not; nothing for another table.
    ///
    /// It runs where the table's rows are staged, so a commit changes no table's indexes, and each
    /// commit deleting the table's rows by its root id finds them by the index. The index's name
    /// takes the prefix no user table has.
    pub fn root_index(&self, owned: &Owned<'_>, table: &TableRef) -> Result<Option<Statement>> {
        owned.is(&table.name)?;
        let Some(key) = table.merge.as_ref().filter(|key| key.root.is_some()) else {
            return Ok(None);
        };
        let (root_id, _) = child_key(&table.name, key)?;
        let target = self.target(table);
        let name = self.root_index_name(&target);
        let sql = self.dialect.create_index(
            &self.quote(&name),
            &self.quote(&target),
            &self.quote(root_id),
        );
        Ok(Some(Statement {
            sql,
            params: Vec::new(),
        }))
    }

    /// The statement removing the rows of the child table `target` whose roots the root table's
    /// rows staged in the segments `of` names publish.
    pub(super) fn replace_children(
        &self,
        target: &str,
        key: &MergeKey,
        of: &Of<'_>,
    ) -> Result<Statement> {
        let (root_id, root) = child_key(&of.staged.name, key)?;
        self.named(&root.table)?;
        // Root columns are qualified, so one the root staging lacks is an error rather than the
        // child table's column of that name.
        let staging = self.quote(&self.staging_table(&root.table));
        let mut sql = self.sql();
        sql.push(&format!(
            "DELETE FROM {target} WHERE {} IN (SELECT {staging}.{} FROM {staging} WHERE ",
            self.quote(root_id),
            self.quote(&root.id),
        ));
        self.rows_of(&mut sql, of.staged, of.pipeline, of.epoch, of.segments);
        sql.push(")");
        Ok(sql.finish())
    }

    /// Narrows `sql`'s staged child rows to those of each root's winning row: whose root id and
    /// sequence are a staged root row's id and greatest sequence.
    pub(super) fn of_winning_roots(
        &self,
        sql: &mut Sql<'_, D>,
        key: &MergeKey,
        of: &Of<'_>,
    ) -> Result<()> {
        let (root_id, root) = child_key(&of.staged.name, key)?;
        let staging = self.quote(&self.staging_table(&root.table));
        let children = self.quote(&self.staging_table(&of.staged.name));
        let id = format!("{staging}.{}", self.quote(&root.id));
        sql.push(&format!(
            " AND EXISTS (SELECT 1 FROM (SELECT {id} AS _rdlt_id, MAX({staging}.{}) AS _rdlt_newest \
             FROM {staging} WHERE ",
            self.quote(&root.seq),
        ));
        self.rows_of(sql, of.staged, of.pipeline, of.epoch, of.segments);
        sql.push(&format!(
            " GROUP BY {id}) _rdlt_winners WHERE _rdlt_winners._rdlt_id = {children}.{} AND \
             _rdlt_winners._rdlt_newest = {children}.{})",
            self.quote(root_id),
            self.quote(&key.seq),
        ));
        Ok(())
    }
}

/// A child table's root id column and its root.
fn child_key<'a>(table: &str, key: &'a MergeKey) -> Result<(&'a str, &'a RootKey)> {
    match (key.columns.first(), &key.root) {
        (Some(root_id), Some(root)) => Ok((root_id, root)),
        (None, _) => Err(keyless(table)),
        (_, None) => Err(ConnectorError::internal(
            "a child table's key names its root",
        )),
    }
}
