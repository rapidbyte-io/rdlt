//! Staging rows, publishing the segments a commit names, swapping generations in, and discarding
//! staging.

mod changes;
mod history;
mod keyed;
mod recorded;
mod swap;

pub(super) use keyed::keyless;
use keyed::{Of, holds_key};
use recorded::encode_merge_key;
pub use recorded::merge_key;

use super::catalog::SEGMENTS;
use super::tables::STAGING_COLUMNS;
use super::{Column, Owned, Sql, SqlDialect, SqlPlanner, SqlValue, Statement, integer};
use crate::commit::{ChildTable, SegmentSet};
use crate::destination::{MergeKey, RootKey, TableRef};
use crate::error::{ConnectorError, Result};
use crate::id::{Epoch, GenerationId, PipelineId, SegmentId};

/// Rows staged for one table in a commit's segments: the table, the generation they fill, and how
/// the table merges.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Staged {
    /// The table's identifier.
    pub name: String,
    /// The generation the rows fill; `None` publishes them into the table.
    pub generation: Option<GenerationId>,
    /// How the table merges; `None` appends.
    pub merge: Option<MergeKey>,
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// The statement staging one row of `columns` for `table`, which `owned` names, as part of
    /// `segment`.
    ///
    /// Its parameters hold who staged the row; bind the row's values after them, in the order of
    /// `columns`.
    pub fn stage(
        &self,
        owned: &Owned,
        table: &TableRef,
        epoch: Epoch,
        segment: SegmentId,
        columns: &[&str],
    ) -> Result<Statement> {
        owned.is(&table.name)?;
        let pipeline = owned.pipeline();
        let mut sql = self.sql();
        let generation = table
            .generation
            .map_or(SqlValue::Null, |generation| integer(generation.0));
        let mut values: Vec<String> = [
            SqlValue::Text(pipeline.to_string()),
            integer(epoch.0),
            integer(segment.0),
            generation,
        ]
        .into_iter()
        .map(|value| sql.bind(value))
        .collect();
        let first = values.len() + 1;
        values.extend((first..first + columns.len()).map(|index| self.dialect.placeholder(index)));
        let names: Vec<String> = STAGING_COLUMNS
            .iter()
            .chain(columns)
            .map(|name| self.quote(name))
            .collect();
        sql.push(&format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.quote(&self.staging_table(&table.name)),
            names.join(", "),
            values.join(", ")
        ));
        Ok(sql.finish())
    }

    /// The statement recording that `rows` rows of `bytes` bytes were staged for `table`, which
    /// `owned` names, in `segment`, with how the writer's table merges.
    pub fn record_segment(
        &self,
        owned: &Owned,
        table: &TableRef,
        epoch: Epoch,
        segment: SegmentId,
        [rows, bytes]: [u64; 2],
    ) -> Result<Statement> {
        owned.is(&table.name)?;
        let pipeline = owned.pipeline();
        let mut sql = self.sql();
        let generation = table
            .generation
            .map_or(SqlValue::Null, |generation| integer(generation.0));
        let values = [
            SqlValue::Text(pipeline.to_string()),
            integer(epoch.0),
            integer(segment.0),
            SqlValue::Text(table.name.to_string()),
            generation,
            table
                .merge
                .as_ref()
                .map_or(SqlValue::Null, |key| SqlValue::Text(encode_merge_key(key))),
            table
                .merge
                .as_ref()
                .map_or(SqlValue::Null, |key| SqlValue::Text(key.seq.to_string())),
            integer(rows),
            integer(bytes),
        ]
        .map(|value| sql.bind(value));
        sql.push(&format!(
            "INSERT INTO {SEGMENTS} (pipeline, epoch, segment, name, generation, merge_key, \
             merge_seq, rows, bytes) VALUES ({})",
            values.join(", ")
        ));
        Ok(sql.finish())
    }

    /// The query returning what `pipeline` staged at `epoch` in `segments`, as rows of table,
    /// generation (or null), merge key columns and sequence column (or nulls; read them with
    /// [`merge_key`]), rows and bytes.
    pub fn staged(&self, pipeline: &PipelineId, epoch: Epoch, segments: &SegmentSet) -> Statement {
        let mut sql = self.sql();
        sql.push(&format!(
            "SELECT name, generation, merge_key, merge_seq, SUM(rows), SUM(bytes) FROM {SEGMENTS} \
             WHERE "
        ));
        self.staged_by(&mut sql, pipeline, epoch, segments, SEGMENT_COLUMNS);
        sql.push(
            " GROUP BY name, generation, merge_key, merge_seq \
             ORDER BY name, generation, merge_key, merge_seq",
        );
        sql.finish()
    }

    /// What a commit publishes, in the order it publishes it: `staged`, what
    /// [`SqlPlanner::staged`] returned, and each of `listed`, the child tables the commit lists,
    /// that staged nothing while its root did, since a child table follows its root.
    ///
    /// Child tables come first: they read their roots' staged rows, which a root's publish
    /// removes. Each is published through [`SqlPlanner::publish`], which takes the owner check
    /// of its table.
    pub fn publishing(&self, staged: Vec<Staged>, listed: &[ChildTable]) -> Vec<Staged> {
        let mut publishing = staged;
        for child in listed {
            let root = child.merge.root.as_ref().map(|root| &*root.table);
            let root_staged = publishing
                .iter()
                .any(|staged| Some(staged.name.as_str()) == root);
            let own_staged = publishing.iter().any(|staged| *staged.name == *child.table);
            if root_staged && !own_staged {
                publishing.push(Staged {
                    name: child.table.to_string(),
                    generation: None,
                    merge: Some(child.merge.clone()),
                });
            }
        }
        publishing.sort_by_key(|staged| staged.merge.as_ref().is_none_or(|key| key.root.is_none()));
        publishing
    }

    /// The statements publishing the rows of `staged`, whose table `owned` names, that its
    /// pipeline staged at `epoch` in `segments` into its target, whose columns are `columns`,
    /// then removing them from staging; a target without columns does not exist, which is a
    /// `Data` error.
    ///
    /// A merge keeps one row per key: a staged row replaces the published rows with its key, and
    /// among staged rows of one key the greatest sequence wins. It needs no key index, so a table
    /// that merged before, or never did, merges alike. A child table of a merge table replaces
    /// the children of the roots its root's staged rows publish, reading them from the root's
    /// staging, so it publishes before its root; [`SqlPlanner::root_index`] indexes it by its
    /// root id where its rows are staged, as each such commit deletes its rows by it.
    pub fn publish(
        &self,
        owned: &Owned,
        staged: &Staged,
        columns: &[Column],
        epoch: Epoch,
        segments: &SegmentSet,
    ) -> Result<Vec<Statement>> {
        owned.is(&staged.name)?;
        let of = Of {
            staged,
            pipeline: owned.pipeline(),
            epoch,
            segments,
        };
        let name = match staged.generation {
            Some(generation) => self.generation_table(&staged.name, generation),
            None => staged.name.clone(),
        };
        if columns.is_empty() {
            return Err(ConnectorError::data(format!(
                "table {name} does not exist to publish into"
            )));
        }
        let names: Vec<String> = columns
            .iter()
            .map(|column| self.quote(&column.name))
            .collect();
        let names = names.join(", ");
        let staging = self.quote(&self.staging_table(&staged.name));
        let target = self.quote(&name);
        let tables = [target.as_str(), staging.as_str(), names.as_str()];
        if let Some(key) = &staged.merge {
            holds_key(&name, key, columns)?;
        }
        let mut plan = match &staged.merge {
            None => vec![self.appended(tables, &of).finish()],
            Some(key) if key.root.is_some() => {
                let mut insert = self.appended(tables, &of);
                self.of_winning_roots(&mut insert, key, &of)?;
                vec![self.replace_children(&target, key, &of)?, insert.finish()]
            }
            Some(key) => match (&key.history, &key.changes) {
                (Some(history), _) => self.versioned(&name, (key, history), columns, &of)?,
                (None, Some(changes)) => self.changed(&name, key, changes, columns, &of)?,
                (None, None) => self
                    .merged(tables, key, columns, &of)
                    .map(Sql::finish)
                    .to_vec(),
            },
        };
        let mut delete = self.sql();
        delete.push(&format!("DELETE FROM {staging} WHERE "));
        self.rows_of(&mut delete, staged, of.pipeline, epoch, segments);
        plan.push(delete.finish());
        Ok(plan)
    }

    /// The statement inserting into `target` the `names` of the staged rows `of` names.
    fn appended<'a>(&'a self, [target, staging, names]: [&str; 3], of: &Of<'_>) -> Sql<'a, D> {
        let mut insert = self.sql();
        insert.push(&format!(
            "INSERT INTO {target} ({names}) SELECT {names} FROM {staging} WHERE "
        ));
        self.rows_of(&mut insert, of.staged, of.pipeline, of.epoch, of.segments);
        insert
    }

    /// The name of the index of the child table `target` by its root id.
    pub(super) fn root_index_name(&self, target: &str) -> String {
        self.fitted(format!("_rdlt_root__{target}"))
    }

    /// The statement indexing `table`, which `owned` names, a child table of a merge table, by
    /// its root id, where it is not; nothing for another table.
    ///
    /// It runs where the table's rows are staged, so a commit changes no table's indexes, and each
    /// commit deleting the table's rows by its root id finds them by the index. The index's name
    /// takes the prefix no user table has.
    pub fn root_index(&self, owned: &Owned, table: &TableRef) -> Result<Option<Statement>> {
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
    fn replace_children(&self, target: &str, key: &MergeKey, of: &Of<'_>) -> Result<Statement> {
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
        let roots = root_staged(root, of.staged);
        self.rows_of(&mut sql, &roots, of.pipeline, of.epoch, of.segments);
        sql.push(")");
        Ok(sql.finish())
    }

    /// Narrows `sql`'s staged child rows to those of each root's winning row: whose root id and
    /// sequence are a staged root row's id and greatest sequence.
    fn of_winning_roots(&self, sql: &mut Sql<'_, D>, key: &MergeKey, of: &Of<'_>) -> Result<()> {
        let (root_id, root) = child_key(&of.staged.name, key)?;
        let staging = self.quote(&self.staging_table(&root.table));
        let children = self.quote(&self.staging_table(&of.staged.name));
        let id = format!("{staging}.{}", self.quote(&root.id));
        sql.push(&format!(
            " AND EXISTS (SELECT 1 FROM (SELECT {id} AS _rdlt_id, MAX({staging}.{}) AS _rdlt_newest \
             FROM {staging} WHERE ",
            self.quote(&root.seq),
        ));
        let roots = root_staged(root, of.staged);
        self.rows_of(sql, &roots, of.pipeline, of.epoch, of.segments);
        sql.push(&format!(
            " GROUP BY {id}) _rdlt_winners WHERE _rdlt_winners._rdlt_id = {children}.{} AND \
             _rdlt_winners._rdlt_newest = {children}.{})",
            self.quote(root_id),
            self.quote(&key.seq),
        ));
        Ok(())
    }

    /// The statement forgetting what `pipeline` staged at `epoch` in `segments`, once published.
    pub fn forget(&self, pipeline: &PipelineId, epoch: Epoch, segments: &SegmentSet) -> Statement {
        let mut sql = self.sql();
        sql.push(&format!("DELETE FROM {SEGMENTS} WHERE "));
        self.staged_by(&mut sql, pipeline, epoch, segments, SEGMENT_COLUMNS);
        sql.finish()
    }

    /// The statements removing what sessions of `pipeline` older than `epoch` staged in
    /// `tables`: every table the pipeline owns, as [`SqlPlanner::owned_by`] lists them.
    ///
    /// A newer session's staging stays: a discard can run after a newer session opened and
    /// staged, when it waited behind that session for the database.
    pub fn discard(&self, pipeline: &PipelineId, epoch: Epoch, tables: &[Owned]) -> Vec<Statement> {
        let older = |table: String, [pipeline_column, epoch_column]: [String; 2]| {
            let mut sql = self.sql();
            let pipeline = sql.bind(SqlValue::Text(pipeline.to_string()));
            let epoch = sql.bind(integer(epoch.0));
            sql.push(&format!(
                "DELETE FROM {table} WHERE {pipeline_column} = {pipeline} AND {epoch_column} < {epoch}"
            ));
            sql.finish()
        };
        let staging = [STAGING_COLUMNS[0], STAGING_COLUMNS[1]].map(|column| self.quote(column));
        let mut plan: Vec<Statement> = tables
            .iter()
            .filter(|table| table.pipeline() == pipeline)
            .map(|table| {
                older(
                    self.quote(&self.staging_table(table.name())),
                    staging.clone(),
                )
            })
            .collect();
        plan.push(older(
            SEGMENTS.to_owned(),
            ["pipeline".to_owned(), "epoch".to_owned()],
        ));
        plan
    }

    /// A condition on staging rows: those of `staged`'s generation `pipeline` staged at `epoch`
    /// in `segments`.
    fn rows_of(
        &self,
        sql: &mut Sql<'_, D>,
        staged: &Staged,
        pipeline: &PipelineId,
        epoch: Epoch,
        segments: &SegmentSet,
    ) {
        self.staged_by(
            sql,
            pipeline,
            epoch,
            segments,
            [STAGING_COLUMNS[0], STAGING_COLUMNS[1], STAGING_COLUMNS[2]],
        );
        let generation = self.quote(STAGING_COLUMNS[3]);
        match staged.generation {
            Some(value) => {
                let value = sql.bind(integer(value.0));
                sql.push(&format!(" AND {generation} = {value}"));
            }
            None => sql.push(&format!(" AND {generation} IS NULL")),
        }
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

/// The root table's rows, staged beside a child table's `staged` rows.
fn root_staged(root: &RootKey, staged: &Staged) -> Staged {
    Staged {
        name: root.table.to_string(),
        generation: staged.generation,
        merge: None,
    }
}

/// The columns of the segments catalog that say who staged a segment.
const SEGMENT_COLUMNS: [&str; 3] = ["pipeline", "epoch", "segment"];
