//! Staging rows, publishing the segments a commit names, swapping generations in, and discarding
//! staging.

use super::catalog::{GENERATIONS, SEGMENTS};
use super::tables::{STAGING_COLUMNS, staging_table};
use super::{Column, Sql, SqlDialect, SqlPlanner, SqlValue, Statement, integer};
use crate::commit::SegmentSet;
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
    /// The statement staging one row of `columns` for `table` as part of `segment`.
    ///
    /// Its parameters hold who staged the row; bind the row's values after them, in the order of
    /// `columns`.
    pub fn stage(
        &self,
        table: &TableRef,
        pipeline: &PipelineId,
        epoch: Epoch,
        segment: SegmentId,
        columns: &[&str],
    ) -> Statement {
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
            self.quote(&staging_table(&table.name)),
            names.join(", "),
            values.join(", ")
        ));
        sql.finish()
    }

    /// The statement recording that `rows` rows of `bytes` bytes were staged for `table` in
    /// `segment`, with how the writer's table merges.
    pub fn record_segment(
        &self,
        table: &TableRef,
        pipeline: &PipelineId,
        epoch: Epoch,
        segment: SegmentId,
        [rows, bytes]: [u64; 2],
    ) -> Statement {
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
        sql.finish()
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

    /// The statements publishing the rows of `staged` that `pipeline` staged at `epoch` in
    /// `segments` into its target, whose columns are `columns`, then removing them from staging;
    /// a target without columns does not exist, which is a `Data` error.
    ///
    /// A merge keeps one row per key: a staged row replaces the published rows with its key, and
    /// among staged rows of one key the greatest sequence wins. It needs no key index, so a table
    /// that merged before, or never did, merges alike. A child table of a merge table replaces
    /// the children of the roots its root's staged rows publish, reading them from the root's
    /// staging, so it publishes before its root.
    pub fn publish(
        &self,
        staged: &Staged,
        columns: &[Column],
        pipeline: &PipelineId,
        epoch: Epoch,
        segments: &SegmentSet,
    ) -> Result<Vec<Statement>> {
        let target = match staged.generation {
            Some(generation) => super::tables::generation_table(&staged.name, generation),
            None => staged.name.clone(),
        };
        if columns.is_empty() {
            return Err(ConnectorError::data(format!(
                "table {target} does not exist to publish into"
            )));
        }
        let names: Vec<String> = columns
            .iter()
            .map(|column| self.quote(&column.name))
            .collect();
        let names = names.join(", ");
        let staging = self.quote(&staging_table(&staged.name));
        let target = self.quote(&target);
        let mut plan = Vec::new();
        let mut insert = self.sql();
        match &staged.merge {
            None => {
                insert.push(&format!(
                    "INSERT INTO {target} ({names}) SELECT {names} FROM {staging} WHERE "
                ));
                self.rows_of(&mut insert, staged, pipeline, epoch, segments);
            }
            Some(key) if key.root.is_some() => {
                plan.push(self.replace_children(&target, staged, key, pipeline, epoch, segments)?);
                insert.push(&format!(
                    "INSERT INTO {target} ({names}) SELECT {names} FROM {staging} WHERE "
                ));
                self.rows_of(&mut insert, staged, pipeline, epoch, segments);
                self.of_winning_roots(&mut insert, staged, key, pipeline, epoch, segments)?;
            }
            Some(key) => {
                let keys: Vec<String> = key.columns.iter().map(|c| self.quote(c)).collect();
                let keys = keys.join(", ");
                let mut replaced = self.sql();
                replaced.push(&format!(
                    "DELETE FROM {target} WHERE ({keys}) IN (SELECT {keys} FROM {staging} WHERE "
                ));
                self.rows_of(&mut replaced, staged, pipeline, epoch, segments);
                replaced.push(")");
                plan.push(replaced.finish());
                insert.push(&format!(
                    "INSERT INTO {target} ({names}) SELECT {names} FROM (SELECT {names}, \
                     ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {} DESC) AS _rdlt_rank \
                     FROM {staging} WHERE ",
                    self.quote(&key.seq)
                ));
                self.rows_of(&mut insert, staged, pipeline, epoch, segments);
                insert.push(") AS _rdlt_ranked WHERE _rdlt_rank = 1");
            }
        }
        plan.push(insert.finish());
        let mut delete = self.sql();
        delete.push(&format!("DELETE FROM {staging} WHERE "));
        self.rows_of(&mut delete, staged, pipeline, epoch, segments);
        plan.push(delete.finish());
        Ok(plan)
    }

    /// The statement removing the rows of the child table `target` whose roots the root table's
    /// rows staged in `segments` publish.
    fn replace_children(
        &self,
        target: &str,
        staged: &Staged,
        key: &MergeKey,
        pipeline: &PipelineId,
        epoch: Epoch,
        segments: &SegmentSet,
    ) -> Result<Statement> {
        let (owner, root) = child_key(key)?;
        // Root columns are qualified, so one the root staging lacks is an error rather than the
        // child table's column of that name.
        let staging = self.quote(&staging_table(&root.table));
        let mut sql = self.sql();
        sql.push(&format!(
            "DELETE FROM {target} WHERE {} IN (SELECT {staging}.{} FROM {staging} WHERE ",
            self.quote(owner),
            self.quote(&root.id),
        ));
        self.rows_of(
            &mut sql,
            &root_staged(root, staged),
            pipeline,
            epoch,
            segments,
        );
        sql.push(")");
        Ok(sql.finish())
    }

    /// Narrows `sql`'s staged child rows to those of each root's winning row: whose root id and
    /// sequence are a staged root row's id and greatest sequence.
    fn of_winning_roots(
        &self,
        sql: &mut Sql<'_, D>,
        staged: &Staged,
        key: &MergeKey,
        pipeline: &PipelineId,
        epoch: Epoch,
        segments: &SegmentSet,
    ) -> Result<()> {
        let (owner, root) = child_key(key)?;
        let staging = self.quote(&staging_table(&root.table));
        let id = format!("{staging}.{}", self.quote(&root.id));
        sql.push(&format!(
            " AND ({}, {}) IN (SELECT {id}, MAX({staging}.{}) FROM {staging} WHERE ",
            self.quote(owner),
            self.quote(&key.seq),
            self.quote(&root.seq),
        ));
        self.rows_of(sql, &root_staged(root, staged), pipeline, epoch, segments);
        sql.push(&format!(" GROUP BY {id})"));
        Ok(())
    }

    /// The statement forgetting what `pipeline` staged at `epoch` in `segments`, once published.
    pub fn forget(&self, pipeline: &PipelineId, epoch: Epoch, segments: &SegmentSet) -> Statement {
        let mut sql = self.sql();
        sql.push(&format!("DELETE FROM {SEGMENTS} WHERE "));
        self.staged_by(&mut sql, pipeline, epoch, segments, SEGMENT_COLUMNS);
        sql.finish()
    }

    /// The statements swapping `generation` in as the table `base`, dropping every other
    /// generation of it; `generations` are the base's generation tables and `base_exists` says
    /// whether the base table does.
    ///
    /// A generation that has no table leaves the base table empty.
    pub fn swap(
        &self,
        base: &str,
        base_exists: bool,
        generation: GenerationId,
        generations: &[(String, GenerationId)],
    ) -> Vec<Statement> {
        let statement = |sql: String| Statement {
            sql,
            params: Vec::new(),
        };
        let mut plan = Vec::new();
        let swapped = generations.iter().find(|(_, found)| *found == generation);
        match swapped {
            Some((name, _)) => {
                plan.push(statement(format!(
                    "DROP TABLE IF EXISTS {}",
                    self.quote(base)
                )));
                plan.push(statement(format!(
                    "ALTER TABLE {} RENAME TO {}",
                    self.quote(name),
                    self.quote(base)
                )));
            }
            None if base_exists => {
                plan.push(statement(format!("DELETE FROM {}", self.quote(base))));
            }
            None => {}
        }
        for (name, _) in generations.iter().filter(|(_, found)| *found != generation) {
            plan.push(statement(format!(
                "DROP TABLE IF EXISTS {}",
                self.quote(name)
            )));
        }
        let mut forget = self.sql();
        let base = forget.bind(SqlValue::Text(base.to_owned()));
        forget.push(&format!("DELETE FROM {GENERATIONS} WHERE base = {base}"));
        plan.push(forget.finish());
        plan
    }

    /// The statements removing what sessions of `pipeline` older than `epoch` staged in the
    /// tables `names`.
    ///
    /// A newer session's staging stays: a discard can run after a newer session opened and
    /// staged, when it waited behind that session for the database.
    pub fn discard(&self, pipeline: &PipelineId, epoch: Epoch, names: &[String]) -> Vec<Statement> {
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
        let mut plan: Vec<Statement> = names
            .iter()
            .map(|name| older(self.quote(&staging_table(name)), staging.clone()))
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

/// How a staged segment records a merge key: its key columns as a JSON array, or, for a child
/// table, an object of its key columns and its root.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum RecordedKey {
    Columns(Vec<String>),
    Child {
        columns: Vec<String>,
        root: RecordedRoot,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RecordedRoot {
    table: String,
    id: String,
    seq: String,
}

/// `key`'s columns and root as a staged segment records them.
fn encode_merge_key(key: &MergeKey) -> String {
    let columns = key.columns.iter().map(ToString::to_string).collect();
    let recorded = match &key.root {
        None => RecordedKey::Columns(columns),
        Some(root) => RecordedKey::Child {
            columns,
            root: RecordedRoot {
                table: root.table.to_string(),
                id: root.id.to_string(),
                seq: root.seq.to_string(),
            },
        },
    };
    serde_json::to_string(&recorded).expect("merge keys serialize")
}

/// The merge key a [`SqlPlanner::staged`] row records: its key columns, a JSON array or an object
/// of the columns and the root of a child table, and its sequence column.
pub fn merge_key(columns: &str, seq: &str) -> Result<MergeKey> {
    let recorded: RecordedKey = serde_json::from_str(columns).map_err(|error| {
        ConnectorError::internal(format!("a staged merge key is not recorded JSON: {error}"))
    })?;
    let (columns, root) = match recorded {
        RecordedKey::Columns(columns) => (columns, None),
        RecordedKey::Child { columns, root } => {
            let root = RootKey {
                table: root.table.into(),
                id: root.id.into(),
                seq: root.seq.into(),
            };
            (columns, Some(root))
        }
    };
    Ok(MergeKey {
        columns: columns.into_iter().map(Into::into).collect(),
        seq: seq.into(),
        root,
    })
}

/// A child table's root id column and its root.
fn child_key(key: &MergeKey) -> Result<(&str, &RootKey)> {
    match (key.columns.first(), &key.root) {
        (Some(owner), Some(root)) => Ok((owner, root)),
        _ => Err(ConnectorError::internal(
            "a child table's key names its root id and its root",
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
