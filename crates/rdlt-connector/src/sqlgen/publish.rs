//! Staging rows, publishing the segments a commit names, swapping generations in, and discarding
//! staging.

use super::catalog::{GENERATIONS, SEGMENTS};
use super::tables::{STAGING_COLUMNS, staging_table};
use super::{Column, Sql, SqlDialect, SqlPlanner, SqlValue, Statement, integer};
use crate::commit::SegmentSet;
use crate::destination::{MergeKey, TableRef};
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
    /// `segment`.
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
            integer(rows),
            integer(bytes),
        ]
        .map(|value| sql.bind(value));
        sql.push(&format!(
            "INSERT INTO {SEGMENTS} (pipeline, epoch, segment, name, generation, rows, bytes) \
             VALUES ({})",
            values.join(", ")
        ));
        sql.finish()
    }

    /// The query returning what `pipeline` staged at `epoch` in `segments`, as rows of table,
    /// generation (or null), rows and bytes.
    pub fn staged(&self, pipeline: &PipelineId, epoch: Epoch, segments: &SegmentSet) -> Statement {
        let mut sql = self.sql();
        sql.push(&format!(
            "SELECT name, generation, SUM(rows), SUM(bytes) FROM {SEGMENTS} WHERE "
        ));
        self.staged_by(&mut sql, pipeline, epoch, segments, SEGMENT_COLUMNS);
        sql.push(" GROUP BY name, generation ORDER BY name, generation");
        sql.finish()
    }

    /// The statements publishing the rows of `staged` that `pipeline` staged at `epoch` in
    /// `segments` into its target, whose columns are `columns`, then removing them from staging;
    /// a target without columns does not exist, which is a `Data` error.
    ///
    /// A merge table keeps one row per key: a staged row replaces the published row with its key,
    /// and among staged rows of one key the greatest sequence wins.
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
        let mut insert = self.sql();
        match &staged.merge {
            None => {
                insert.push(&format!(
                    "INSERT INTO {} ({names}) SELECT {names} FROM {staging} WHERE ",
                    self.quote(&target)
                ));
                self.rows_of(&mut insert, staged, pipeline, epoch, segments);
            }
            Some(key) => {
                let keys: Vec<String> = key.columns.iter().map(|c| self.quote(c)).collect();
                let keys = keys.join(", ");
                insert.push(&format!(
                    "INSERT INTO {} ({names}) SELECT {names} FROM (SELECT {names}, ROW_NUMBER() \
                     OVER (PARTITION BY {keys} ORDER BY {} DESC) AS _rdlt_rank FROM {staging} WHERE ",
                    self.quote(&target),
                    self.quote(&key.seq)
                ));
                self.rows_of(&mut insert, staged, pipeline, epoch, segments);
                let updates: Vec<String> = columns
                    .iter()
                    .filter(|column| !key.columns.iter().any(|k| **k == column.name))
                    .map(|column| {
                        let name = self.quote(&column.name);
                        format!("{name} = excluded.{name}")
                    })
                    .collect();
                let action = if updates.is_empty() {
                    "NOTHING".to_owned()
                } else {
                    format!("UPDATE SET {}", updates.join(", "))
                };
                insert.push(&format!(
                    ") AS _rdlt_ranked WHERE _rdlt_rank = 1 ON CONFLICT ({keys}) DO {action}"
                ));
            }
        }
        let mut delete = self.sql();
        delete.push(&format!("DELETE FROM {staging} WHERE "));
        self.rows_of(&mut delete, staged, pipeline, epoch, segments);
        Ok(vec![insert.finish(), delete.finish()])
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

    /// The statements removing everything `pipeline` staged in the tables `names`.
    pub fn discard(&self, pipeline: &PipelineId, names: &[String]) -> Vec<Statement> {
        let mut plan: Vec<Statement> = names
            .iter()
            .map(|name| {
                let mut sql = self.sql();
                let pipeline = sql.bind(SqlValue::Text(pipeline.to_string()));
                sql.push(&format!(
                    "DELETE FROM {} WHERE {} = {pipeline}",
                    self.quote(&staging_table(name)),
                    self.quote(STAGING_COLUMNS[0])
                ));
                sql.finish()
            })
            .collect();
        let mut sql = self.sql();
        let pipeline = sql.bind(SqlValue::Text(pipeline.to_string()));
        sql.push(&format!(
            "DELETE FROM {SEGMENTS} WHERE pipeline = {pipeline}"
        ));
        plan.push(sql.finish());
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

/// The columns of the segments catalog that say who staged a segment.
const SEGMENT_COLUMNS: [&str; 3] = ["pipeline", "epoch", "segment"];
