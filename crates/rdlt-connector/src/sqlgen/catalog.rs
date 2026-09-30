//! The catalog tables: epochs, state records, receipts, and the tables and generations written.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::upsert::Row;
use super::{SqlDialect, SqlPlanner, SqlValue, Statement, integer};
use crate::commit::Receipt;
use crate::destination::TableRef;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{CommitSeq, Epoch, LoadId, PipelineId, TablePath};
use crate::state::StateChange;

/// The catalog tables, which destination tables must not be named after.
pub const CATALOG_TABLES: &[&str] = &[
    EPOCHS,
    STATE,
    RECEIPTS,
    TABLES,
    GENERATIONS,
    SEGMENTS,
    OWNERS,
];

const EPOCHS: &str = "_rdlt_epochs";
const STATE: &str = "_rdlt_state";
const RECEIPTS: &str = "_rdlt_receipts";
const TABLES: &str = "_rdlt_tables";
pub(super) const GENERATIONS: &str = "_rdlt_generations";
pub(super) const SEGMENTS: &str = "_rdlt_segments";
const OWNERS: &str = "_rdlt_owners";

impl<D: SqlDialect> SqlPlanner<D> {
    /// Creates the catalog tables where they are missing.
    pub fn bootstrap(&self) -> Vec<Statement> {
        let (text, integer, blob) = (&self.text, &self.integer, &self.blob);
        [
            format!("{EPOCHS} (pipeline {text} PRIMARY KEY, epoch {integer} NOT NULL)"),
            format!(
                "{STATE} (pipeline {text} NOT NULL, key {text} NOT NULL, value {blob} NOT NULL, \
                 PRIMARY KEY (pipeline, key))"
            ),
            format!(
                "{RECEIPTS} (pipeline {text} NOT NULL, load_id {text} NOT NULL, \
                 commit_seq {integer} NOT NULL, committed_at {integer} NOT NULL, \
                 rows {integer} NOT NULL, bytes {integer} NOT NULL, \
                 PRIMARY KEY (pipeline, load_id, commit_seq))"
            ),
            format!("{TABLES} (path {text} PRIMARY KEY, name {text} NOT NULL)"),
            format!("{OWNERS} (name {text} PRIMARY KEY, pipeline {text} NOT NULL)"),
            format!(
                "{GENERATIONS} (name {text} PRIMARY KEY, base {text} NOT NULL, \
                 generation {integer} NOT NULL)"
            ),
            format!(
                "{SEGMENTS} (pipeline {text} NOT NULL, epoch {integer} NOT NULL, \
                 segment {integer} NOT NULL, name {text} NOT NULL, generation {integer}, \
                 merge_key {text}, merge_seq {text}, rows {integer} NOT NULL, \
                 bytes {integer} NOT NULL)"
            ),
        ]
        .into_iter()
        .map(|table| Statement {
            sql: format!("CREATE TABLE IF NOT EXISTS {table}"),
            params: Vec::new(),
        })
        .collect()
    }

    /// Increments `pipeline`'s epoch, starting it at 1; read it back with [`SqlPlanner::epoch`].
    pub fn open(&self, pipeline: &PipelineId) -> Vec<Statement> {
        let key = [("pipeline", SqlValue::Text(pipeline.to_string()))];
        let values = [("epoch", integer(0))];
        let row = Row {
            table: EPOCHS,
            key: &key,
            values: &values,
        };
        let mut plan = self.upsert(&row, false);
        let mut bump = self.sql();
        let name = bump.bind(SqlValue::Text(pipeline.to_string()));
        bump.push(&format!(
            "UPDATE {EPOCHS} SET epoch = epoch + 1 WHERE pipeline = {name}"
        ));
        plan.push(bump.finish());
        plan
    }

    /// The query returning `pipeline`'s epoch.
    pub fn epoch(&self, pipeline: &PipelineId) -> Statement {
        let mut sql = self.sql();
        let name = sql.bind(SqlValue::Text(pipeline.to_string()));
        sql.push(&format!(
            "SELECT epoch FROM {EPOCHS} WHERE pipeline = {name}"
        ));
        sql.finish()
    }

    /// The statement that changes one row exactly when `epoch` is still `pipeline`'s, locking it
    /// until the transaction ends; zero rows changed means a newer session fenced this one.
    pub fn fence(&self, pipeline: &PipelineId, epoch: Epoch) -> Statement {
        let mut sql = self.sql();
        let name = sql.bind(SqlValue::Text(pipeline.to_string()));
        let epoch = sql.bind(integer(epoch.0));
        sql.push(&format!(
            "UPDATE {EPOCHS} SET epoch = epoch WHERE pipeline = {name} AND epoch = {epoch}"
        ));
        sql.finish()
    }

    /// The query returning `pipeline`'s state records as rows of key and value.
    pub fn state(&self, pipeline: &PipelineId) -> Statement {
        let mut sql = self.sql();
        let name = sql.bind(SqlValue::Text(pipeline.to_string()));
        sql.push(&format!(
            "SELECT key, value FROM {STATE} WHERE pipeline = {name} ORDER BY key"
        ));
        sql.finish()
    }

    /// The statements applying `changes` to `pipeline`'s state records, in order.
    pub fn state_changes(&self, pipeline: &PipelineId, changes: &[StateChange]) -> Vec<Statement> {
        changes
            .iter()
            .flat_map(|change| match change {
                StateChange::Put(record) => {
                    let key = [
                        ("pipeline", SqlValue::Text(pipeline.to_string())),
                        ("key", SqlValue::Text(record.key.clone())),
                    ];
                    let values = [("value", SqlValue::Blob(record.value.to_vec()))];
                    let row = Row {
                        table: STATE,
                        key: &key,
                        values: &values,
                    };
                    self.upsert(&row, true)
                }
                StateChange::Delete(key) => {
                    let mut sql = self.sql();
                    let name = sql.bind(SqlValue::Text(pipeline.to_string()));
                    let key = sql.bind(SqlValue::Text(key.clone()));
                    sql.push(&format!(
                        "DELETE FROM {STATE} WHERE pipeline = {name} AND key = {key}"
                    ));
                    vec![sql.finish()]
                }
            })
            .collect()
    }

    /// The query returning the receipt of `(load_id, commit_seq)` as a row of commit time, rows
    /// and bytes; read it with [`receipt`].
    pub fn receipt(
        &self,
        pipeline: &PipelineId,
        load_id: LoadId,
        commit_seq: CommitSeq,
    ) -> Statement {
        let mut sql = self.sql();
        let name = sql.bind(SqlValue::Text(pipeline.to_string()));
        let load = sql.bind(SqlValue::Text(load_id.to_string()));
        let seq = sql.bind(integer(commit_seq.get()));
        sql.push(&format!(
            "SELECT committed_at, rows, bytes FROM {RECEIPTS} WHERE pipeline = {name} \
             AND load_id = {load} AND commit_seq = {seq}"
        ));
        sql.finish()
    }

    /// The statement storing `receipt`; commit times are kept to the microsecond, so build
    /// receipts with [`receipt`] to return the same one again.
    pub fn record_receipt(&self, pipeline: &PipelineId, receipt: &Receipt) -> Statement {
        let mut sql = self.sql();
        let values = [
            SqlValue::Text(pipeline.to_string()),
            SqlValue::Text(receipt.load_id.to_string()),
            integer(receipt.commit_seq.get()),
            integer(micros(receipt.committed_at)),
            integer(receipt.rows),
            integer(receipt.bytes),
        ]
        .map(|value| sql.bind(value));
        sql.push(&format!(
            "INSERT INTO {RECEIPTS} (pipeline, load_id, commit_seq, committed_at, rows, bytes) \
             VALUES ({})",
            values.join(", ")
        ));
        sql.finish()
    }

    /// The statements recording that `table` exists, and for a generation the base table it
    /// replaces.
    pub fn register(&self, table: &TableRef) -> Vec<Statement> {
        let key = [("path", SqlValue::Text(path_key(&table.path)))];
        let values = [("name", SqlValue::Text(table.name.to_string()))];
        let row = Row {
            table: TABLES,
            key: &key,
            values: &values,
        };
        let mut statements = self.upsert(&row, true);
        if let Some(generation) = table.generation {
            let key = [("name", SqlValue::Text(self.target(table)))];
            let values = [
                ("base", SqlValue::Text(table.name.to_string())),
                ("generation", integer(generation.0)),
            ];
            let row = Row {
                table: GENERATIONS,
                key: &key,
                values: &values,
            };
            statements.extend(self.upsert(&row, false));
        }
        statements
    }

    /// The statement claiming the table `name` for `pipeline` where no pipeline owns it yet;
    /// [`SqlPlanner::owner`] then reads who does.
    pub fn claim(&self, pipeline: &PipelineId, name: &str) -> Vec<Statement> {
        let key = [("name", SqlValue::Text(name.to_owned()))];
        let values = [("pipeline", SqlValue::Text(pipeline.to_string()))];
        let row = Row {
            table: OWNERS,
            key: &key,
            values: &values,
        };
        self.upsert(&row, false)
    }

    /// The statements dropping the table `name` with its generation tables `generations`, its
    /// staging and its tombstones, and forgetting its registration, generations, staged segments
    /// and owner, so any pipeline may create a table of that name again.
    ///
    /// Where the dialect's schema changes do not commit with its transactions the drop could not
    /// land with its commit, so it is `Unsupported`.
    pub fn drop_table(&self, name: &str, generations: &[String]) -> Result<Vec<Statement>> {
        if !self.swaps_atomically() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "the dialect's schema changes do not commit with its transactions, so a table \
                 cannot be dropped with a commit",
            ));
        }
        let data = std::iter::once(name.to_owned()).chain(generations.iter().cloned());
        let kept = [self.staging_table(name), self.tombstone_table(name)];
        let mut plan: Vec<Statement> = data
            .clone()
            .chain(kept)
            .map(|table| Statement {
                sql: format!("DROP TABLE IF EXISTS {}", self.quote(&table)),
                params: Vec::new(),
            })
            .collect();
        let forgotten = [(GENERATIONS, "base"), (TABLES, "name"), (OWNERS, "name")]
            .into_iter()
            .map(|(catalog, column)| (catalog, column, name.to_owned()))
            .chain(data.map(|table| (SEGMENTS, "name", table)));
        for (catalog, column, value) in forgotten {
            let mut forget = self.sql();
            let bound = forget.bind(SqlValue::Text(value));
            forget.push(&format!("DELETE FROM {catalog} WHERE {column} = {bound}"));
            plan.push(forget.finish());
        }
        Ok(plan)
    }

    /// The query returning the pipeline that owns the table `name`: the first to claim it.
    pub fn owner(&self, name: &str) -> Statement {
        let mut sql = self.sql();
        let name = sql.bind(SqlValue::Text(name.to_owned()));
        sql.push(&format!(
            "SELECT pipeline FROM {OWNERS} WHERE name = {name}"
        ));
        sql.finish()
    }

    /// The query returning the identifier of every registered table.
    pub fn tables(&self) -> Statement {
        Statement {
            sql: format!("SELECT name FROM {TABLES} ORDER BY name"),
            params: Vec::new(),
        }
    }

    /// The query returning the identifier of the table registered for `path`.
    pub fn table_name(&self, path: &TablePath) -> Statement {
        let mut sql = self.sql();
        let path = sql.bind(SqlValue::Text(path_key(path)));
        sql.push(&format!("SELECT name FROM {TABLES} WHERE path = {path}"));
        sql.finish()
    }

    /// The query returning the generation tables of `base` as rows of identifier and generation.
    pub fn generations(&self, base: &str) -> Statement {
        let mut sql = self.sql();
        let base = sql.bind(SqlValue::Text(base.to_owned()));
        sql.push(&format!(
            "SELECT name, generation FROM {GENERATIONS} WHERE base = {base} ORDER BY name"
        ));
        sql.finish()
    }
}

/// `at` as microseconds since the Unix epoch, the precision receipts keep.
pub fn micros(at: SystemTime) -> u64 {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since.as_micros()).unwrap_or(u64::MAX)
}

/// The receipt a [`SqlPlanner::receipt`] row describes.
pub fn receipt(
    load_id: LoadId,
    commit_seq: CommitSeq,
    committed_at: i64,
    rows: i64,
    bytes: i64,
) -> Receipt {
    let count = |value: i64| u64::try_from(value).unwrap_or_default();
    Receipt {
        load_id,
        commit_seq,
        committed_at: UNIX_EPOCH + Duration::from_micros(count(committed_at)),
        rows: count(rows),
        bytes: count(bytes),
    }
}

/// How the catalog stores a table path: its segments as a JSON array.
fn path_key(path: &TablePath) -> String {
    serde_json::to_string(path).expect("table paths serialize")
}
