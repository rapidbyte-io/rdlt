//! The catalog tables: each pipeline's fence row, its state records, and the tables and
//! generations written.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use super::owned::OWNERS;
use super::upsert::Row;
use super::{Owned, SqlDialect, SqlPlanner, SqlValue, Statement, integer};
use crate::commit::{CommitKey, Receipt};
use crate::destination::TableRef;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{Epoch, PipelineId, TablePath};
use crate::state::StateChange;

const EPOCHS: &str = "_rdlt_epochs";
const STATE: &str = "_rdlt_state";
const TABLES: &str = "_rdlt_tables";
pub(super) const GENERATIONS: &str = "_rdlt_generations";
pub(super) const SEGMENTS: &str = "_rdlt_segments";

/// The index a commit finds the segments it publishes or forgets by, and a discard the
/// segments older sessions staged.
const SEGMENTS_STAGED: &str = "_rdlt_segments_staged";

/// Every table of the catalog.
pub(super) const CATALOG: [&str; 6] = [EPOCHS, STATE, TABLES, OWNERS, GENERATIONS, SEGMENTS];

impl<D: SqlDialect> SqlPlanner<D> {
    /// Creates the catalog tables, and the index commits find their segments by, where they are
    /// missing.
    pub fn bootstrap(&self) -> Vec<Statement> {
        let (text, integer, blob) = (&self.text, &self.integer, &self.blob);
        [
            format!(
                "{EPOCHS} (pipeline {text} PRIMARY KEY, epoch {integer} NOT NULL, \
                 landed_epoch {integer}, landed_seq {integer}, landed_at {integer}, \
                 landed_rows {integer}, landed_bytes {integer})"
            ),
            format!(
                "{STATE} (pipeline {text} NOT NULL, key {text} NOT NULL, value {blob} NOT NULL, \
                 PRIMARY KEY (pipeline, key))"
            ),
            format!(
                "{TABLES} (pipeline {text} NOT NULL, path {text} NOT NULL, name {text} NOT NULL, \
                 PRIMARY KEY (pipeline, path))"
            ),
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
        .chain(std::iter::once(Statement {
            sql: self
                .dialect
                .create_index(SEGMENTS_STAGED, SEGMENTS, "pipeline, epoch, segment"),
            params: Vec::new(),
        }))
        .collect()
    }

    /// Increments `pipeline`'s epoch, starting it at 1; read it back with [`SqlPlanner::fence`].
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

    /// The query reading `pipeline`'s fence row, whatever its epoch, under the transaction's
    /// write lock until the transaction ends.
    ///
    /// Its row holds the epoch, then the landed key's epoch, place, commit time, rows and bytes,
    /// null before the first commit lands; read it with [`receipt`] into a
    /// [`Fence`](crate::Fence), which answers a commit.
    pub fn fence(&self, pipeline: &PipelineId) -> Statement {
        let mut sql = self.sql();
        let name = sql.bind(SqlValue::Text(pipeline.to_string()));
        sql.push(&format!(
            "SELECT epoch, landed_epoch, landed_seq, landed_at, landed_rows, landed_bytes \
             FROM {EPOCHS} WHERE pipeline = {name}{}",
            self.dialect.lock_clause()
        ));
        sql.finish()
    }

    /// The statement writing `landed`, the receipt of the commit that applies, into
    /// `pipeline`'s fence row, in the commit's own transaction; commit times are kept to the
    /// microsecond, so build the receipt with [`receipt`] to answer the commit sent again with
    /// the same one.
    pub fn land(&self, pipeline: &PipelineId, landed: &Receipt) -> Statement {
        let mut sql = self.sql();
        let values = [
            integer(landed.key.epoch.0),
            integer(landed.key.seq),
            integer(micros(landed.committed_at)),
            integer(landed.rows),
            integer(landed.bytes),
        ]
        .map(|value| sql.bind(value));
        let name = sql.bind(SqlValue::Text(pipeline.to_string()));
        let [epoch, seq, at, rows, bytes] = values;
        sql.push(&format!(
            "UPDATE {EPOCHS} SET landed_epoch = {epoch}, landed_seq = {seq}, \
             landed_at = {at}, landed_rows = {rows}, landed_bytes = {bytes} \
             WHERE pipeline = {name}"
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

    /// The statements applying `changes` to `pipeline`'s state records: every key a change
    /// names deleted, then the keys whose last change puts a record inserted, each in as few
    /// statements as the dialect's bound on values allows.
    ///
    /// The commit is fenced and the state table keyed by pipeline and key, so this writes what
    /// the changes in order would.
    pub fn state_changes(&self, pipeline: &PipelineId, changes: &[StateChange]) -> Vec<Statement> {
        let mut last: BTreeMap<&str, Option<&Bytes>> = BTreeMap::new();
        for change in changes {
            match change {
                StateChange::Put(record) => last.insert(&record.key, Some(&record.value)),
                StateChange::Delete(key) => last.insert(key, None),
            };
        }
        let most = self.dialect.max_params();
        let keys: Vec<&str> = last.keys().copied().collect();
        let mut plan = Vec::new();
        for chunk in keys.chunks(most.saturating_sub(1).max(1)) {
            let mut sql = self.sql();
            let name = sql.bind(SqlValue::Text(pipeline.to_string()));
            let listed: Vec<String> = chunk
                .iter()
                .map(|key| sql.bind(SqlValue::Text((*key).to_owned())))
                .collect();
            sql.push(&format!(
                "DELETE FROM {STATE} WHERE pipeline = {name} AND key IN ({})",
                listed.join(", ")
            ));
            plan.push(sql.finish());
        }
        let puts: Vec<(&str, &Bytes)> = last
            .iter()
            .filter_map(|(key, value)| value.map(|value| (*key, value)))
            .collect();
        for chunk in puts.chunks((most / 3).max(1)) {
            let mut sql = self.sql();
            let rows: Vec<String> = chunk
                .iter()
                .map(|(key, value)| {
                    let name = sql.bind(SqlValue::Text(pipeline.to_string()));
                    let key = sql.bind(SqlValue::Text((*key).to_owned()));
                    let value = sql.bind(SqlValue::Blob((*value).clone()));
                    format!("({name}, {key}, {value})")
                })
                .collect();
            sql.push(&format!(
                "INSERT INTO {STATE} (pipeline, key, value) VALUES {}",
                rows.join(", ")
            ));
            plan.push(sql.finish());
        }
        plan
    }

    /// The statements recording that `table`, which `owned` names, exists at its path for its
    /// pipeline, and for a generation the base table it replaces.
    ///
    /// Each pipeline keeps its own paths, so no pipeline's table answers for another's path.
    pub(super) fn register(&self, owned: &Owned<'_>, table: &TableRef) -> Result<Vec<Statement>> {
        owned.names(&table.name)?;
        let key = [
            ("pipeline", SqlValue::Text(owned.pipeline().to_string())),
            ("path", SqlValue::Text(path_key(&table.path))),
        ];
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
        Ok(statements)
    }

    /// The statements dropping the table `owned` names with its generation tables `generations`,
    /// its staging and its tombstones, and forgetting its registration, generations, staged
    /// segments and owner, so any pipeline may create a table of that name again.
    ///
    /// Where the dialect's schema changes do not commit with its transactions the drop could not
    /// land with its commit, so it is `Unsupported`.
    pub fn drop_table(&self, owned: &Owned<'_>, generations: &[String]) -> Result<Vec<Statement>> {
        owned.is(owned.name())?;
        if !self.swaps_atomically() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "the dialect's schema changes do not commit with its transactions, so a table \
                 cannot be dropped with a commit",
            ));
        }
        let name = owned.name();
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

    /// The statements forgetting the owner record of the table `owned` names, with its
    /// registration and generations, where the database holds no table for it: nothing is
    /// dropped, and any pipeline may create a table of that name again.
    pub fn release(&self, owned: &Owned<'_>) -> Vec<Statement> {
        [(GENERATIONS, "base"), (TABLES, "name"), (OWNERS, "name")]
            .into_iter()
            .map(|(catalog, column)| {
                let mut forget = self.sql();
                let bound = forget.bind(SqlValue::Text(owned.name().to_owned()));
                forget.push(&format!("DELETE FROM {catalog} WHERE {column} = {bound}"));
                forget.finish()
            })
            .collect()
    }

    /// The query returning the identifier of the table `pipeline` registered for `path`.
    pub fn table_name(&self, pipeline: &PipelineId, path: &TablePath) -> Statement {
        let mut sql = self.sql();
        let pipeline = sql.bind(SqlValue::Text(pipeline.to_string()));
        let path = sql.bind(SqlValue::Text(path_key(path)));
        sql.push(&format!(
            "SELECT name FROM {TABLES} WHERE pipeline = {pipeline} AND path = {path}"
        ));
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

    /// The query returning every generation table as a row of its identifier and its base
    /// table's.
    pub fn generation_tables(&self) -> Statement {
        Statement {
            sql: format!("SELECT name, base FROM {GENERATIONS} ORDER BY name"),
            params: Vec::new(),
        }
    }
}

/// `at` as microseconds since the Unix epoch, the precision receipts keep.
pub fn micros(at: SystemTime) -> u64 {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since.as_micros()).unwrap_or(u64::MAX)
}

/// The receipt of the commit keyed `epoch` and `seq` that a [`SqlPlanner::fence`] row, or the
/// time of a commit and the rows and bytes it published, describe: values as the planner stores
/// them.
pub fn receipt(epoch: i64, seq: i64, committed_at: i64, rows: i64, bytes: i64) -> Receipt {
    let count = |value: i64| u64::try_from(value).unwrap_or_default();
    Receipt {
        key: CommitKey {
            epoch: Epoch(super::unsigned(epoch)),
            seq: super::unsigned(seq),
        },
        committed_at: UNIX_EPOCH + Duration::from_micros(count(committed_at)),
        rows: count(rows),
        bytes: count(bytes),
    }
}

/// How the catalog stores a table path: its segments as a JSON array.
fn path_key(path: &TablePath) -> String {
    serde_json::to_string(path).expect("table paths serialize")
}
