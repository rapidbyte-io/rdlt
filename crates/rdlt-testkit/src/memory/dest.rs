//! In-memory destination — the reference implementation of the destination contract
//! (clauses D1–D8) and the substrate for crash-injection tests.
//!
//! State survives across sessions (`Arc<Mutex<Inner>>`), which is exactly what lets
//! tests simulate "the process died, a new session opened against the same warehouse".

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use arrow::json::writer::{JsonArray, WriterBuilder};
use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, Destination, DestinationCapabilities,
    DestinationError, LoadSession, OpenCtx, PipelineId, RecordBatch, StateDoc, TableName,
    TableSchema, WriteMode, core::LoadId, core::schema::system_columns,
};
use serde_json::{Map, Value};

/// One committed or staged row: a JSON object keyed by column name, the memory
/// destination's row representation.
pub type Row = Map<String, Value>;

/// Render a batch as JSON rows (explicit nulls) for easy assertions.
fn batch_to_rows(batch: &RecordBatch) -> Vec<Map<String, Value>> {
    let buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(buf);
    writer
        .write(batch)
        .expect("in-memory JSON write cannot fail");
    writer.finish().expect("finish JSON array");
    serde_json::from_slice(&writer.into_inner()).expect("arrow JSON writer emits valid JSON")
}

#[derive(Debug, Default)]
struct Inner {
    /// Reader-visible data (clause D1: only `commit` moves anything here).
    committed: BTreeMap<TableName, Vec<Row>>,
    /// Ordered uncommitted writes of the current session.
    staged: Vec<(TableName, Vec<Row>)>,
    schemas: BTreeMap<TableName, TableSchema>,
    modes: BTreeMap<TableName, WriteMode>,
    state: Option<StateDoc>,
    receipts: BTreeMap<(String, u64), CommitReceipt>,
    /// Tables already truncated by a `Replace` write in the current load — the first
    /// `Replace` batch per table wipes, later batches for it in the same load append.
    truncated_tables: BTreeSet<TableName>,
    /// The load the `truncated_tables` bookkeeping belongs to; a new load resets it.
    truncated_load: Option<LoadId>,
    /// Diagnostics for conformance tests.
    opens: u64,
    staged_dropped_on_open: u64,
}

/// Cloneable handle; every clone shares the same "warehouse".
#[derive(Debug, Clone, Default)]
pub struct MemoryDestination {
    inner: Arc<Mutex<Inner>>,
    capabilities: DestinationCapabilities,
}

impl MemoryDestination {
    /// Full-featured by default (merge, structs, lists, json, decimal) so engine tests
    /// exercise the native paths; degrade with [`Self::with_capabilities`] to test
    /// lowering.
    pub fn new() -> Self {
        Self {
            inner: Arc::default(),
            capabilities: DestinationCapabilities {
                merge: true,
                structs: true,
                scalar_lists: true,
                json_type: true,
                decimal: true,
                ident_rules: Default::default(),
            },
        }
    }

    pub fn with_capabilities(mut self, capabilities: DestinationCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    // ---- test inspection API ----

    /// Reader-visible rows (what a warehouse query would see).
    pub fn committed_rows(&self, table: &str) -> Vec<Row> {
        self.lock()
            .committed
            .get(&TableName::new(table))
            .cloned()
            .unwrap_or_default()
    }

    pub fn committed_tables(&self) -> Vec<TableName> {
        self.lock().committed.keys().cloned().collect()
    }

    pub fn schema(&self, table: &str) -> Option<TableSchema> {
        self.lock().schemas.get(&TableName::new(table)).cloned()
    }

    pub fn state(&self) -> Option<StateDoc> {
        self.lock().state.clone()
    }

    pub fn staged_batches(&self) -> usize {
        self.lock().staged.len()
    }

    pub fn opens(&self) -> u64 {
        self.lock().opens
    }

    pub fn staged_dropped_on_open(&self) -> u64 {
        self.lock().staged_dropped_on_open
    }

    /// Full content snapshot for byte-identical comparisons in crash tests.
    pub fn snapshot(&self) -> BTreeMap<TableName, Vec<Row>> {
        self.lock().committed.clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("memory destination lock")
    }
}

#[async_trait]
impl Destination for MemoryDestination {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("memory-destination", env!("CARGO_PKG_VERSION"))
    }

    fn capabilities(&self) -> DestinationCapabilities {
        self.capabilities
    }

    async fn open(&self, _ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestinationError> {
        let mut inner = self.lock();
        inner.opens += 1;
        // Clause D4: uncommitted staged data from any previous session becomes
        // invisible and reclaimable.
        inner.staged_dropped_on_open += inner.staged.len() as u64;
        inner.staged.clear();
        drop(inner);
        Ok(Box::new(MemorySession {
            inner: Arc::clone(&self.inner),
            ensured: std::collections::BTreeSet::new(),
        }))
    }
}

#[derive(Debug)]
struct MemorySession {
    inner: Arc<Mutex<Inner>>,
    /// Tables ensured on THIS session — real destinations register publishable
    /// tables per session, so writes to un-ensured tables are contract violations
    /// (clause E1) and must fail here too.
    ensured: std::collections::BTreeSet<TableName>,
}

impl MemorySession {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("memory destination lock")
    }
}

#[async_trait]
impl LoadSession for MemorySession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        self.ensured.insert(schema.table.clone());
        let mut inner = self.lock();
        // Clause D5: apply migrations. Widened columns cast existing rows to the new
        // type's representation — the in-memory analogue of `ALTER TABLE … USING`.
        let migrating = inner
            .schemas
            .get(&schema.table)
            .map(|old| old.content_hash() != schema.content_hash())
            .unwrap_or(false);
        if migrating {
            let columns = schema.columns.clone();
            if let Some(rows) = inner.committed.get_mut(&schema.table) {
                for row in rows.iter_mut() {
                    migrate_row(row, &columns);
                }
            }
            for (staged_table, rows) in inner.staged.iter_mut() {
                if staged_table == &schema.table {
                    for row in rows.iter_mut() {
                        migrate_row(row, &columns);
                    }
                }
            }
        }
        inner.schemas.insert(schema.table.clone(), schema.clone());
        inner.modes.insert(schema.table.clone(), mode.clone());
        inner.committed.entry(schema.table.clone()).or_default();
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let rows = batch_to_rows(&batch);
        if !self.ensured.contains(table) {
            return Err(DestinationError::fatal(format!(
                "write before ensure_table for `{table}` ON THIS SESSION (violates clause E1)"
            )));
        }
        let mut inner = self.lock();
        inner.staged.push((table.clone(), rows));
        Ok(())
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let mut inner = self.lock();
        let key = (meta.load_id.as_str().to_owned(), meta.commit_seq);
        // Clause D3: idempotent per (load_id, commit_seq) — return prior receipt,
        // re-publish nothing.
        if let Some(prior) = inner.receipts.get(&key).cloned() {
            inner.staged.clear();
            return Ok(prior);
        }

        // Replace bookkeeping is per load.
        if inner.truncated_load.as_ref() != Some(&meta.load_id) {
            inner.truncated_tables.clear();
            inner.truncated_load = Some(meta.load_id.clone());
        }

        // Merge pass 1: per merge-root table, the staged root ids define which
        // subtrees are being replaced — merge deletes whole subtrees by root id.
        let mut staged_root_ids: BTreeMap<TableName, BTreeSet<String>> = BTreeMap::new();
        for (table, rows) in &inner.staged {
            let is_merge_root = matches!(inner.modes.get(table), Some(WriteMode::Merge { .. }))
                && inner.schemas.get(table).is_none_or(|s| s.parent.is_none());
            if is_merge_root {
                let ids = staged_root_ids.entry(table.clone()).or_default();
                ids.extend(rows.iter().filter_map(|r| {
                    r.get(system_columns::ID)
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                }));
            }
        }

        // Pass 2: apply staged writes in arrival order (clause D2: all-or-nothing is
        // trivial under one mutex). Each write mode has its own algorithm; the match
        // selects it and the extracted `apply_*` functions carry the rules.
        let staged = std::mem::take(&mut inner.staged);
        for (table, rows) in staged {
            let mode = inner
                .modes
                .get(&table)
                .cloned()
                .unwrap_or(WriteMode::Append);
            match mode {
                WriteMode::Append => apply_append(&mut inner, table, rows),
                WriteMode::Replace => apply_replace(&mut inner, table, rows),
                WriteMode::Merge { key }
                    if inner.schemas.get(&table).is_some_and(|s| {
                        s.columns.iter().all(|c| c.name != system_columns::ID)
                    }) =>
                {
                    apply_merge_keyed(&mut inner, table, rows, &key);
                }
                WriteMode::Merge { .. } => {
                    apply_merge_by_id(&mut inner, table, rows, &staged_root_ids);
                }
            }
        }

        // Clause D2: state persists in the same "transaction" as the data.
        inner.state = Some(meta.state.clone());
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        inner.receipts.insert(key, receipt.clone());
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let inner = self.lock();
        Ok(inner.state.clone().filter(|s| &s.pipeline == pipeline))
    }
}

/// Append mode: staged rows are concatenated onto the committed table in arrival
/// order; nothing already committed is touched.
fn apply_append(inner: &mut Inner, table: TableName, rows: Vec<Row>) {
    inner.committed.entry(table).or_default().extend(rows);
}

/// Replace mode: the first `Replace` batch for a table within a load truncates it
/// (recorded in `truncated_tables`); any later batch for the same table in the same
/// load appends, so a multi-batch Replace accumulates instead of repeatedly wiping.
fn apply_replace(inner: &mut Inner, table: TableName, rows: Vec<Row>) {
    if inner.truncated_tables.insert(table.clone()) {
        inner.committed.insert(table, rows);
    } else {
        inner.committed.entry(table).or_default().extend(rows);
    }
}

/// Keyed structured merge: used when the table has no per-row `_rdlt_id` column, so
/// identity is the declared merge `key`. Delete every committed row whose key tuple
/// appears in the staged batch, then append the staged rows deduplicated last-wins by
/// that same key.
fn apply_merge_keyed(inner: &mut Inner, table: TableName, rows: Vec<Row>, key: &[String]) {
    let key_of = |row: &Row| -> String {
        let tuple: Vec<&Value> = key
            .iter()
            .map(|k| row.get(k).unwrap_or(&Value::Null))
            .collect();
        serde_json::to_string(&tuple).expect("key tuple serializes")
    };
    let staged_keys: BTreeSet<String> = rows.iter().map(&key_of).collect();
    let committed = inner.committed.entry(table).or_default();
    committed.retain(|row| !staged_keys.contains(&key_of(row)));
    let mut seen = BTreeSet::new();
    let mut deduped: Vec<Row> = Vec::new();
    for row in rows.into_iter().rev() {
        if seen.insert(key_of(&row)) {
            deduped.push(row);
        }
    }
    deduped.reverse();
    committed.extend(deduped);
}

/// Id-keyed merge: the staged root ids for this table's merge root define which whole
/// subtrees are being replaced. Delete committed rows keyed by `_rdlt_id` (the root
/// table) or `_rdlt_root_id` (a child table) that fall in that set, then append the
/// staged rows deduplicated last-wins by `_rdlt_id`.
fn apply_merge_by_id(
    inner: &mut Inner,
    table: TableName,
    rows: Vec<Row>,
    staged_root_ids: &BTreeMap<TableName, BTreeSet<String>>,
) {
    let root = root_table(&inner.schemas, &table);
    let replaced_root_ids: BTreeSet<String> =
        staged_root_ids.get(&root).cloned().unwrap_or_default();
    let id_column = if table == root {
        system_columns::ID
    } else {
        system_columns::ROOT_ID
    };
    let committed = inner.committed.entry(table).or_default();
    committed.retain(|row| {
        row.get(id_column)
            .and_then(Value::as_str)
            .is_none_or(|id| !replaced_root_ids.contains(id))
    });
    // Upsert semantics also within one staged batch: identical `_rdlt_id`s collapse,
    // last write wins (keyless content-hash dedup).
    let mut seen = BTreeSet::new();
    let mut deduped: Vec<Row> = Vec::new();
    for row in rows.into_iter().rev() {
        let id = row
            .get(system_columns::ID)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if seen.insert(id) {
            deduped.push(row);
        }
    }
    deduped.reverse();
    committed.extend(deduped);
}

/// Cast one stored row to the (possibly widened) column types — the memory analogue
/// of a column-type migration.
fn migrate_row(row: &mut Row, columns: &[rdlt_connector::core::ColumnDef]) {
    for column in columns {
        if let Some(value) = row.get_mut(&column.name) {
            coerce_value(value, &column.column_type);
        }
    }
}

fn coerce_value(value: &mut Value, ty: &rdlt_connector::core::ColumnType) {
    use rdlt_connector::core::{ColumnType, LogicalType};
    match ty {
        ColumnType::Scalar {
            scalar: LogicalType::Utf8,
        } => {
            let text = match &*value {
                Value::String(_) | Value::Null => return,
                Value::Bool(b) => if *b { "true" } else { "false" }.to_owned(),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        i.to_string()
                    } else if let Some(u) = n.as_u64() {
                        u.to_string()
                    } else {
                        n.as_f64().map(|f| f.to_string()).unwrap_or_default()
                    }
                }
                other => serde_json::to_string(other).unwrap_or_default(),
            };
            *value = Value::String(text);
        }
        ColumnType::Scalar {
            scalar: LogicalType::Json,
        } => {
            if !value.is_null() && !value.is_string() {
                *value = Value::String(serde_json::to_string(value).unwrap_or_default());
            }
        }
        ColumnType::Scalar {
            scalar: LogicalType::Float64,
        } => {
            if let Value::Number(n) = value
                && let Some(i) = n.as_i64()
                && let Some(as_float) = serde_json::Number::from_f64(i as f64)
            {
                *value = Value::Number(as_float);
            }
        }
        ColumnType::Struct { fields } => {
            if let Value::Object(map) = value {
                for field in fields {
                    if let Some(inner) = map.get_mut(&field.name) {
                        coerce_value(inner, &field.column_type);
                    }
                }
            }
        }
        ColumnType::ScalarList { item } => {
            if let Value::Array(items) = value {
                let item_ty = ColumnType::Scalar { scalar: *item };
                for entry in items {
                    coerce_value(entry, &item_ty);
                }
            }
        }
        _ => {}
    }
}

/// Walk the parent chain to the root table (child schemas link upward via `parent`).
fn root_table(schemas: &BTreeMap<TableName, TableSchema>, table: &TableName) -> TableName {
    let mut current = table.clone();
    let mut hops = 0;
    while let Some(parent) = schemas.get(&current).and_then(|s| s.parent.as_ref()) {
        current = parent.parent.clone();
        hops += 1;
        if hops > 64 {
            break; // defensive: a schema cycle is a bug elsewhere, don't spin
        }
    }
    current
}
