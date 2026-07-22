//! The file DESTINATION side (absorbed rdlt-connector-parquet, 015 FF1/FF5):
//! parquet or jsonl output to a LOCAL directory or an S3-compatible object
//! store, optional partition column, commit-atomic visibility.
//!
//! Write-only, Append/Replace (no merge — `merge: false`). One directory/
//! prefix per table; staged parts live under `.rdlt-staging/<pipeline>/<load>/`
//! and publication is atomic renames (local) or COPY+DELETE (object store —
//! per-key atomic visibility: a reader can never observe a partial object
//! under a final name), plus a rewrite of the JSON state/receipt files
//! (contract: specs/002-file-arrow-ingestion/contracts/file-connectors.md;
//! honesty note on multi-file set-atomicity in research R18 — recovery
//! converges because staged names are deterministic per (load_id,
//! commit_seq, table, partition, n), with `n` counted PER TABLE+PARTITION so
//! cross-table arrival order cannot change a file's final name).
//!
//! Pipeline scoping: staging, state, and the commit log are all keyed by a
//! hash of the pipeline id, so pipelines sharing one output location cannot
//! clobber each other's staged data, cursors, or receipts (same rule the
//! Postgres destination applies to its stage tables).

pub mod config;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, DestCapabilities, DestError, Destination,
    LoadSession, OpenCtx, RecordBatch, WriteMode,
    core::{
        LoadId, PipelineId, StateDoc, TableName, TableSchema,
        naming::{IdentRules, ident_hash},
    },
};
use serde::{Deserialize, Serialize};

pub use config::{DestFormat, FileDestConfig, dest_config_schema};

const STAGING_DIR: &str = ".rdlt-staging";
pub const LAYOUT_FORMAT_VERSION: u32 = 1;

fn fatal(e: impl std::fmt::Display) -> DestError {
    DestError::fatal(e.to_string())
}
use rdlt_connector::core::crash_point;

/// Fail-point registry (gate G2.2): every `crash_point!` site in this
/// module. The `pq.*` names are the PRESERVED pre-015 set (sweep tooling);
/// the `file.*` points are the object-store finalize boundaries (015 R9).
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &[
    "pq.replace.truncate",
    "pq.staged.sync",
    "pq.part.rename",
    "pq.dir.fsync",
    "pq.state.write",
    "pq.receipt.write",
    "file.stage.put",
    "file.finalize.copy",
    "file.finalize.delete",
];

/// Short stable scope key for one pipeline's files inside a shared output dir.
fn pipeline_scope(pipeline: &PipelineId) -> String {
    ident_hash(pipeline.as_str(), 12)
}

/// Persisted-format identity (persisted-formats contract) — named constants
/// so a product-wide rename is a one-line decision, never a config option.
/// (Format-family destination: deliberately NOT a sqlcore consumer — the SQL
/// naming vocabulary lives there; this crate owns its file-name spellings.)
const STATE_FILE_PREFIX: &str = "_rdlt_state";
const COMMITS_FILE_PREFIX: &str = "_rdlt_commits";

fn state_file(scope: &str) -> String {
    format!("{STATE_FILE_PREFIX}.{scope}.json")
}

fn commits_file(scope: &str) -> String {
    format!("{COMMITS_FILE_PREFIX}.{scope}.json")
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CommitLog {
    #[serde(default)]
    format_version: u32,
    #[serde(default)]
    receipts: Vec<(String, u64)>,
}

/// Fsync a directory so a preceding rename inside it survives power loss.
fn fsync_dir(path: &Path) -> Result<(), DestError> {
    std::fs::File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(fatal)
}

/// Atomic durable JSON rewrite: write-temp + fsync + rename + parent-dir fsync.
/// The data-file path fsyncs before rename too — metadata must not be LESS durable
/// than the parquet parts it describes (clause D2).
fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), DestError> {
    use std::io::Write;
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).map_err(fatal)?;
    let mut file = std::fs::File::create(&tmp).map_err(fatal)?;
    file.write_all(&bytes).map_err(fatal)?;
    file.sync_all().map_err(fatal)?;
    drop(file);
    std::fs::rename(&tmp, path).map_err(fatal)?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Option<T>, DestError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(fatal)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(fatal(e)),
    }
}

/// Where this destination's bytes live.
#[derive(Debug, Clone)]
enum Store {
    Local {
        out: PathBuf,
    },
    S3 {
        store: object_store::aws::AmazonS3,
        prefix: String,
    },
}

impl Store {
    fn s3_key(prefix: &str, tail: &str) -> object_store::path::Path {
        let joined = if prefix.is_empty() {
            tail.to_owned()
        } else {
            format!("{}/{tail}", prefix.trim_end_matches('/'))
        };
        object_store::path::Path::from(joined)
    }

    async fn read_doc(&self, name: &str) -> Result<Option<Vec<u8>>, DestError> {
        match self {
            Self::Local { out } => Ok(read_json::<serde_json::Value>(&out.join(name))?
                .map(|v| serde_json::to_vec(&v).expect("reserialize"))),
            Self::S3 { store, prefix } => match store.get(&Self::s3_key(prefix, name)).await {
                Ok(result) => Ok(Some(result.bytes().await.map_err(fatal)?.to_vec())),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(fatal(e)),
            },
        }
    }

    async fn write_doc<T: Serialize>(&self, name: &str, value: &T) -> Result<(), DestError> {
        match self {
            Self::Local { out } => write_json_atomic(&out.join(name), value),
            Self::S3 { store, prefix } => {
                let bytes = serde_json::to_vec_pretty(value).map_err(fatal)?;
                store
                    .put(
                        &Self::s3_key(prefix, name),
                        bytes::Bytes::from(bytes).into(),
                    )
                    .await
                    .map_err(fatal)?;
                Ok(())
            }
        }
    }

    /// List keys under a tail-prefix (S3) — used for staging cleanup,
    /// replace truncation, and row counting.
    async fn s3_list(&self, tail: &str) -> Result<Vec<object_store::path::Path>, DestError> {
        use futures::StreamExt;
        let Self::S3 { store, prefix } = self else {
            unreachable!("s3_list on a local store");
        };
        let full = Self::s3_key(prefix, tail);
        let mut listing = store.list(Some(&full));
        let mut keys = Vec::new();
        while let Some(entry) = listing.next().await {
            keys.push(entry.map_err(fatal)?.location);
        }
        Ok(keys)
    }
}

#[derive(Debug, Clone)]
pub struct FileDest {
    config: FileDestConfig,
    store: Store,
}

/// The pre-015 name, frozen: `ParquetDir::open(dir)` ≡ local parquet.
pub type ParquetDir = FileDest;

impl FileDest {
    /// Open (creating if needed) a LOCAL output directory as a parquet
    /// destination — the pre-015 constructor, byte-identical behavior.
    pub fn open(out: impl Into<PathBuf>) -> Result<Self, DestError> {
        let out = out.into();
        Self::from_config(FileDestConfig::new(out.to_string_lossy().into_owned()))
    }

    /// The full 015 vocabulary: format, location, partitioning.
    pub fn from_config(config: FileDestConfig) -> Result<Self, DestError> {
        config.validate().map_err(fatal)?;
        let store = match config.location.as_ref().and_then(|l| l.s3.as_ref()) {
            None => {
                let out = PathBuf::from(&config.path);
                std::fs::create_dir_all(&out).map_err(fatal)?;
                Store::Local { out }
            }
            Some(options) => Store::S3 {
                store: crate::location::s3::build_store(options).map_err(fatal)?,
                prefix: config.path.trim_matches('/').to_owned(),
            },
        };
        Ok(Self { config, store })
    }

    /// Test/inspection helper: total published rows of a table (parquet
    /// footers / jsonl line counts), partitions included, staged excluded.
    pub fn count_rows(&self, table: &str) -> Result<u64, DestError> {
        match &self.store {
            Store::Local { out } => count_rows_local(&out.join(table)),
            Store::S3 { .. } => Err(fatal(
                "count_rows is synchronous; use count_rows_async for object stores",
            )),
        }
    }

    /// Async row count (object stores; works for local too).
    pub async fn count_rows_async(&self, table: &str) -> Result<u64, DestError> {
        match &self.store {
            Store::Local { out } => count_rows_local(&out.join(table)),
            Store::S3 { store, .. } => {
                let keys = self.store.s3_list(table).await?;
                let mut total = 0u64;
                for key in keys {
                    let name = key.to_string();
                    if name.ends_with(".parquet") {
                        let bytes = store
                            .get(&key)
                            .await
                            .map_err(fatal)?
                            .bytes()
                            .await
                            .map_err(fatal)?;
                        use parquet::file::reader::{FileReader, SerializedFileReader};
                        let reader = SerializedFileReader::new(bytes).map_err(fatal)?;
                        total += reader.metadata().file_metadata().num_rows() as u64;
                    } else if name.ends_with(".jsonl") {
                        let bytes = store
                            .get(&key)
                            .await
                            .map_err(fatal)?
                            .bytes()
                            .await
                            .map_err(fatal)?;
                        total += bytes.iter().filter(|b| **b == b'\n').count() as u64;
                    }
                }
                Ok(total)
            }
        }
    }
}

/// Recursive local row count (partition subdirectories included).
fn count_rows_local(dir: &Path) -> Result<u64, DestError> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let mut total = 0u64;
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(fatal(e)),
    };
    for entry in entries {
        let path = entry.map_err(fatal)?.path();
        if path.is_dir() {
            total += count_rows_local(&path)?;
        } else if path.extension().is_some_and(|e| e == "parquet") {
            let file = std::fs::File::open(&path).map_err(fatal)?;
            let reader = SerializedFileReader::new(file).map_err(fatal)?;
            total += reader.metadata().file_metadata().num_rows() as u64;
        } else if path.extension().is_some_and(|e| e == "jsonl") {
            let bytes = std::fs::read(&path).map_err(fatal)?;
            total += bytes.iter().filter(|b| **b == b'\n').count() as u64;
        }
    }
    Ok(total)
}

#[async_trait]
impl Destination for FileDest {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new("file", env!("CARGO_PKG_VERSION"));
        spec.config_schema = Some(dest_config_schema());
        spec
    }

    fn capabilities(&self) -> DestCapabilities {
        DestCapabilities {
            merge: false, // write-only destination; no per-row identity semantics
            structs: true,
            scalar_lists: true,
            json_type: false,
            decimal: true,
            ident_rules: IdentRules::default(),
        }
    }

    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError> {
        let scope = pipeline_scope(&ctx.pipeline);
        // Clause D4: staged data from THIS PIPELINE's dead sessions becomes
        // invisible/reclaimable. Scoped — another pipeline sharing this output
        // location keeps its live staged data (the same rule the Postgres
        // destination applies to its stage tables).
        match &self.store {
            Store::Local { out } => {
                let scope_root = out.join(STAGING_DIR).join(&scope);
                if scope_root.exists() {
                    std::fs::remove_dir_all(&scope_root).map_err(fatal)?;
                }
                std::fs::create_dir_all(scope_root.join(ctx.load_id.as_str())).map_err(fatal)?;
            }
            Store::S3 { store, .. } => {
                for key in self
                    .store
                    .s3_list(&format!("{STAGING_DIR}/{scope}"))
                    .await?
                {
                    store.delete(&key).await.map_err(fatal)?;
                }
            }
        }
        Ok(Box::new(FileSession {
            store: self.store.clone(),
            format: self.config.format,
            partition_by: self.config.partition_by.clone(),
            scope,
            load_id: ctx.load_id,
            tables: BTreeMap::new(),
            staged: Vec::new(),
        }))
    }
}

/// One staged part: table, partition value (path-rendered), staged name.
#[derive(Debug, Clone)]
struct StagedPart {
    table: TableName,
    partition: Option<String>,
    name: String,
}

struct FileSession {
    store: Store,
    format: DestFormat,
    partition_by: Option<String>,
    scope: String,
    load_id: LoadId,
    tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    staged: Vec<StagedPart>,
}

impl FileSession {
    fn staging_tail(&self, name: &str) -> String {
        format!("{STAGING_DIR}/{}/{}/{name}", self.scope, self.load_id)
    }

    /// Encode one batch per the configured format.
    fn encode(&self, batch: &RecordBatch) -> Result<Vec<u8>, DestError> {
        match self.format {
            DestFormat::Parquet => {
                let mut buf = Vec::new();
                let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(&batch.schema()), None)
                    .map_err(fatal)?;
                writer.write(batch).map_err(fatal)?;
                writer.close().map_err(fatal)?;
                Ok(buf)
            }
            DestFormat::Jsonl => {
                let mut writer = arrow::json::LineDelimitedWriter::new(Vec::new());
                writer.write(batch).map_err(fatal)?;
                writer.finish().map_err(fatal)?;
                Ok(writer.into_inner())
            }
        }
    }

    /// Split one batch by the partition column (path-safe rendered values;
    /// NULL → `__null__`). Missing column is typed, naming it (FR-009).
    fn split_partitions(
        &self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<Vec<(Option<String>, RecordBatch)>, DestError> {
        let Some(column) = &self.partition_by else {
            return Ok(vec![(None, batch)]);
        };
        let Some((index, _)) = batch.schema().column_with_name(column) else {
            return Err(fatal(format!(
                "partition_by column `{column}` does not exist in stream `{table}` \
                 (columns: {:?})",
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
            )));
        };
        let values = batch.column(index);
        let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let rendered = if values.is_null(row) {
                "__null__".to_owned()
            } else {
                path_safe(&arrow::util::display::array_value_to_string(values, row).map_err(fatal)?)
            };
            groups.entry(rendered).or_default().push(row as u32);
        }
        let mut out = Vec::with_capacity(groups.len());
        for (value, rows) in groups {
            let indices = arrow::array::UInt32Array::from(rows);
            let taken = arrow::compute::take_record_batch(&batch, &indices).map_err(fatal)?;
            out.push((Some(value), taken));
        }
        Ok(out)
    }
}

/// Render a partition value path-safe (never a separator or hidden name).
fn path_safe(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "__empty__".to_owned()
    } else {
        cleaned
    }
}

/// Final data-file tail path for a part.
fn final_tail(
    table: &TableName,
    partition: Option<&str>,
    load: &LoadId,
    seq: u64,
    n: u64,
    extension: &str,
) -> String {
    match partition {
        Some(value) => format!("{table}/{value}/part-{load}-{seq}-{n}.{extension}"),
        None => format!("{table}/part-{load}-{seq}-{n}.{extension}"),
    }
}

#[async_trait]
impl LoadSession for FileSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestError> {
        if matches!(mode, WriteMode::Merge { .. }) {
            return Err(fatal(
                "file destination does not support Merge (capabilities.merge = false)",
            ));
        }
        if let Store::Local { out } = &self.store {
            std::fs::create_dir_all(out.join(schema.table.as_str())).map_err(fatal)?;
        }
        self.tables
            .insert(schema.table.clone(), (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError> {
        if !self.tables.contains_key(table) {
            return Err(fatal(format!(
                "write before ensure_table for `{table}` (clause E1)"
            )));
        }
        // Staged name is deterministic per (table, partition, per-part write
        // index); the FINAL name is assigned at commit (needs commit_seq).
        // Per-table writes arrive in order (clause E1), so recovery replay
        // reproduces both (research R18).
        for (partition, part) in self.split_partitions(table, batch)? {
            let n = self
                .staged
                .iter()
                .filter(|s| s.table == *table && s.partition == partition)
                .count();
            let slug = partition.as_deref().unwrap_or("all");
            let name = format!(
                "{}-{}-{slug}-{n}.{}",
                self.load_id,
                table,
                self.format.extension()
            );
            let bytes = self.encode(&part)?;
            match &self.store {
                Store::Local { out } => {
                    let path = out
                        .join(STAGING_DIR)
                        .join(&self.scope)
                        .join(self.load_id.as_str())
                        .join(&name);
                    std::fs::write(&path, &bytes).map_err(fatal)?;
                }
                Store::S3 { store, prefix } => {
                    crash_point!(
                        "file.stage.put",
                        Err(DestError::fatal("injected crash at file.stage.put"))
                    );
                    store
                        .put(
                            &Store::s3_key(prefix, &self.staging_tail(&name)),
                            bytes::Bytes::from(bytes).into(),
                        )
                        .await
                        .map_err(fatal)?;
                }
            }
            self.staged.push(StagedPart {
                table: table.clone(),
                partition,
                name,
            });
        }
        Ok(())
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let commits_name = commits_file(&self.scope);
        let mut log: CommitLog = match self.store.read_doc(&commits_name).await? {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(fatal)?,
            None => CommitLog::default(),
        };
        let key = (meta.load_id.as_str().to_owned(), meta.commit_seq);

        // Clause D3: idempotent per (load_id, commit_seq) — discard staged,
        // return the prior receipt.
        if log.receipts.contains(&key) {
            let parts: Vec<StagedPart> = self.staged.drain(..).collect();
            for part in parts {
                match &self.store {
                    Store::Local { out } => {
                        let _ = std::fs::remove_file(
                            out.join(STAGING_DIR)
                                .join(&self.scope)
                                .join(self.load_id.as_str())
                                .join(&part.name),
                        );
                    }
                    Store::S3 { store, prefix } => {
                        let _ = store
                            .delete(&Store::s3_key(prefix, &self.staging_tail(&part.name)))
                            .await;
                    }
                }
            }
            return Ok(receipt);
        }

        // Replace: clear each replace-mode table's data files once per load.
        // The guard is DURABLE — "has any earlier commit of THIS load
        // landed?" comes from the receipt log, not session memory, so a
        // crash-recovery session (fresh state, same load_id) never
        // re-truncates files that a prior commit of this load already
        // published. If no receipt landed, re-truncating is convergent: WAL
        // replay re-delivers everything since the last committed checkpoint.
        let load_committed_before = log
            .receipts
            .iter()
            .any(|(load, _)| load == meta.load_id.as_str());
        if !load_committed_before {
            crash_point!(
                "pq.replace.truncate",
                Err(DestError::fatal("injected crash at pq.replace.truncate"))
            );
            for (table, (_, mode)) in &self.tables {
                if matches!(mode, WriteMode::Replace) {
                    match &self.store {
                        Store::Local { out } => {
                            remove_data_files(&out.join(table.as_str()))?;
                        }
                        Store::S3 { store, .. } => {
                            for key in self.store.s3_list(&table.to_string()).await? {
                                let name = key.to_string();
                                if name.ends_with(".parquet") || name.ends_with(".jsonl") {
                                    store.delete(&key).await.map_err(fatal)?;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Publish: staged parts move to their deterministic final names.
        // Local: fsync + rename (atomic visibility per file). Object store:
        // COPY staged→final + DELETE staged — puts and copies are atomic
        // per key, so a reader can never observe a partial object (FF5).
        let mut per_part: BTreeMap<(&TableName, Option<&str>), u64> = BTreeMap::new();
        for part in &self.staged {
            let n = per_part
                .entry((&part.table, part.partition.as_deref()))
                .or_insert(0);
            let tail = final_tail(
                &part.table,
                part.partition.as_deref(),
                &meta.load_id,
                meta.commit_seq,
                *n,
                self.format.extension(),
            );
            *n += 1;
            match &self.store {
                Store::Local { out } => {
                    let from = out
                        .join(STAGING_DIR)
                        .join(&self.scope)
                        .join(self.load_id.as_str())
                        .join(&part.name);
                    let to = out.join(&tail);
                    if let Some(parent) = to.parent() {
                        std::fs::create_dir_all(parent).map_err(fatal)?;
                    }
                    crash_point!(
                        "pq.staged.sync",
                        Err(DestError::fatal("injected crash at pq.staged.sync"))
                    );
                    let file = std::fs::File::open(&from).map_err(fatal)?;
                    file.sync_all().map_err(fatal)?;
                    crash_point!(
                        "pq.part.rename",
                        Err(DestError::fatal("injected crash at pq.part.rename"))
                    );
                    std::fs::rename(&from, &to).map_err(fatal)?;
                }
                Store::S3 { store, prefix } => {
                    let from = Store::s3_key(prefix, &self.staging_tail(&part.name));
                    let to = Store::s3_key(prefix, &tail);
                    crash_point!(
                        "file.finalize.copy",
                        Err(DestError::fatal("injected crash at file.finalize.copy"))
                    );
                    store.copy(&from, &to).await.map_err(fatal)?;
                    crash_point!(
                        "file.finalize.delete",
                        Err(DestError::fatal("injected crash at file.finalize.delete"))
                    );
                    // Idempotent: a replayed finalize may find it gone.
                    match store.delete(&from).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                        Err(e) => return Err(fatal(e)),
                    }
                }
            }
        }
        // Renames are only durable once their directories are — fsync each
        // touched dir before the receipt claims the commit happened (D2).
        // (Object stores have no directory metadata to sync.)
        crash_point!(
            "pq.dir.fsync",
            Err(DestError::fatal("injected crash at pq.dir.fsync"))
        );
        if let Store::Local { out } = &self.store {
            let mut synced = std::collections::BTreeSet::new();
            for part in &self.staged {
                let dir = match &part.partition {
                    Some(value) => out.join(part.table.as_str()).join(value),
                    None => out.join(part.table.as_str()),
                };
                if synced.insert(dir.clone()) {
                    fsync_dir(&dir)?;
                }
            }
        }
        self.staged.clear();

        // State + receipt land last.
        crash_point!(
            "pq.state.write",
            Err(DestError::fatal("injected crash at pq.state.write"))
        );
        self.store
            .write_doc(&state_file(&self.scope), &meta.state)
            .await?;
        log.format_version = LAYOUT_FORMAT_VERSION;
        log.receipts.push(key);
        crash_point!(
            "pq.receipt.write",
            Err(DestError::fatal("injected crash at pq.receipt.write"))
        );
        self.store.write_doc(&commits_name, &log).await?;
        Ok(receipt)
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        let name = state_file(&pipeline_scope(pipeline));
        let state: Option<StateDoc> = match self.store.read_doc(&name).await? {
            Some(bytes) => Some(serde_json::from_slice(&bytes).map_err(fatal)?),
            None => None,
        };
        Ok(state.filter(|s| &s.pipeline == pipeline))
    }
}

/// Remove data files (both extensions) under a table dir, recursively
/// (partition subdirectories included).
fn remove_data_files(dir: &Path) -> Result<(), DestError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(fatal(e)),
    };
    for entry in entries {
        let path = entry.map_err(fatal)?.path();
        if path.is_dir() {
            remove_data_files(&path)?;
        } else if path
            .extension()
            .is_some_and(|e| e == "parquet" || e == "jsonl")
        {
            std::fs::remove_file(path).map_err(fatal)?;
        }
    }
    Ok(())
}
