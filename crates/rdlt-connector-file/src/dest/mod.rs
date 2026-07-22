//! The file DESTINATION side (absorbed rdlt-connector-parquet, 015 FF1):
//! parquet-file output — temp-dir staging, atomic rename publication.
//!
//! Write-only, Append/Replace (no merge — `merge: false`). One directory per table;
//! staged batches live under `.rdlt-staging/<pipeline>/<load>/` and publication is a
//! set of atomic renames plus a rewrite of the JSON state/receipt files (contract:
//! specs/002-file-arrow-ingestion/contracts/file-connectors.md; honesty note on
//! multi-file set-atomicity in research R18 — recovery converges because staged
//! names are deterministic per (load_id, commit_seq, table, n), with `n` counted
//! PER TABLE so cross-table arrival order cannot change a file's final name).
//!
//! Pipeline scoping: staging, state, and the commit log are all keyed by a hash of
//! the pipeline id, so pipelines sharing one output directory cannot clobber each
//! other's staged data, cursors, or receipts (same rule the Postgres destination
//! applies to its stage tables).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
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

const STAGING_DIR: &str = ".rdlt-staging";
pub const LAYOUT_FORMAT_VERSION: u32 = 1;

fn fatal(e: impl std::fmt::Display) -> DestError {
    DestError::fatal(e.to_string())
}
use rdlt_connector::core::crash_point;

/// Fail-point registry (gate G2.2): every `crash_point!` site in this crate.
/// The macro is defined once in `rdlt_core::failpoint`.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &[
    "pq.replace.truncate",
    "pq.staged.sync",
    "pq.part.rename",
    "pq.dir.fsync",
    "pq.state.write",
    "pq.receipt.write",
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

#[derive(Debug, Clone)]
pub struct ParquetDir {
    out: PathBuf,
}

impl ParquetDir {
    /// Open (creating if needed) an output directory as a destination.
    pub fn open(out: impl Into<PathBuf>) -> Result<Self, DestError> {
        let out = out.into();
        std::fs::create_dir_all(&out).map_err(fatal)?;
        Ok(Self { out })
    }

    /// Test/inspection helper: total published rows of a table (parquet footers).
    pub fn count_rows(&self, table: &str) -> Result<u64, DestError> {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let dir = self.out.join(table);
        let mut total = 0u64;
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(fatal(e)),
        };
        for entry in entries {
            let path = entry.map_err(fatal)?.path();
            if path.extension().is_some_and(|e| e == "parquet") {
                let file = std::fs::File::open(&path).map_err(fatal)?;
                let reader = SerializedFileReader::new(file).map_err(fatal)?;
                total += reader.metadata().file_metadata().num_rows() as u64;
            }
        }
        Ok(total)
    }
}

#[async_trait]
impl Destination for ParquetDir {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("parquet-dir", env!("CARGO_PKG_VERSION"))
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
        // directory keeps its live staged data (the same rule the Postgres
        // destination applies to its stage tables).
        let scope_root = self.out.join(STAGING_DIR).join(&scope);
        if scope_root.exists() {
            std::fs::remove_dir_all(&scope_root).map_err(fatal)?;
        }
        let staging = scope_root.join(ctx.load_id.as_str());
        std::fs::create_dir_all(&staging).map_err(fatal)?;
        Ok(Box::new(ParquetSession {
            out: self.out.clone(),
            staging,
            scope,
            load_id: ctx.load_id,
            tables: BTreeMap::new(),
            staged: Vec::new(),
        }))
    }
}

struct ParquetSession {
    out: PathBuf,
    staging: PathBuf,
    scope: String,
    load_id: LoadId,
    tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    /// Staged batches in arrival order: (table, staged file name).
    staged: Vec<(TableName, String)>,
}

#[async_trait]
impl LoadSession for ParquetSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestError> {
        if matches!(mode, WriteMode::Merge { .. }) {
            return Err(fatal(
                "parquet destination does not support Merge (capabilities.merge = false)",
            ));
        }
        std::fs::create_dir_all(self.out.join(schema.table.as_str())).map_err(fatal)?;
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
        // Staged name is deterministic per (table, per-table write index); the FINAL
        // name is assigned at commit (needs commit_seq). Per-table writes arrive in
        // order (clause E1), so recovery replay reproduces both (research R18).
        let n = self.staged.iter().filter(|(t, _)| t == table).count();
        let name = format!("{}-{}-{n}.parquet", self.load_id, table);
        let path = self.staging.join(&name);
        let file = std::fs::File::create(&path).map_err(fatal)?;
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&batch.schema()), None).map_err(fatal)?;
        writer.write(&batch).map_err(fatal)?;
        let file = writer.close().map_err(fatal).map(|_| ())?;
        let _ = file;
        self.staged.push((table.clone(), name));
        Ok(())
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let commits_path = self.out.join(commits_file(&self.scope));
        let mut log: CommitLog = read_json(&commits_path)?.unwrap_or_default();
        let key = (meta.load_id.as_str().to_owned(), meta.commit_seq);

        // Clause D3: idempotent per (load_id, commit_seq) — discard staged, return
        // the prior receipt.
        if log.receipts.contains(&key) {
            for (_, name) in self.staged.drain(..) {
                let _ = std::fs::remove_file(self.staging.join(name));
            }
            return Ok(receipt);
        }

        // Replace: clear each replace-mode table's data files once per load. The
        // guard is DURABLE — "has any earlier commit of THIS load landed?" comes
        // from the receipt log, not session memory, so a crash-recovery session
        // (fresh state, same load_id) never re-truncates files that a prior commit
        // of this load already published. If no receipt landed, re-truncating is
        // convergent: WAL replay re-delivers everything since the last committed
        // checkpoint.
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
                    let dir = self.out.join(table.as_str());
                    if let Ok(entries) = std::fs::read_dir(&dir) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if path.extension().is_some_and(|e| e == "parquet") {
                                std::fs::remove_file(path).map_err(fatal)?;
                            }
                        }
                    }
                }
            }
        }

        // Publish: rename staged files to their deterministic final names —
        // (load_id, commit_seq, n) with n counted PER TABLE, inside the table's own
        // directory. Cross-table arrival order (concurrent streams) cannot change a
        // name, so a mid-commit crash re-runs this with identical names,
        // overwriting idempotently (research R18).
        let mut per_table: BTreeMap<&TableName, u64> = BTreeMap::new();
        for (table, staged_name) in &self.staged {
            let n = per_table.entry(table).or_insert(0);
            let final_name = format!(
                "part-{}-{}-{n}.parquet",
                meta.load_id.as_str(),
                meta.commit_seq
            );
            *n += 1;
            let from = self.staging.join(staged_name);
            let to = self.out.join(table.as_str()).join(final_name);
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
        // Renames are only durable once their directories are — fsync each touched
        // table dir before the receipt claims the commit happened (clause D2).
        crash_point!(
            "pq.dir.fsync",
            Err(DestError::fatal("injected crash at pq.dir.fsync"))
        );
        for table in per_table.keys() {
            fsync_dir(&self.out.join(table.as_str()))?;
        }
        self.staged.clear();

        // State + receipt land last (write-temp + fsync + rename each).
        crash_point!(
            "pq.state.write",
            Err(DestError::fatal("injected crash at pq.state.write"))
        );
        write_json_atomic(&self.out.join(state_file(&self.scope)), &meta.state)?;
        log.format_version = LAYOUT_FORMAT_VERSION;
        log.receipts.push(key);
        crash_point!(
            "pq.receipt.write",
            Err(DestError::fatal("injected crash at pq.receipt.write"))
        );
        write_json_atomic(&commits_path, &log)?;
        Ok(receipt)
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        let path = self.out.join(state_file(&pipeline_scope(pipeline)));
        let state: Option<StateDoc> = read_json(&path)?;
        Ok(state.filter(|s| &s.pipeline == pipeline))
    }
}
