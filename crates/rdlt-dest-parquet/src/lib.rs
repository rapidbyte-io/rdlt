//! # rdlt-dest-parquet — minimal parquet-file destination
//!
//! Write-only, Append/Replace (no merge — `merge: false`). One directory per table;
//! staged batches live under `.rdlt-staging/<session>/` and publication is a set of
//! atomic renames plus a rewrite of the JSON state/receipt files (contract:
//! specs/002-file-arrow-ingestion/contracts/file-connectors.md; honesty note on
//! multi-file set-atomicity in research R18 — recovery converges because staged
//! names are deterministic per (load_id, commit_seq, table, n)).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, DestCapabilities, DestError, Destination,
    LoadSession, OpenCtx, RecordBatch, WriteMode,
    core::{LoadId, PipelineId, StateDoc, TableName, TableSchema, naming::IdentRules},
};
use serde::{Deserialize, Serialize};

const STATE_FILE: &str = "_rdlt_state.json";
const COMMITS_FILE: &str = "_rdlt_commits.json";
const STAGING_DIR: &str = ".rdlt-staging";
pub const LAYOUT_FORMAT_VERSION: u32 = 1;

fn fatal(e: impl std::fmt::Display) -> DestError {
    DestError::fatal(e.to_string())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct CommitLog {
    #[serde(default)]
    format_version: u32,
    #[serde(default)]
    receipts: Vec<(String, u64)>,
}

/// Atomic JSON rewrite: write-temp + rename.
fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), DestError> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).map_err(fatal)?;
    std::fs::write(&tmp, bytes).map_err(fatal)?;
    std::fs::rename(&tmp, path).map_err(fatal)?;
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
        // Clause D4: staged data from any dead session becomes invisible/reclaimable.
        let staging_root = self.out.join(STAGING_DIR);
        if staging_root.exists() {
            std::fs::remove_dir_all(&staging_root).map_err(fatal)?;
        }
        let staging = staging_root.join(ctx.load_id.as_str());
        std::fs::create_dir_all(&staging).map_err(fatal)?;
        Ok(Box::new(ParquetSession {
            out: self.out.clone(),
            staging,
            load_id: ctx.load_id,
            tables: BTreeMap::new(),
            staged: Vec::new(),
            replaced: Vec::new(),
        }))
    }
}

struct ParquetSession {
    out: PathBuf,
    staging: PathBuf,
    load_id: LoadId,
    tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    /// Staged batches in arrival order: (table, staged file name).
    staged: Vec<(TableName, String)>,
    /// Tables already truncated by Replace in this load.
    replaced: Vec<TableName>,
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
        // Staged name is deterministic per session write index; the FINAL name is
        // assigned at commit (needs commit_seq). Recovery replay repeats the same
        // write sequence, so both are reproducible (research R18).
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
        let commits_path = self.out.join(COMMITS_FILE);
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

        // Replace: clear each replace-mode table's data files once per load.
        for (table, (_, mode)) in &self.tables {
            if matches!(mode, WriteMode::Replace) && !self.replaced.contains(table) {
                let dir = self.out.join(table.as_str());
                if let Ok(entries) = std::fs::read_dir(&dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().is_some_and(|e| e == "parquet") {
                            std::fs::remove_file(path).map_err(fatal)?;
                        }
                    }
                }
                self.replaced.push(table.clone());
            }
        }

        // Publish: rename staged files to their deterministic final names.
        // (Multi-file rename is not atomic as a SET — see research R18: a mid-commit
        // crash re-runs this with identical names, overwriting idempotently.)
        for (n, (table, staged_name)) in self.staged.iter().enumerate() {
            let final_name = format!(
                "part-{}-{}-{n}.parquet",
                meta.load_id.as_str(),
                meta.commit_seq
            );
            let from = self.staging.join(staged_name);
            let to = self.out.join(table.as_str()).join(final_name);
            let file = std::fs::File::open(&from).map_err(fatal)?;
            file.sync_all().map_err(fatal)?;
            std::fs::rename(&from, &to).map_err(fatal)?;
        }
        self.staged.clear();

        // State + receipt land last (write-temp + rename each).
        write_json_atomic(&self.out.join(STATE_FILE), &meta.state)?;
        log.format_version = LAYOUT_FORMAT_VERSION;
        log.receipts.push(key);
        write_json_atomic(&commits_path, &log)?;
        Ok(receipt)
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        let state: Option<StateDoc> = read_json(&self.out.join(STATE_FILE))?;
        Ok(state.filter(|s| &s.pipeline == pipeline))
    }
}
