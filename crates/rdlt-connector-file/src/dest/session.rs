//! One load session: stage batches, then publish them commit-atomically. The
//! commit runs four named phases — replay dedup, Replace truncation, part
//! publish (+ durability fsync), state/receipt write — each a method below.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use rdlt_connector::core::crash_point;
use rdlt_connector::{
    CommitMeta, CommitReceipt, DestinationError, LoadSession, RecordBatch, WriteMode,
    core::{LoadId, PipelineId, StateDoc, TableName, TableSchema},
};

use super::config::DestFormat;
use super::layout::{
    CommitLog, LAYOUT_FORMAT_VERSION, commits_file, final_tail, path_safe, pipeline_scope,
    staged_part_name, staging_tail, state_file,
};
use super::{fatal, truncate};
use crate::location::Location;

/// One staged part: table, partition value (path-rendered), staged name, and
/// its per-(table,partition) index — recorded HERE at write time so the commit
/// never recomputes it (the final name and the staged name share this index).
#[derive(Debug, Clone)]
pub(super) struct StagedPart {
    table: TableName,
    partition: Option<String>,
    name: String,
    part_index: u64,
}

pub(super) struct FileSession {
    pub(super) location: Location,
    pub(super) format: DestFormat,
    /// Resolved at session open, reused for every part. Cheap to clone (the
    /// library's own type is `Arc`-backed internally for column overrides).
    pub(super) writer_properties: parquet::file::properties::WriterProperties,
    pub(super) partition_by: Option<String>,
    pub(super) scope: String,
    pub(super) load_id: LoadId,
    pub(super) tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    pub(super) staged: Vec<StagedPart>,
}

impl FileSession {
    /// Encode one batch per the configured format.
    fn encode(&self, batch: &RecordBatch) -> Result<Vec<u8>, DestinationError> {
        match self.format {
            DestFormat::Parquet => {
                let mut buf = Vec::new();
                let mut writer = ArrowWriter::try_new(
                    &mut buf,
                    Arc::clone(&batch.schema()),
                    Some(self.writer_properties.clone()),
                )
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
    /// NULL → `__null__`). A missing partition column is a typed error naming it.
    fn split_partitions(
        &self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<Vec<(Option<String>, RecordBatch)>, DestinationError> {
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
        // ONE formatter for the column, not one per row. `array_value_to_string`
        // builds an `ArrayFormatter` — which boxes a `dyn DisplayIndex` — on
        // every call, and arrow's own documentation says it "is quite
        // inefficient and is unlikely to be suitable for converting large
        // arrays", pointing at this type instead. The options match what that
        // function uses, so rendered values are unchanged.
        let options = arrow::util::display::FormatOptions::default().with_display_error(true);
        let formatter = arrow::util::display::ArrayFormatter::try_new(values.as_ref(), &options)
            .map_err(fatal)?;
        let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        // Reused across rows: the rendered value is consumed by `path_safe` and
        // only the sanitised result needs to outlive the iteration.
        let mut rendered = String::new();
        for row in 0..batch.num_rows() {
            let key = if values.is_null(row) {
                "__null__".to_owned()
            } else {
                rendered.clear();
                write!(rendered, "{}", formatter.value(row))
                    .map_err(|e| fatal(format!("partition value at row {row}: {e}")))?;
                path_safe(&rendered)
            };
            groups.entry(key).or_default().push(row as u32);
        }
        let mut out = Vec::with_capacity(groups.len());
        for (value, rows) in groups {
            let indices = arrow::array::UInt32Array::from(rows);
            let taken = arrow::compute::take_record_batch(&batch, &indices).map_err(fatal)?;
            out.push((Some(value), taken));
        }
        Ok(out)
    }

    /// Phase 1 (replay): discard this session's staged parts and republish
    /// nothing — the prior receipt already claimed this (load, seq).
    async fn discard_staged(&mut self) {
        for part in self.staged.drain(..).collect::<Vec<_>>() {
            let tail = staging_tail(&self.scope, &self.load_id, &part.name);
            self.location.stage_remove(&tail).await;
        }
    }

    /// Is the filesystem publish protocol in play?
    ///
    /// Names, once, why several places test the location kind: the
    /// stage → rename → fsync-the-directory sequence and the crash points that
    /// punctuate it exist ONLY on a filesystem. An object store has no
    /// directory to fsync and no rename to make atomic, so firing those points
    /// there would inject a crash into a protocol that is not running — and its
    /// own failure surface is swept separately through `S3_FAIL_POINTS`.
    ///
    /// Deliberately NOT used for the frozen truncation rule below: that one
    /// tests `is_local()` because its scope is a statement about OWNERSHIP
    /// (which files this destination may delete), not about durability.
    fn filesystem_protocol(&self) -> bool {
        self.location.is_local()
    }

    /// Phase 2 (truncate): clear each Replace-mode table's owned files, ONCE per
    /// load. The crash point fires before any deletion (local protocol only).
    async fn truncate_replace_tables(&self) -> Result<(), DestinationError> {
        if self.filesystem_protocol() {
            crash_point!(
                "pq.replace.truncate",
                Err(DestinationError::fatal(
                    "injected crash at pq.replace.truncate"
                ))
            );
        }
        // The frozen any-top-level-parquet rule is LOCAL-ONLY (its stated
        // scope): object stores always use the owned-parts rule, so a
        // user-placed *.parquet under the table prefix is never ours to
        // delete.
        //
        // The owned-parts rule takes no configuration: what this destination
        // owns is decided by the name shape it writes, so a load that changed
        // format or dropped partitioning still clears its predecessors.
        let frozen = self.location.is_local()
            && self.format == DestFormat::Parquet
            && self.partition_by.is_none();
        for (table, (_, mode)) in &self.tables {
            if matches!(mode, WriteMode::Replace) {
                truncate::truncate_table(&self.location, table.as_str(), frozen).await?;
            }
        }
        Ok(())
    }

    /// Phase 3 (publish): move each staged part to its deterministic final name,
    /// then fsync every touched directory so the renames survive power loss (D2,
    /// local only). The per-part index was recorded at write time.
    async fn publish_staged(&mut self, meta: &CommitMeta) -> Result<(), DestinationError> {
        let ext = self.format.extension();
        for part in &self.staged {
            let to = final_tail(
                &part.table,
                part.partition.as_deref(),
                &meta.load_id,
                meta.commit_seq,
                part.part_index,
                ext,
            );
            let from = staging_tail(&self.scope, &self.load_id, &part.name);
            self.location.publish_part(&from, &to).await?;
        }
        if self.filesystem_protocol() {
            crash_point!(
                "pq.dir.fsync",
                Err(DestinationError::fatal("injected crash at pq.dir.fsync"))
            );
            let mut synced = BTreeSet::new();
            for part in &self.staged {
                let table = part.table.as_str();
                if let Some(value) = &part.partition {
                    // The rename's dir AND the table dir whose (possibly new)
                    // partition dirent must survive power loss (D2).
                    let leaf = format!("{table}/{value}");
                    if synced.insert(leaf.clone()) {
                        self.location.sync_dir(&leaf)?;
                    }
                }
                if synced.insert(table.to_owned()) {
                    self.location.sync_dir(table)?;
                }
            }
        }
        self.staged.clear();
        Ok(())
    }

    /// Phase 4 (record): state then receipt land LAST — the receipt claiming the
    /// commit happened is the durable idempotency guard. Crash points local-only.
    async fn write_state_and_receipt(
        &self,
        meta: &CommitMeta,
        commits_name: &str,
        log: &mut CommitLog,
        key: (String, u64),
    ) -> Result<(), DestinationError> {
        if self.filesystem_protocol() {
            crash_point!(
                "pq.state.write",
                Err(DestinationError::fatal("injected crash at pq.state.write"))
            );
        }
        self.location
            .write_doc(&state_file(&self.scope), &meta.state)
            .await?;
        log.format_version = LAYOUT_FORMAT_VERSION;
        log.receipts.push(key);
        if self.filesystem_protocol() {
            crash_point!(
                "pq.receipt.write",
                Err(DestinationError::fatal(
                    "injected crash at pq.receipt.write"
                ))
            );
        }
        self.location.write_doc(commits_name, &*log).await?;
        Ok(())
    }
}

#[async_trait]
impl LoadSession for FileSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        if matches!(mode, WriteMode::Merge { .. }) {
            return Err(fatal(
                "file destination does not support Merge (capabilities.merge = false)",
            ));
        }
        if let Some(root) = self.location.local_root() {
            std::fs::create_dir_all(root.join(schema.table.as_str())).map_err(fatal)?;
        }
        self.tables
            .insert(schema.table.clone(), (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        if !self.tables.contains_key(table) {
            return Err(fatal(format!("write before ensure_table for `{table}`")));
        }
        // Staged name is deterministic per (table, partition, per-part write
        // index); the FINAL name is assigned at commit (needs commit_seq).
        // Per-table writes arrive in order, so recovery replay reproduces both
        // the staged and final names identically.
        for (partition, part) in self.split_partitions(table, batch)? {
            let part_index = self
                .staged
                .iter()
                .filter(|s| s.table == *table && s.partition == partition)
                .count() as u64;
            let name = staged_part_name(
                &self.load_id,
                table,
                partition.as_deref(),
                part_index,
                self.format.extension(),
            );
            let bytes = self.encode(&part)?;
            let staged = staging_tail(&self.scope, &self.load_id, &name);
            self.location.stage_put(&staged, bytes).await?;
            self.staged.push(StagedPart {
                table: table.clone(),
                partition,
                name,
                part_index,
            });
        }
        Ok(())
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let commits_name = commits_file(&self.scope);
        let mut log: CommitLog = match self.location.read_doc(&commits_name).await? {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(fatal)?,
            None => CommitLog::default(),
        };
        log.check_readable(&commits_name)?;
        let key = (meta.load_id.as_str().to_owned(), meta.commit_seq);

        // Phase 1 — idempotent per (load_id, commit_seq): discard staged, return
        // the prior receipt.
        if log.receipts.contains(&key) {
            self.discard_staged().await;
            return Ok(receipt);
        }

        // Phase 2 — Replace truncation, guarded DURABLY: "has any earlier commit
        // of THIS load landed?" comes from the receipt log, not session memory,
        // so a crash-recovery session never re-truncates files a prior commit of
        // this load already published. If no receipt landed, re-truncating is
        // convergent (WAL replay re-delivers since the last checkpoint).
        let load_committed_before = log
            .receipts
            .iter()
            .any(|(load, _)| load == meta.load_id.as_str());
        if !load_committed_before {
            self.truncate_replace_tables().await?;
        }

        // Phase 3 — publish staged parts to their final names.
        self.publish_staged(&meta).await?;

        // Phase 4 — state + receipt land last.
        self.write_state_and_receipt(&meta, &commits_name, &mut log, key)
            .await?;
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let name = state_file(&pipeline_scope(pipeline));
        let state: Option<StateDoc> = match self.location.read_doc(&name).await? {
            Some(bytes) => Some(serde_json::from_slice(&bytes).map_err(fatal)?),
            None => None,
        };
        Ok(state.filter(|s| &s.pipeline == pipeline))
    }
}
