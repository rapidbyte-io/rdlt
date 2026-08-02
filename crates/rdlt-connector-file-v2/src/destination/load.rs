//! One load session's system IO — the [`Backend`] behind the sdk's
//! choreography, mapping the 4-phase commit onto files.
//!
//! The phases, in publish order:
//! 1. REPLAY DEDUP is the framework's: `existing_receipt` reads the
//!    persisted commit log and `replay` discards this session's staged
//!    parts, so a redelivered commit returns the prior receipt without
//!    republishing.
//! 2. REPLACE TRUNCATION, guarded DURABLY: "has any earlier commit of
//!    THIS load landed?" is read from the receipt log, never session
//!    memory — a crash-recovery session must not re-truncate files a
//!    prior commit of this load already published, and if no receipt
//!    landed, re-truncating is convergent under WAL re-delivery.
//! 3. PUBLISH each staged part to its deterministic final name, then
//!    (local only) fsync every touched directory.
//! 4. RECORD state, then the receipt LAST — the durable idempotency
//!    guard.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use parquet::file::properties::WriterProperties;
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::{DestinationError, RecordBatch};

use super::config::DestFormat;
use super::layout::{
    CommitLog, LAYOUT_FORMAT_VERSION, commits_file, final_tail, staged_name, staging_tail,
    state_file,
};
use super::stage::{encode_part, split_partitions};
use super::truncate::truncate_table;
use crate::location::Location;

/// The session state: where to write, how to encode, and what has been
/// staged so far.
#[derive(Debug)]
pub struct Load {
    location: Location,
    format: DestFormat,
    partition_by: Option<String>,
    props: WriterProperties,
    scope: String,
    load_id: LoadId,
    /// Write dispositions recorded by `ensure_table` — Replace is what
    /// truncation iterates.
    tables: BTreeMap<TableName, WriteMode>,
    /// Staged parts awaiting the next publish, in staging order.
    staged: Vec<StagedPart>,
    /// Session-lifetime part counts per (table, partition) — the index
    /// in both staged and final names.
    counts: BTreeMap<(String, Option<String>), usize>,
}

#[derive(Debug)]
struct StagedPart {
    table: String,
    partition: Option<String>,
    index: usize,
    staging_tail: String,
}

impl Load {
    /// Open one session: reclaim the scope's staging (a crashed
    /// predecessor's debris), then ready this load's area.
    pub(super) async fn open(
        location: Location,
        format: DestFormat,
        partition_by: Option<String>,
        props: WriterProperties,
        scope: String,
        load_id: LoadId,
    ) -> Result<Self, DestinationError> {
        location.prepare_staging(&scope, load_id.as_str()).await?;
        Ok(Self {
            location,
            format,
            partition_by,
            props,
            scope,
            load_id,
            tables: BTreeMap::new(),
            staged: Vec::new(),
            counts: BTreeMap::new(),
        })
    }

    async fn commit_log(&self) -> Result<CommitLog, DestinationError> {
        let file = commits_file(&self.scope);
        let bytes = self.location.read_doc(&file).await?;
        CommitLog::decode(bytes.as_deref(), &file)
    }

    /// The frozen pre-015 truncation addendum applies only to the
    /// local + parquet + unpartitioned shape it was recorded for.
    fn frozen_plain_parquet(&self) -> bool {
        self.location.is_local()
            && self.format == DestFormat::Parquet
            && self.partition_by.is_none()
    }
}

#[async_trait]
impl Backend for Load {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        if matches!(mode, WriteMode::Merge { .. }) {
            return Err(DestinationError::fatal(
                "file destination does not support Merge (capabilities.merge = false)",
            ));
        }
        if let Some(root) = self.location.local_root() {
            std::fs::create_dir_all(root.join(schema.table.as_str()))
                .map_err(|e| DestinationError::fatal(format!("ensure_table: {e}")))?;
        }
        self.tables.insert(schema.table.clone(), mode.clone());
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        for (partition, group) in split_partitions(table, &batch, self.partition_by.as_deref())? {
            let bytes = encode_part(self.format, &group, &self.props)?;
            let count = self
                .counts
                .entry((table.to_string(), partition.clone()))
                .or_insert(0);
            let index = *count;
            *count += 1;
            let name = staged_name(
                self.load_id.as_str(),
                table.as_str(),
                partition.as_deref(),
                index,
                self.format.extension(),
            );
            let tail = staging_tail(&self.scope, self.load_id.as_str(), &name);
            self.location.stage_put(&tail, bytes).await?;
            self.staged.push(StagedPart {
                table: table.to_string(),
                partition,
                index,
                staging_tail: tail,
            });
        }
        Ok(())
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        let log = self.commit_log().await?;
        let key = (load_id.as_str().to_owned(), commit_seq);
        Ok(log
            .receipts
            .iter()
            .any(|r| r == &key)
            .then(|| CommitReceipt {
                load_id: load_id.clone(),
                commit_seq,
            }))
    }

    async fn replay(
        &mut self,
        _meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // A redelivered commit's staged parts must not linger for a
        // later genuine commit to find — discard them, best-effort.
        for part in self.staged.drain(..) {
            self.location.stage_remove(&part.staging_tail).await;
        }
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let mut log = self.commit_log().await?;

        // Phase 2 — Replace truncation, once per LOAD, guarded by the
        // durable receipt log: any earlier commit of this load means
        // its truncation already ran and its parts are live.
        let first_commit_of_load = !log
            .receipts
            .iter()
            .any(|(load, _)| load == meta.load_id.as_str());
        if first_commit_of_load {
            if self.location.is_local() {
                crash_point!(
                    "pq.replace.truncate",
                    Err(DestinationError::fatal(
                        "injected crash at pq.replace.truncate"
                    ))
                );
            }
            let frozen = self.frozen_plain_parquet();
            let replaced: Vec<String> = self
                .tables
                .iter()
                .filter(|(_, mode)| matches!(mode, WriteMode::Replace))
                .map(|(table, _)| table.to_string())
                .collect();
            for table in replaced {
                truncate_table(&self.location, &table, frozen).await?;
            }
        }

        // Phase 3 — publish every staged part to its deterministic
        // final name, then make the renames durable (local only).
        let mut touched: BTreeSet<String> = BTreeSet::new();
        for part in std::mem::take(&mut self.staged) {
            let tail = final_tail(
                &part.table,
                part.partition.as_deref(),
                meta.load_id.as_str(),
                meta.commit_seq,
                part.index,
                self.format.extension(),
            );
            self.location
                .publish_part(&part.staging_tail, &tail)
                .await?;
            touched.insert(part.table.clone());
            if let Some(partition) = &part.partition {
                touched.insert(format!("{}/{partition}", part.table));
            }
        }
        if self.location.is_local() {
            crash_point!(
                "pq.dir.fsync",
                Err(DestinationError::fatal("injected crash at pq.dir.fsync"))
            );
            for dir in &touched {
                self.location.sync_dir(dir)?;
            }
        }

        // Phase 4 — state, then the receipt LAST.
        if self.location.is_local() {
            crash_point!(
                "pq.state.write",
                Err(DestinationError::fatal("injected crash at pq.state.write"))
            );
        }
        self.location
            .write_doc(&state_file(&self.scope), &meta.state)
            .await?;
        if self.location.is_local() {
            crash_point!(
                "pq.receipt.write",
                Err(DestinationError::fatal(
                    "injected crash at pq.receipt.write"
                ))
            );
        }
        log.format_version = LAYOUT_FORMAT_VERSION;
        log.receipts
            .push((meta.load_id.as_str().to_owned(), meta.commit_seq));
        self.location
            .write_doc(&commits_file(&self.scope), &log)
            .await?;
        Ok(CommitReceipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
        })
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let file = state_file(&super::layout::scope_of(pipeline.as_str()));
        let Some(bytes) = self.location.read_doc(&file).await? else {
            return Ok(None);
        };
        let state: StateDoc = serde_json::from_slice(&bytes)
            .map_err(|e| DestinationError::fatal(format!("unreadable state `{file}`: {e}")))?;
        // The scope is a 12-hex hash: on the astronomically unlikely
        // collision, the embedded pipeline id is the truth.
        Ok(Some(state).filter(|s| &s.pipeline == pipeline))
    }
}
