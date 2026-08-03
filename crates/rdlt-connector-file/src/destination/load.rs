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
use super::stage::{OpenPart, split_partitions};
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
    /// Staged parts awaiting the next publish, in staging order. The
    /// part INDEX is this list's per-(table, partition) count — per
    /// COMMIT, not per session, deliberately: a crash-recovery session
    /// resumes from committed state and stages FEWER parts, so a
    /// session-lifetime count would re-publish the pending commit
    /// under different indices and orphan the crashed attempt's
    /// already-copied finals (measured live in the S3 sweep: 6 rows
    /// where 4 were loaded).
    staged: Vec<StagedPart>,
    /// When a part is closed and the next begun.
    parts: rdlt_connector_sdk::spi::PartOptions,
    /// Parts still being written, keyed by table and partition — a
    /// part holds one table's rows for one partition, so those two
    /// are what identify it.
    open: BTreeMap<(String, Option<String>), (OpenPart, std::time::Instant)>,
}

/// Read the receipt log.
///
/// A FREE function rather than a method, and the reason is load-bearing:
/// `Load` holds an open `ArrowWriter`, which is `Send` but never `Sync`,
/// so a `&self` borrow held across an await would not compile. Taking
/// the two fields it reads keeps the borrow narrow enough to be `Send`.
async fn commit_log(location: &Location, scope: &str) -> Result<CommitLog, DestinationError> {
    let file = commits_file(scope);
    let bytes = location.read_doc(&file).await?;
    CommitLog::decode(bytes.as_deref(), &file)
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
        parts: rdlt_connector_sdk::spi::PartOptions,
    ) -> Result<Self, DestinationError> {
        location.prepare_staging(&scope, load_id.as_str()).await?;
        Ok(Self {
            parts,
            open: BTreeMap::new(),
            location,
            format,
            partition_by,
            props,
            scope,
            load_id,
            tables: BTreeMap::new(),
            staged: Vec::new(),
        })
    }

    /// Close one open part and stage it.
    ///
    /// Staging is what makes the part real: until then it exists only
    /// as encoded bytes in memory, and a crash simply loses it — which
    /// is correct, because nothing has claimed it landed.
    async fn close_part(&mut self, key: &(String, Option<String>)) -> Result<(), DestinationError> {
        let Some((open, _)) = self.open.remove(key) else {
            return Ok(());
        };
        let (table, partition) = key;
        let bytes = open.finish()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let index = self
            .staged
            .iter()
            .filter(|s| &s.table == table && &s.partition == partition)
            .count();
        let name = staged_name(table, partition.as_deref(), index, self.format.extension());
        let tail = staging_tail(&self.scope, self.load_id.as_str(), &name);
        self.location.stage_put(&tail, bytes).await?;
        self.staged.push(StagedPart {
            table: table.clone(),
            partition: partition.clone(),
            index,
            staging_tail: tail,
        });
        Ok(())
    }

    /// Keep the open parts inside their memory ceiling.
    ///
    /// The LARGEST is closed first: it is nearest its target, so the
    /// part this produces is the least undersized one available. The
    /// loop terminates because each pass removes one part, and an
    /// empty set holds nothing.
    async fn enforce_open_budget(&mut self) -> Result<(), DestinationError> {
        loop {
            let total: u64 = self.open.values().map(|(open, _)| open.encoded_len()).sum();
            if !self.parts.over_budget(total) {
                return Ok(());
            }
            let Some(largest) = self
                .open
                .iter()
                .max_by_key(|(_, (open, _))| open.encoded_len())
                .map(|(key, _)| key.clone())
            else {
                return Ok(());
            };
            self.close_part(&largest).await?;
        }
    }

    /// Close EVERY open part.
    ///
    /// Called before publish: a part never spans a commit, because the
    /// publish protocol moves whole staged files and a half-written
    /// one has nothing to move.
    async fn close_all_parts(&mut self) -> Result<(), DestinationError> {
        let keys: Vec<_> = self.open.keys().cloned().collect();
        for key in keys {
            self.close_part(&key).await?;
        }
        Ok(())
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
            let key = (table.to_string(), partition.clone());
            // A parquet file carries ONE schema, so a schema change
            // closes the part rather than being appended into it.
            let schema_changed = self
                .open
                .get(&key)
                .is_some_and(|(open, _)| open.schema_differs(&group));
            if schema_changed {
                self.close_part(&key).await?;
            }
            let (open, opened_at) = match self.open.remove(&key) {
                Some(existing) => existing,
                None => (
                    OpenPart::begin(self.format, &group, &self.props)?,
                    std::time::Instant::now(),
                ),
            };
            let mut open = open;
            open.append(&group)?;
            let encoded = open.encoded_len();
            self.open.insert(key.clone(), (open, opened_at));
            if self
                .parts
                .should_roll(encoded, opened_at.elapsed().as_secs())
            {
                self.close_part(&key).await?;
            }
        }
        self.enforce_open_budget().await
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        let log = commit_log(&self.location, &self.scope).await?;
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
        // A redelivered commit's parts must not linger for a later
        // genuine commit to find. The OPEN ones are dropped outright —
        // they exist only in memory and were never claimed to land —
        // and the staged ones are removed best-effort.
        self.open.clear();
        for part in self.staged.drain(..) {
            self.location.stage_remove(&part.staging_tail).await;
        }
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // Phase 1 — no part spans a commit. Publish moves WHOLE staged
        // files, and a part still open has no file to move, so every
        // one is closed and staged before anything else happens. This
        // makes the commit cadence an upper bound on part size.
        self.close_all_parts().await?;

        let mut log = commit_log(&self.location, &self.scope).await?;

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
