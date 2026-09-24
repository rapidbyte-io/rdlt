use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{
    DESTINATION_CLAUSES, Outcome, Probe, Report, SOURCE_CLAUSES, certify_destination,
    certify_source,
};
use crate::capabilities::{Capabilities, SchemaChanges};
use crate::catalog::{Catalog, Checkpointing, StreamSpec};
use crate::commit::{CommitMeta, Receipt};
use crate::destination::{
    DestinationConnector, MergeKey, OpenContext, Opened, Session, TableChange, TableRef,
    TableWriter, WriteStats,
};
use crate::emitter::Emitter;
use crate::error::{ConnectorError, Result};
use crate::id::{
    CommitSeq, Epoch, GenerationId, LoadId, PipelineId, SegmentId, StreamName, TablePath,
};
use crate::sink::Push;
use crate::source::{Partition, ReadStream, SourceConnector, Streams};
use crate::spec::{BoxFuture, ConnectContext};
use crate::state::{StateChange, StateRecord, StreamState};
use crate::types::LogicalType;

fn failed(report: &Report) -> Vec<&'static str> {
    report.failures().map(|result| result.clause.id).collect()
}

/// A source that is correct unless a flag breaks one behavior.
#[derive(Deserialize, JsonSchema)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag breaks one behavior"
)]
struct PagesConfig {
    pages: u32,
    ignore_cursor: bool,
    ignore_barriers: bool,
    natural: bool,
    unstable_discover: bool,
    empty_catalog: bool,
    repeat_partitions: bool,
    fail_on_stop: bool,
    refuse_connect: bool,
    /// The call that never returns: `connect`, `check`, `discover` or `plan`.
    hang: String,
}

impl Default for PagesConfig {
    fn default() -> Self {
        Self {
            pages: 4,
            ignore_cursor: false,
            ignore_barriers: false,
            natural: false,
            unstable_discover: false,
            empty_catalog: false,
            repeat_partitions: false,
            fail_on_stop: false,
            refuse_connect: false,
            hang: String::new(),
        }
    }
}

struct Pages {
    config: PagesConfig,
    discovered: AtomicBool,
}

impl PagesConfig {
    /// Never returns when this configuration hangs in `call`.
    async fn hang_in(&self, call: &str) {
        if self.hang == call {
            std::future::pending::<()>().await;
        }
    }
}

impl SourceConnector for Pages {
    const ID: &'static str = "io.test.pages";
    const VERSION: &'static str = "0.0.1";
    type Config = PagesConfig;

    async fn connect(config: PagesConfig, _context: &ConnectContext) -> Result<Self> {
        config.hang_in("connect").await;
        if config.refuse_connect {
            return Err(ConnectorError::config("refused"));
        }
        Ok(Self {
            config,
            discovered: AtomicBool::new(false),
        })
    }

    async fn check(&self) -> Result<()> {
        self.config.hang_in("check").await;
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        if self.config.empty_catalog {
            Streams::new()
        } else {
            Streams::new().with(Page {
                natural: self.config.natural,
            })
        }
    }

    async fn discover(&self) -> Result<Catalog> {
        self.config.hang_in("discover").await;
        let mut streams: Vec<StreamSpec> =
            self.streams().catalog().map(Vec::from).unwrap_or_default();
        if self.config.unstable_discover && self.discovered.swap(true, Ordering::SeqCst) {
            streams.push(StreamSpec::new(StreamName::new("late").unwrap()));
        }
        Ok(Catalog::new(streams).unwrap())
    }
}

struct Page {
    natural: bool,
}

impl ReadStream<Pages> for Page {
    type Cursor = u32;

    fn spec(&self) -> StreamSpec {
        let checkpointing = if self.natural {
            Checkpointing::Natural
        } else {
            Checkpointing::OnDemand
        };
        StreamSpec::new(StreamName::new("pages").unwrap()).with_checkpointing(checkpointing)
    }

    async fn partitions(&self, source: &Pages, _state: &StreamState) -> Result<Vec<Partition>> {
        source.config.hang_in("plan").await;
        let copies = if source.config.repeat_partitions {
            2
        } else {
            1
        };
        Ok(vec![Partition::single(); copies])
    }

    async fn read(
        &self,
        source: &Pages,
        _partition: &Partition,
        cursor: u32,
        out: &mut Emitter<u32>,
    ) -> Result<()> {
        let start = if source.config.ignore_cursor {
            0
        } else {
            cursor
        };
        for page in start..source.config.pages {
            let pushed = out.rows(&[json!({ "page": page })]).await;
            if source.config.fail_on_stop && pushed.is_err() {
                return Err(ConnectorError::data("gave up"));
            }
            pushed?;
            if !source.config.ignore_barriers || !out.checkpoint_due() {
                out.checkpoint(&(page + 1)).await?;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn a_correct_source_passes_every_clause() {
    certify_source::<Pages>(json!({})).await.assert_passed();
    let empty = certify_source::<Pages>(json!({ "pages": 0 })).await;
    empty.assert_passed();
    assert_eq!(
        empty.outcome("S-BARRIER"),
        Some(&Outcome::Passed),
        "a source with no data owes no answer"
    );
}

#[tokio::test]
async fn natural_checkpointing_skips_the_barrier_clause() {
    let report = certify_source::<Pages>(json!({ "natural": true })).await;
    report.assert_passed();
    assert!(
        matches!(report.outcome("S-BARRIER"), Some(Outcome::Skipped(_))),
        "{report}"
    );
    assert_eq!(report.outcome("S-CHECK"), Some(&Outcome::Passed));
    assert_eq!(report.outcome("S-NONE"), None);
}

#[tokio::test]
async fn each_broken_source_behavior_fails_exactly_its_clause() {
    let cases = [
        ("ignore_cursor", "S-RESUME"),
        ("ignore_barriers", "S-BARRIER"),
        ("unstable_discover", "S-DISCOVER"),
        ("empty_catalog", "S-DISCOVER"),
        ("repeat_partitions", "S-PLAN"),
        ("fail_on_stop", "S-STOP"),
    ];
    for (flag, clause) in cases {
        let report = certify_source::<Pages>(json!({ flag: true })).await;
        assert_eq!(failed(&report), [clause], "{flag}: {report}");
    }
}

/// Certification must end even when a connector never answers; a day of paused time passes
/// instantly, so a call without a timeout fails this bound instead of hanging the test.
async fn within_a_day<T>(certification: impl Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_hours(24), certification)
        .await
        .expect("certification ended")
}

#[tokio::test(start_paused = true)]
async fn a_source_call_that_never_returns_fails_instead_of_hanging() {
    let cases = [
        ("connect", SOURCE_CLAUSES.len()),
        ("check", 1),
        ("discover", SOURCE_CLAUSES.len() - 1),
        ("plan", 4),
    ];
    for (call, failures) in cases {
        let report = within_a_day(certify_source::<Pages>(json!({ "hang": call }))).await;
        assert_eq!(failed(&report).len(), failures, "{call}: {report}");
        assert!(
            report.to_string().contains("took longer"),
            "{call}: {report}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_destination_connect_that_never_returns_fails_instead_of_hanging() {
    let report = within_a_day(certify_vault("hang_connect", Some("hang_connect"))).await;
    assert_eq!(failed(&report).len(), DESTINATION_CLAUSES.len(), "{report}");
    assert!(report.to_string().contains("took longer"), "{report}");
}

#[tokio::test]
async fn a_source_that_cannot_connect_fails_every_clause() {
    let report = certify_source::<Pages>(json!({ "refuse_connect": true })).await;
    assert!(report.results.iter().all(
        |result| matches!(&result.outcome, Outcome::Failed(reason) if reason.contains("connect"))
    ));
    let rendered = report.to_string();
    assert!(rendered.contains("FAIL S-CHECK"), "{rendered}");
    assert!(std::panic::catch_unwind(|| report.assert_passed()).is_err());
}

#[test]
fn json_pushes_compare_by_rows_not_formatting() {
    let array = super::source::normalize(Push::Json(Bytes::from_static(
        b"[ {\"a\": 1}, {\"a\": 2} ]",
    )));
    let lines = super::source::normalize(Push::Json(Bytes::from_static(b"{\"a\":1}\n{\"a\":2}\n")));
    assert_eq!(array, lines);
    assert_eq!(
        array,
        Push::Json(Bytes::from_static(b"[{\"a\":1},{\"a\":2}]"))
    );
}

/// A destination that is correct unless a flag breaks one behavior.
#[derive(Default, Deserialize, JsonSchema)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag breaks one behavior"
)]
struct VaultConfig {
    store: String,
    static_epoch: bool,
    miscount: bool,
    republish: bool,
    forget_state: bool,
    ignore_deletes: bool,
    publish_on_write: bool,
    keep_staging: bool,
    no_fence: bool,
    wrong_fence_kind: bool,
    refuse_connect: bool,
    forget_receipts: bool,
    publish_all: bool,
    local_epoch: bool,
    local_state: bool,
    stale_writes: bool,
    refuse_unstaged: bool,
    hang_connect: bool,
    replace_early: bool,
    merge_appends: bool,
    refuse_repeated_changes: bool,
    ignore_added_columns: bool,
    refuse_widening: bool,
    /// Declares only the minimal capabilities.
    minimal: bool,
    /// Declares no schema change at all.
    fixed_schema: bool,
    /// Applies a change that conflicts with a column's type instead of refusing it.
    accept_conflicts: bool,
    /// Refuses a batch whose column is narrower than the table's.
    refuse_narrower: bool,
    /// Reports a conflict as a plain data error, without the `schema_conflict` code.
    uncoded_conflicts: bool,
}

#[derive(Default)]
struct VaultStore {
    epochs: BTreeMap<PipelineId, u64>,
    state: BTreeMap<PipelineId, BTreeMap<String, StateRecord>>,
    receipts: BTreeMap<(LoadId, CommitSeq), Receipt>,
    staged: BTreeMap<(PipelineId, SegmentId), Vec<Staged>>,
    published: BTreeMap<String, Vec<RecordBatch>>,
    /// Committed rows of replace generations, by table and generation.
    generations: BTreeMap<(String, GenerationId), Vec<RecordBatch>>,
    /// Each table's name, by path, as writers named it.
    tables: BTreeMap<TablePath, String>,
    /// Every schema change applied, rendered.
    changes: BTreeSet<String>,
    /// Columns added while `ignore_added_columns` was set, by table; writes drop them.
    ignored: BTreeSet<(String, String)>,
    /// Each table's columns and their types, as schema changes left them.
    columns: BTreeMap<String, BTreeMap<String, LogicalType>>,
}

/// A batch staged for a table, a replace generation of it, or a merge into it.
#[derive(Clone)]
struct Staged {
    table: String,
    generation: Option<GenerationId>,
    merge: Option<MergeKey>,
    batch: RecordBatch,
}

type SharedVault = Arc<Mutex<VaultStore>>;

static VAULTS: LazyLock<Mutex<BTreeMap<String, SharedVault>>> = LazyLock::new(Mutex::default);

fn vault(name: &str) -> SharedVault {
    Arc::clone(VAULTS.lock().unwrap().entry(name.to_owned()).or_default())
}

/// The shared store, and this connection's private one for the `local_*` flags.
#[derive(Clone)]
struct VaultStores {
    shared: SharedVault,
    local: SharedVault,
}

impl VaultConfig {
    /// The pipeline's current epoch, from the store this configuration keeps epochs in.
    fn epoch(&self, shared: &VaultStore, stores: &VaultStores, pipeline: &PipelineId) -> u64 {
        let epochs = |store: &VaultStore| store.epochs.get(pipeline).copied().unwrap_or_default();
        if self.local_epoch {
            epochs(&stores.local.lock().unwrap())
        } else {
            epochs(shared)
        }
    }

    fn state_store<'a>(&self, stores: &'a VaultStores) -> &'a SharedVault {
        if self.local_state {
            &stores.local
        } else {
            &stores.shared
        }
    }
}

struct Vault {
    config: Arc<VaultConfig>,
    stores: VaultStores,
}

struct VaultSession {
    config: Arc<VaultConfig>,
    stores: VaultStores,
    pipeline: PipelineId,
    epoch: u64,
}

struct VaultWriter {
    config: Arc<VaultConfig>,
    stores: VaultStores,
    pipeline: PipelineId,
    epoch: u64,
    table: String,
    generation: Option<GenerationId>,
    merge: Option<MergeKey>,
}

impl DestinationConnector for Vault {
    const ID: &'static str = "io.test.vault";
    const VERSION: &'static str = "0.0.1";
    type Config = VaultConfig;
    type Session = VaultSession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        if !self.config.minimal {
            capabilities.write_modes.replace = true;
            capabilities.write_modes.merge = true;
            capabilities.schema_changes = SchemaChanges::all();
        }
        if self.config.fixed_schema {
            capabilities.schema_changes = SchemaChanges::default();
        }
        capabilities
    }

    async fn connect(config: VaultConfig, _context: &ConnectContext) -> Result<Self> {
        if config.hang_connect {
            std::future::pending::<()>().await;
        }
        if config.refuse_connect {
            return Err(ConnectorError::config("refused"));
        }
        let stores = VaultStores {
            shared: vault(&config.store),
            local: SharedVault::default(),
        };
        Ok(Self {
            config: Arc::new(config),
            stores,
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<VaultSession>> {
        let epochs = if self.config.local_epoch {
            &self.stores.local
        } else {
            &self.stores.shared
        };
        let epoch = {
            let mut store = epochs.lock().unwrap();
            let epoch = store.epochs.entry(context.pipeline.clone()).or_default();
            *epoch += 1;
            *epoch
        };
        let state = self
            .config
            .state_store(&self.stores)
            .lock()
            .unwrap()
            .state
            .get(&context.pipeline)
            .map(|state| state.values().cloned().collect())
            .unwrap_or_default();
        let reported = if self.config.static_epoch {
            Epoch(1)
        } else {
            Epoch(epoch)
        };
        let session = VaultSession {
            config: Arc::clone(&self.config),
            stores: self.stores.clone(),
            pipeline: context.pipeline.clone(),
            epoch,
        };
        Ok(Opened {
            session,
            epoch: reported,
            state,
        })
    }
}

impl VaultSession {
    /// Applies the state changes of `meta`, to this connection's store with `local_state`.
    fn apply_state(&self, shared: &mut VaultStore, meta: &CommitMeta) {
        let mut local = self.stores.local.lock().unwrap();
        let states = if self.config.local_state {
            &mut local.state
        } else {
            &mut shared.state
        };
        let state = states.entry(self.pipeline.clone()).or_default();
        for change in &meta.state_delta {
            match change {
                StateChange::Put(record) => state.insert(record.key.clone(), record.clone()),
                StateChange::Delete(_) if self.config.ignore_deletes => None,
                StateChange::Delete(key) => state.remove(key),
            };
        }
    }

    /// Publishes the segments of `meta` into tables, generations and merges, and swaps in the
    /// generations it finishes; returns the rows published.
    fn publish(&self, store: &mut VaultStore, meta: &CommitMeta) -> u64 {
        let mut rows = 0;
        let mut merging: BTreeMap<String, Vec<(MergeKey, RecordBatch)>> = BTreeMap::new();
        for segment in self.segments_to_publish(store, meta) {
            let key = (self.pipeline.clone(), segment);
            let batches = if self.config.republish {
                store.staged.get(&key).cloned().unwrap_or_default()
            } else {
                store.staged.remove(&key).unwrap_or_default()
            };
            for staged in batches {
                rows += staged.batch.num_rows() as u64;
                match (staged.generation, staged.merge) {
                    (Some(generation), _) if !self.config.replace_early => store
                        .generations
                        .entry((staged.table, generation))
                        .or_default()
                        .push(staged.batch),
                    (_, Some(key)) if !self.config.merge_appends => merging
                        .entry(staged.table)
                        .or_default()
                        .push((key, staged.batch)),
                    _ => store
                        .published
                        .entry(staged.table)
                        .or_default()
                        .push(staged.batch),
                }
            }
        }
        for (table, incoming) in merging {
            merge(store.published.entry(table).or_default(), incoming);
        }
        for (path, generation) in &meta.finish_generations {
            let Some(table) = store.tables.get(path).cloned() else {
                continue;
            };
            let rows = store
                .generations
                .remove(&(table.clone(), *generation))
                .unwrap_or_default();
            store.generations.retain(|(name, _), _| *name != table);
            store.published.insert(table, rows);
        }
        rows
    }

    /// With `refuse_unstaged`, the first segment of `meta` this pipeline never staged.
    fn refused_segment(&self, store: &VaultStore, meta: &CommitMeta) -> Option<SegmentId> {
        let staged = |segment: &SegmentId| {
            store
                .staged
                .contains_key(&(self.pipeline.clone(), *segment))
        };
        self.config
            .refuse_unstaged
            .then(|| meta.segments.iter().find(|segment| !staged(segment)))
            .flatten()
    }

    /// The segments `meta` commits, or with `publish_all` every segment the pipeline staged.
    fn segments_to_publish(&self, store: &VaultStore, meta: &CommitMeta) -> Vec<SegmentId> {
        if self.config.publish_all {
            store
                .staged
                .keys()
                .filter(|(staged, _)| *staged == self.pipeline)
                .map(|(_, segment)| *segment)
                .collect()
        } else {
            meta.segments.iter().collect()
        }
    }
}

impl Session for VaultSession {
    type Writer = VaultWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        let mut store = self.stores.shared.lock().unwrap();
        let first = store.changes.insert(format!("{change:?}"));
        let alters = !matches!(change, TableChange::Create { .. });
        if self.config.refuse_repeated_changes && alters && !first {
            return Err(ConnectorError::data("the change was already applied"));
        }
        if !self.config.accept_conflicts
            && let Some(conflict) = conflict(&store, change)
        {
            let error = ConnectorError::data(conflict);
            if self.config.uncoded_conflicts {
                return Err(error);
            }
            return Err(error.with_code("schema_conflict"));
        }
        record(&mut store, change);
        match change {
            TableChange::AddColumn { table, field } if self.config.ignore_added_columns => {
                store
                    .ignored
                    .insert((table.name.to_string(), field.name().to_owned()));
            }
            TableChange::Widen { .. } if self.config.refuse_widening => {
                return Err(ConnectorError::data("columns never widen here"));
            }
            _ => {}
        }
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<VaultWriter> {
        self.stores
            .shared
            .lock()
            .unwrap()
            .tables
            .insert(table.path.clone(), table.name.to_string());
        Ok(VaultWriter {
            config: Arc::clone(&self.config),
            stores: self.stores.clone(),
            pipeline: self.pipeline.clone(),
            epoch: self.epoch,
            table: table.name.to_string(),
            generation: table.generation,
            merge: table.merge.clone(),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        if !self.config.keep_staging {
            self.stores
                .shared
                .lock()
                .unwrap()
                .staged
                .retain(|(pipeline, _), _| *pipeline != self.pipeline);
        }
        Ok(())
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let mut store = self.stores.shared.lock().unwrap();
        let current = self.config.epoch(&store, &self.stores, &self.pipeline);
        if !self.config.no_fence && current != self.epoch {
            let error = if self.config.wrong_fence_kind {
                ConnectorError::data("stale")
            } else {
                ConnectorError::fenced("stale")
            };
            return Err(error);
        }
        let key = (meta.load_id, meta.commit_seq);
        let recall = !self.config.republish && !self.config.forget_receipts;
        if let Some(receipt) = store.receipts.get(&key).filter(|_| recall) {
            return Ok(receipt.clone());
        }
        if let Some(segment) = self.refused_segment(&store, meta) {
            return Err(ConnectorError::data(format!(
                "segment {segment} is not staged"
            )));
        }
        let rows = self.publish(&mut store, meta);
        if !self.config.forget_state {
            self.apply_state(&mut store, meta);
        }
        let rows = if self.config.miscount { rows + 1 } else { rows };
        let receipt = Receipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
            committed_at: UNIX_EPOCH,
            rows,
            bytes: 0,
        };
        store.receipts.insert(key, receipt.clone());
        Ok(receipt)
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

impl TableWriter for VaultWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        let mut store = self.stores.shared.lock().unwrap();
        let current = self.config.epoch(&store, &self.stores, &self.pipeline);
        if !self.config.stale_writes && current != self.epoch {
            return Err(ConnectorError::fenced("stale"));
        }
        if self.config.refuse_narrower
            && let Some(columns) = store.columns.get(&self.table)
            && batch.schema().fields().iter().any(|field| {
                columns
                    .get(field.name())
                    .is_some_and(|column| column.to_arrow() != *field.data_type())
            })
        {
            return Err(ConnectorError::data("the batch does not match the table"));
        }
        let dropped: Vec<usize> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                store
                    .ignored
                    .contains(&(self.table.clone(), field.name().clone()))
            })
            .map(|(index, _)| index)
            .collect();
        let kept: Vec<usize> = (0..batch.num_columns())
            .filter(|index| !dropped.contains(index))
            .collect();
        let batch = batch.project(&kept).expect("kept columns exist");
        if self.config.publish_on_write {
            store
                .published
                .entry(self.table.clone())
                .or_default()
                .push(batch.clone());
        }
        store
            .staged
            .entry((self.pipeline.clone(), segment))
            .or_default()
            .push(Staged {
                table: self.table.clone(),
                generation: self.generation,
                merge: self.merge.clone(),
                batch,
            });
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        Ok(WriteStats::default())
    }
}

/// Merges `incoming` into `published` a row at a time: an incoming row replaces the published
/// row with its key, and among incoming rows of one key the greatest sequence wins.
fn merge(published: &mut Vec<RecordBatch>, incoming: Vec<(MergeKey, RecordBatch)>) {
    let Some(key) = incoming.first().map(|(key, _)| key.clone()) else {
        return;
    };
    let key_of = |row: &RecordBatch| -> Vec<String> {
        key.columns
            .iter()
            .map(|column| {
                let values = row
                    .column_by_name(column)
                    .expect("merge rows carry their key");
                arrow_cast::display::array_value_to_string(values, 0).unwrap()
            })
            .collect()
    };
    let seq_of = |row: &RecordBatch| -> Vec<u8> {
        let seq = row
            .column_by_name(&key.seq)
            .expect("merge rows carry a sequence");
        seq.as_binary::<i32>().value(0).to_vec()
    };
    let rows = |batches: &[RecordBatch]| -> Vec<RecordBatch> {
        batches
            .iter()
            .flat_map(|batch| (0..batch.num_rows()).map(|row| batch.slice(row, 1)))
            .collect()
    };
    let mut winners: BTreeMap<Vec<String>, RecordBatch> = BTreeMap::new();
    let batches: Vec<RecordBatch> = incoming.into_iter().map(|(_, batch)| batch).collect();
    for row in rows(&batches) {
        match winners.get(&key_of(&row)) {
            Some(best) if seq_of(best) >= seq_of(&row) => {}
            _ => {
                winners.insert(key_of(&row), row);
            }
        }
    }
    let mut kept: Vec<RecordBatch> = rows(published)
        .into_iter()
        .filter(|row| !winners.contains_key(&key_of(row)))
        .collect();
    kept.extend(winners.into_values());
    *published = kept;
}

/// Why `change` conflicts with the table's columns, if it does: a column it names already has
/// another type.
fn conflict(store: &VaultStore, change: &TableChange) -> Option<String> {
    let columns = store.columns.get(change.table().name.as_ref())?;
    let clash = |name: &str, declared: &LogicalType| {
        columns
            .get(name)
            .filter(|existing| existing.join(declared) != **existing)
            .map(|existing| format!("column {name} is {existing}"))
    };
    match change {
        TableChange::Create { schema, .. } => schema
            .fields()
            .iter()
            .find_map(|field| clash(field.name(), field.logical_type())),
        TableChange::AddColumn { field, .. } => clash(field.name(), field.logical_type()),
        TableChange::Widen { .. } => None,
    }
}

/// Records what `change` leaves the table's columns as.
fn record(store: &mut VaultStore, change: &TableChange) {
    let columns = store
        .columns
        .entry(change.table().name.to_string())
        .or_default();
    match change {
        TableChange::Create { schema, .. } => {
            for field in schema.fields().iter() {
                columns
                    .entry(field.name().to_owned())
                    .or_insert_with(|| field.logical_type().clone());
            }
        }
        TableChange::AddColumn { field, .. } => {
            columns
                .entry(field.name().to_owned())
                .or_insert_with(|| field.logical_type().clone());
        }
        TableChange::Widen { column, to, .. } => {
            let held = columns
                .entry(column.to_string())
                .or_insert_with(|| to.clone());
            *held = held.join(to);
        }
    }
}

struct VaultProbe(SharedVault);

impl Probe for VaultProbe {
    fn published<'a>(&'a self, table: &'a TableRef) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        let batches = self
            .0
            .lock()
            .unwrap()
            .published
            .get(table.name.as_ref())
            .cloned()
            .unwrap_or_default();
        Box::pin(async move { Ok(batches) })
    }
}

async fn certify_vault(name: &str, flag: Option<&str>) -> Report {
    let mut config = json!({ "store": name });
    if let Some(flag) = flag {
        config[flag] = json!(true);
    }
    certify_destination::<Vault>(config, &VaultProbe(vault(name))).await
}

#[tokio::test]
async fn a_correct_destination_passes_every_clause() {
    certify_vault("correct", None).await.assert_passed();
}

#[tokio::test]
async fn each_broken_destination_behavior_fails_exactly_its_clause() {
    let cases = [
        ("static_epoch", "D-EPOCH"),
        ("miscount", "D-COMMIT"),
        ("republish", "D-IDEMPOTENT"),
        ("forget_state", "D-STATE"),
        ("ignore_deletes", "D-STATE"),
        ("keep_staging", "D-DISCARD"),
        ("no_fence", "D-FENCE"),
        ("wrong_fence_kind", "D-FENCE"),
        ("forget_receipts", "D-IDEMPOTENT"),
        ("publish_all", "D-COMMIT"),
        ("local_state", "D-STATE"),
        ("stale_writes", "D-DISCARD"),
        ("replace_early", "D-REPLACE"),
        ("merge_appends", "D-MERGE"),
        ("refuse_repeated_changes", "D-SCHEMA"),
        ("ignore_added_columns", "D-SCHEMA"),
        ("accept_conflicts", "D-SCHEMA"),
        ("refuse_narrower", "D-SCHEMA"),
        ("uncoded_conflicts", "D-SCHEMA"),
        ("refuse_widening", "D-SCHEMA"),
    ];
    for (flag, clause) in cases {
        let report = certify_vault(flag, Some(flag)).await;
        assert_eq!(failed(&report), [clause], "{flag}: {report}");
    }
}

#[tokio::test]
async fn epochs_kept_by_one_connection_fail_the_clauses_that_span_two() {
    let report = certify_vault("local_epoch", Some("local_epoch")).await;
    assert_eq!(
        failed(&report),
        ["D-EPOCH", "D-DISCARD", "D-FENCE"],
        "{report}"
    );
}

#[tokio::test]
async fn a_destination_may_refuse_to_commit_segments_it_never_staged() {
    certify_vault("refuse_unstaged", Some("refuse_unstaged"))
        .await
        .assert_passed();
}

#[tokio::test]
async fn certification_passes_again_against_the_same_store() {
    certify_vault("rerun", None).await.assert_passed();
    certify_vault("rerun", None).await.assert_passed();
}

#[tokio::test]
async fn visible_staging_fails_every_clause_that_reads_published_data() {
    let report = certify_vault("publish_on_write", Some("publish_on_write")).await;
    assert_eq!(
        failed(&report),
        [
            "D-STAGING",
            "D-COMMIT",
            "D-IDEMPOTENT",
            "D-DISCARD",
            "D-REPLACE",
            "D-SCHEMA",
            "D-FENCE"
        ],
        "{report}"
    );
}

#[tokio::test]
async fn a_destination_that_cannot_connect_fails_every_clause() {
    let report = certify_vault("refused", Some("refuse_connect")).await;
    assert_eq!(failed(&report).len(), DESTINATION_CLAUSES.len());
}

#[tokio::test]
async fn clauses_for_capabilities_a_destination_lacks_are_skipped() {
    let report = certify_vault("minimal", Some("minimal")).await;
    report.assert_passed();
    for clause in ["D-REPLACE", "D-MERGE"] {
        assert!(
            matches!(report.outcome(clause), Some(Outcome::Skipped(_))),
            "{clause}: {report}"
        );
    }
    assert_eq!(
        report.outcome("D-SCHEMA"),
        Some(&Outcome::Passed),
        "minimal destinations add columns"
    );
}

#[tokio::test]
async fn the_schema_clause_is_skipped_for_a_destination_that_changes_no_schema() {
    let report = certify_vault("fixed_schema", Some("fixed_schema")).await;
    report.assert_passed();
    assert!(
        matches!(report.outcome("D-SCHEMA"), Some(Outcome::Skipped(_))),
        "{report}"
    );
}
