use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use bytes::Bytes;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{DESTINATION_CLAUSES, Outcome, Probe, Report, certify_destination, certify_source};
use crate::capabilities::Capabilities;
use crate::catalog::{Catalog, Checkpointing, StreamSpec};
use crate::commit::{CommitMeta, Receipt};
use crate::destination::{
    DestinationConnector, OpenContext, Opened, Session, TableChange, TableRef, TableWriter,
    WriteStats,
};
use crate::emitter::Emitter;
use crate::error::{ConnectorError, Result};
use crate::id::{CommitSeq, Epoch, LoadId, PipelineId, SegmentId, StreamName};
use crate::sink::Push;
use crate::source::{Partition, ReadStream, SourceConnector, Streams};
use crate::spec::{BoxFuture, ConnectContext};
use crate::state::{StateChange, StateRecord, StreamState};

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
        }
    }
}

struct Pages {
    config: PagesConfig,
    discovered: AtomicBool,
}

impl SourceConnector for Pages {
    const ID: &'static str = "io.test.pages";
    const VERSION: &'static str = "0.0.1";
    type Config = PagesConfig;

    async fn connect(config: PagesConfig, _context: &ConnectContext) -> Result<Self> {
        if config.refuse_connect {
            return Err(ConnectorError::config("refused"));
        }
        Ok(Self {
            config,
            discovered: AtomicBool::new(false),
        })
    }

    async fn check(&self) -> Result<()> {
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
}

#[derive(Default)]
struct VaultStore {
    epochs: BTreeMap<PipelineId, u64>,
    state: BTreeMap<PipelineId, BTreeMap<String, StateRecord>>,
    receipts: BTreeMap<(LoadId, CommitSeq), Receipt>,
    staged: BTreeMap<(PipelineId, SegmentId), Vec<(String, RecordBatch)>>,
    published: BTreeMap<String, Vec<RecordBatch>>,
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
}

impl DestinationConnector for Vault {
    const ID: &'static str = "io.test.vault";
    const VERSION: &'static str = "0.0.1";
    type Config = VaultConfig;
    type Session = VaultSession;

    fn capabilities(&self) -> Capabilities {
        Capabilities::minimal()
    }

    async fn connect(config: VaultConfig, _context: &ConnectContext) -> Result<Self> {
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

    async fn apply_schema(&mut self, _change: &TableChange) -> Result<()> {
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<VaultWriter> {
        Ok(VaultWriter {
            config: Arc::clone(&self.config),
            stores: self.stores.clone(),
            pipeline: self.pipeline.clone(),
            epoch: self.epoch,
            table: table.name.to_string(),
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
        let mut rows = 0;
        for segment in self.segments_to_publish(&store, meta) {
            let key = (self.pipeline.clone(), segment);
            let batches = if self.config.republish {
                store.staged.get(&key).cloned().unwrap_or_default()
            } else {
                store.staged.remove(&key).unwrap_or_default()
            };
            for (table, batch) in batches {
                rows += batch.num_rows() as u64;
                store.published.entry(table).or_default().push(batch);
            }
        }
        if !self.config.forget_state {
            let mut local = self.stores.local.lock().unwrap();
            let states = if self.config.local_state {
                &mut local.state
            } else {
                &mut store.state
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
            .push((self.table.clone(), batch));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        Ok(WriteStats::default())
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
