//! The world one simulation shares: its workload, faults, destination store and findings.

mod reports;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    Capabilities, CommitKind, ConnectorError, ConnectorErrorKind, IdentifierCase, IdentifierChars,
    SchemaChanges, TypeKind,
};
use tokio::sync::Notify;

use crate::changes::ChangeWorkload;
use crate::destination::Store;
use crate::rng::SplitMix64;
use crate::swarm::Features;
use crate::wal::SimWal;
use crate::workload::Workload;
pub(crate) use reports::Reports;

/// Where a connector can fail, and how often, in failures per thousand calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FaultPoint {
    Open,
    Read,
    Write,
    Flush,
    /// Before the commit lands.
    CommitBefore,
    /// After the commit lands, so its response is lost.
    CommitAfter,
    Acknowledge,
    /// Before a schema change applies.
    ApplyBefore,
    /// After a schema change applies, so its response is lost.
    ApplyAfter,
    /// Opening a table's writer.
    Writer,
    /// Closing a session.
    Close,
    /// Listing a stream's partitions.
    Partitions,
}

impl FaultPoint {
    fn per_mille(self) -> u64 {
        match self {
            Self::Open | Self::Acknowledge | Self::Close | Self::Partitions => 30,
            Self::Read => 15,
            Self::Write | Self::Flush => 10,
            Self::Writer => 20,
            Self::CommitBefore | Self::CommitAfter | Self::ApplyBefore | Self::ApplyAfter => 40,
        }
    }
}

/// Everything one simulation's connectors share.
#[derive(Debug)]
pub struct World {
    /// What the source serves.
    pub workload: Workload,
    /// What the change source serves; empty in a world whose source is not one.
    pub changes: ChangeWorkload,
    /// What the destination can store, until it is granted adding columns.
    capabilities: Capabilities,
    /// Whether the destination was granted adding columns, as an operator would after its runs
    /// were refused for want of it.
    granted: AtomicBool,
    phase: AtomicUsize,
    faulty: AtomicBool,
    rng: Mutex<SplitMix64>,
    pub(crate) store: Mutex<Store>,
    violations: Mutex<Vec<String>>,
    /// Where the engine keeps its write-ahead logs, which outlive runs as a disk does.
    pub(crate) wal: Arc<SimWal>,
    /// The furthest offset acknowledged to each partition of a stream that cannot read again,
    /// by stream and partition: what it no longer holds.
    pub(crate) acknowledged: Mutex<BTreeMap<(String, String), u64>>,
    /// The streams a reset is clearing while runs race it, whose committed positions move back:
    /// an acknowledgement of a commit that landed before the reset may reach the source after it.
    pub(crate) reset: Mutex<BTreeSet<String>>,
    /// What the source remembers of the reports it may hear, and what it heard.
    pub(crate) reports: Reports,
    /// How many rows of each followed partition have arrived, by stream and partition index,
    /// while a streaming phase produces them; every row has arrived when there is none.
    produced: Mutex<Option<BTreeMap<(usize, usize), usize>>>,
    /// Wakes reads that follow a partition when more of its rows arrive.
    pub(crate) arrived: Notify,
}

static WORLDS: LazyLock<Mutex<BTreeMap<String, Arc<World>>>> = LazyLock::new(Mutex::default);

impl World {
    /// A world whose workload and faults derive from `rng`, registered as `name`.
    pub fn register(name: &str, rng: &mut SplitMix64) -> Arc<Self> {
        let features = Features::draw(rng);
        let world = Arc::new(Self {
            workload: Workload::generate(rng, features),
            changes: ChangeWorkload::default(),
            capabilities: capabilities(rng, features),
            granted: AtomicBool::new(false),
            phase: AtomicUsize::new(0),
            faulty: AtomicBool::new(false),
            rng: Mutex::new(SplitMix64::new(rng.next_u64())),
            store: Mutex::new(Store::default()),
            violations: Mutex::new(Vec::new()),
            wal: Arc::default(),
            acknowledged: Mutex::default(),
            reset: Mutex::default(),
            reports: Reports::default(),
            produced: Mutex::new(None),
            arrived: Notify::new(),
        });
        WORLDS.lock().insert(name.to_owned(), Arc::clone(&world));
        world
    }

    /// A world whose change workload and faults derive from `rng`, and which of its merge streams
    /// keep history from `apart`, registered as `name`: its destination merges changes, removing
    /// rows or marking them deleted, keeps columns updates leave unchanged, and keeps history.
    pub(crate) fn register_changes(
        name: &str,
        rng: &mut SplitMix64,
        apart: &mut SplitMix64,
    ) -> Arc<Self> {
        let features = Features::draw(rng);
        let mut capabilities = Capabilities::minimal();
        capabilities.write_modes.merge = true;
        capabilities.write_modes.history = true;
        capabilities.delete_modes.hard = true;
        capabilities.delete_modes.soft = true;
        capabilities.partial_updates = true;
        capabilities.merge_changes = true;
        capabilities.drop_tables = true;
        capabilities.max_parallel_writers =
            std::num::NonZeroU16::new(u16::try_from(1 + rng.below(4)).unwrap_or(1))
                .expect("writer counts are positive");
        let world = Arc::new(Self {
            workload: Workload::empty(features),
            changes: ChangeWorkload::generate(rng, features, apart),
            capabilities,
            granted: AtomicBool::new(false),
            phase: AtomicUsize::new(0),
            faulty: AtomicBool::new(false),
            rng: Mutex::new(SplitMix64::new(rng.next_u64())),
            store: Mutex::new(Store::default()),
            violations: Mutex::new(Vec::new()),
            wal: Arc::default(),
            acknowledged: Mutex::default(),
            reset: Mutex::default(),
            reports: Reports::default(),
            produced: Mutex::new(None),
            arrived: Notify::new(),
        });
        WORLDS.lock().insert(name.to_owned(), Arc::clone(&world));
        world
    }

    /// The world registered as `name`.
    pub(crate) fn named(name: &str) -> Option<Arc<Self>> {
        WORLDS.lock().get(name).cloned()
    }

    /// Removes the world registered as `name`.
    pub fn unregister(name: &str) {
        WORLDS.lock().remove(name);
    }

    /// What the destination can store.
    pub fn capabilities(&self) -> Capabilities {
        let mut capabilities = self.capabilities.clone();
        capabilities.schema_changes.add_column |= self.granted.load(Ordering::SeqCst);
        capabilities
    }

    /// Lets the destination add columns from now on.
    pub fn grant_add_column(&self) {
        self.granted.store(true, Ordering::SeqCst);
    }

    /// How many of the `rows` rows of partition `partition` of stream `stream` have arrived.
    pub(crate) fn available(&self, stream: usize, partition: usize, rows: usize) -> usize {
        match &*self.produced.lock() {
            Some(produced) => produced
                .get(&(stream, partition))
                .map_or(rows, |arrived| (*arrived).min(rows)),
            None => rows,
        }
    }

    /// Sets how many rows of each followed partition have arrived, or none for every row, and
    /// wakes the reads that follow them.
    pub(crate) fn produce(&self, produced: Option<BTreeMap<(usize, usize), usize>>) {
        *self.produced.lock() = produced;
        self.arrived.notify_waiters();
    }

    /// The phase the source serves.
    pub fn phase(&self) -> usize {
        self.phase.load(Ordering::SeqCst)
    }

    /// Moves the source to `phase`.
    pub fn set_phase(&self, phase: usize) {
        self.phase.store(phase, Ordering::SeqCst);
    }

    /// Turns fault injection on or off, the write-ahead log's disk's included.
    pub fn set_faulty(&self, faulty: bool) {
        self.faulty.store(faulty, Ordering::SeqCst);
        // Drawn only where logs are kept, so every other seed's faults fall as they did.
        let logged = faulty && self.workload.features.wal;
        let draws = logged.then(|| SplitMix64::new(self.rng.lock().next_u64()));
        self.wal.set_faults(draws);
    }

    /// Crashes the worker running `pipeline`: what its logs had not made durable is lost, but for
    /// a part the draw keeps, which may be torn.
    pub(crate) fn crash_logs(&self, pipeline: &rdlt_connector::PipelineId) {
        let mut rng = self.rng.lock();
        self.wal.crash(pipeline, &mut rng);
    }

    /// A failure at `point`, when faults are on and the draw says so: mostly transient or
    /// rate-limited, sometimes permanent, and now and then a panic.
    ///
    /// # Panics
    ///
    /// Panics when the draw says the connector panics.
    pub(crate) fn fault(&self, point: FaultPoint) -> Option<ConnectorError> {
        if !self.faulty.load(Ordering::SeqCst) {
            return None;
        }
        let mut rng = self.rng.lock();
        if !rng.chance(point.per_mille()) {
            return None;
        }
        let message = format!("injected fault at {point:?}");
        let fault = match rng.below(20) {
            0 => {
                drop(rng);
                panic!("injected panic at {point:?}");
            }
            1 | 2 => ConnectorError::new(ConnectorErrorKind::Data, message),
            3..=7 => {
                // Now and then a connector asks for years: the engine waits no longer than its
                // policy's longest delay, or the run would outlast the simulation's limit.
                let after = if rng.chance(100) {
                    Duration::from_hours(87_600)
                } else {
                    Duration::from_millis(1 + rng.below(500))
                };
                ConnectorError::rate_limited(message, Some(after))
            }
            _ => ConnectorError::new(ConnectorErrorKind::Transient, message),
        };
        Some(fault)
    }

    /// Sleeps a short random while, sometimes, when faults are on.
    pub(crate) async fn latency(&self) {
        if !self.faulty.load(Ordering::SeqCst) {
            return;
        }
        let pause = {
            let mut rng = self.rng.lock();
            rng.chance(100)
                .then(|| Duration::from_millis(rng.below(50)))
        };
        if let Some(pause) = pause {
            tokio::time::sleep(pause).await;
        }
    }

    /// Records a broken invariant.
    pub(crate) fn violation(&self, finding: String) {
        self.violations.lock().push(finding);
    }

    /// Every broken invariant recorded so far.
    pub fn violations(&self) -> Vec<String> {
        self.violations.lock().clone()
    }
}

/// Destination capabilities drawn from `rng`: how it commits, which types it stores natively,
/// whether it stores JSON, which widenings and nested types it stores, whether it adds columns,
/// and the identifier rules it names tables and columns under.
fn capabilities(rng: &mut SplitMix64, features: Features) -> Capabilities {
    let mut capabilities = Capabilities::minimal();
    if rng.chance(500) {
        capabilities.commit = CommitKind::Manifest;
    }
    if features.narrow {
        // Text always among them, every other type stored natively or as text.
        capabilities
            .types
            .retain(|kind| *kind == TypeKind::Utf8 || rng.chance(500));
    }
    capabilities.write_modes.replace = true;
    capabilities.write_modes.merge = true;
    capabilities.drop_tables = true;
    if rng.chance(700) {
        capabilities.nested.json = true;
        capabilities.types.insert(TypeKind::Json);
    }
    if rng.chance(500) {
        capabilities.nested.structs = true;
        capabilities.types.insert(TypeKind::Struct);
    }
    if rng.chance(500) {
        capabilities.nested.lists = true;
        capabilities.types.insert(TypeKind::List);
    }
    if rng.chance(500) {
        capabilities.types.insert(TypeKind::Uuid);
    }
    let all = SchemaChanges::all();
    capabilities.schema_changes.widenings = match rng.below(3) {
        0 => all.widenings,
        1 => BTreeSet::new(),
        _ => all
            .widenings
            .into_iter()
            .filter(|_| rng.chance(500))
            .collect(),
    };
    capabilities.identifiers.case = match rng.below(3) {
        0 => IdentifierCase::Preserve,
        1 => IdentifierCase::Lower,
        _ => IdentifierCase::Upper,
    };
    let max_len = [63, 16, 12][usize::try_from(rng.below(3)).unwrap_or(0)];
    capabilities.identifiers.max_len =
        std::num::NonZeroU16::new(max_len).expect("identifier lengths are positive");
    if rng.chance(300) {
        capabilities.identifiers.reserved.insert("value".to_owned());
    }
    if features.settings && rng.chance(250) {
        capabilities.schema_changes.add_column = false;
    }
    if features.identifiers {
        identifiers(rng, &mut capabilities);
    }
    capabilities.max_parallel_writers =
        std::num::NonZeroU16::new(u16::try_from(1 + rng.below(4)).unwrap_or(1))
            .expect("writer counts are positive");
    capabilities
}

/// Identifier rules beyond the common ones: any characters, more reserved words, and reserved
/// table prefixes.
fn identifiers(rng: &mut SplitMix64, capabilities: &mut Capabilities) {
    let rules = &mut capabilities.identifiers;
    if rng.chance(500) {
        rules.chars = IdentifierChars::Any;
    }
    for word in ["id", "key", "offset", "_rdlt_load_id", "d0"] {
        if rng.chance(250) {
            rules.reserved.insert(word.to_owned());
        }
    }
    for prefix in ["s", "S0", "_s", "tmp_"] {
        if rng.chance(300) {
            rules.reserved_table_prefixes.insert(prefix.to_owned());
        }
    }
}
