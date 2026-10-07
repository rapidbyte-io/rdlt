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
use crate::env::SimEnv;
use crate::objects::ObjectLogs;
use crate::rng::SplitMix64;
use crate::seed::Seed;
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
    /// Where the engine keeps them instead, in a world that keeps them in an object store.
    objects: Mutex<Option<Arc<ObjectLogs>>>,
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
    /// How hard the source presses on the engine's memory budget.
    pressure: Mutex<Pressure>,
}

/// How hard a source presses on the engine's memory budget: the bytes each Arrow batch it pushes
/// keeps alive beside its rows, and the bytes each cursor carries beside its offset.
///
/// Both are drawn against the limits the engine admits within, so a push or a cursor is never
/// refused, and several of them fill their share of the budget: pushes then wait for room as
/// lowering releases it, and cursors for a commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Pressure {
    /// Bytes: the allocation each Arrow batch keeps alive, of which its rows take the first.
    pub(crate) ballast: usize,
    /// Bytes: what each cursor carries beside its offset.
    pub(crate) pad: usize,
}

impl Pressure {
    /// The pressure drawn from `rng` for an engine admitting within `limits`, of a budget whose
    /// cursors may take `cursors` bytes, reading `partitions` in all: in three worlds of four a
    /// push of a fifth to nine tenths of a frame, and cursors of half to all a cursor may be, the
    /// cursors of every partition together within a quarter of the state an open may answer.
    pub(crate) fn draw(
        rng: &mut SplitMix64,
        limits: &rdlt_wire::Limits,
        cursors: u64,
        partitions: u64,
    ) -> Self {
        let within = |limit: u64, low: u64, high: u64, chance: u64, rng: &mut SplitMix64| {
            let low = limit / low;
            let bytes = rng
                .chance(chance)
                .then(|| low + rng.below((limit / high).saturating_sub(low).max(1)));
            usize::try_from(bytes.unwrap_or(0)).unwrap_or(0)
        };
        let frame = limits.frame_bytes.saturating_mul(9) / 10;
        let state = limits.state_bytes / 4 / partitions.saturating_add(1);
        Self {
            ballast: within(frame, 5, 1, 750, rng),
            // A cursor carries its offset and its field names beside its padding.
            pad: within(
                cursors
                    .min(limits.cursor_bytes)
                    .min(state)
                    .saturating_sub(64),
                2,
                1,
                750,
                rng,
            ),
        }
    }
}

/// What the object store's draws mix into the disk's and the seed: "objects" in ASCII.
const OBJECTS: u64 = 0x006f_626a_6563_7473;

static WORLDS: LazyLock<Mutex<BTreeMap<String, Arc<World>>>> = LazyLock::new(Mutex::default);

/// A world's entry in the registry, under its name, which it leaves when this drops: when its
/// simulation ends, or unwinds from a panic.
#[derive(Debug)]
#[must_use = "the world leaves the registry when this drops"]
pub(crate) struct Registered {
    name: String,
    world: Arc<World>,
}

impl Registered {
    /// Registers `world` as `name`.
    ///
    /// # Panics
    ///
    /// Panics when a world is registered as `name` already: the runs of one seed share a name,
    /// so no two of them may be in flight in one process.
    fn enter(name: &str, world: Arc<World>) -> Self {
        let mut worlds = WORLDS.lock();
        assert!(
            !worlds.contains_key(name),
            "a world is registered as {name} already: two runs of one seed are in flight"
        );
        worlds.insert(name.to_owned(), Arc::clone(&world));
        Self {
            name: name.to_owned(),
            world,
        }
    }

    /// The world registered.
    pub(crate) fn world(&self) -> &Arc<World> {
        &self.world
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        WORLDS.lock().remove(&self.name);
    }
}

impl World {
    /// A world whose workload and faults derive from `rng`, registered as `name` until what this
    /// returns drops.
    ///
    /// # Panics
    ///
    /// Panics when a world is registered as `name` already. The simulation's tests run under
    /// nextest, one process a test, so two tests that check one seed never meet here; under
    /// `cargo test`, which runs a binary's tests on threads of one process, they would.
    pub(crate) fn register(name: &str, rng: &mut SplitMix64) -> Registered {
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
            objects: Mutex::default(),
            acknowledged: Mutex::default(),
            reset: Mutex::default(),
            reports: Reports::default(),
            produced: Mutex::new(None),
            arrived: Notify::new(),
            pressure: Mutex::default(),
        });
        Registered::enter(name, world)
    }

    /// Presses on the engine's memory budget as `pressure` says from now on.
    pub(crate) fn press(&self, pressure: Pressure) {
        *self.pressure.lock() = pressure;
    }

    /// How hard the source presses on the engine's memory budget.
    pub(crate) fn pressure(&self) -> Pressure {
        *self.pressure.lock()
    }

    /// A world whose change workload and faults derive from `rng`, and which of its merge streams
    /// keep history from `apart`, registered as `name` until what this returns drops: its
    /// destination merges changes, removing rows or marking them deleted, keeps columns updates
    /// leave unchanged, and keeps history.
    ///
    /// # Panics
    ///
    /// Panics when a world is registered as `name` already. The simulation's tests run under
    /// nextest, one process a test, so two tests that check one seed never meet here; under
    /// `cargo test`, which runs a binary's tests on threads of one process, they would.
    pub(crate) fn register_changes(
        name: &str,
        rng: &mut SplitMix64,
        apart: &mut SplitMix64,
    ) -> Registered {
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
            objects: Mutex::default(),
            acknowledged: Mutex::default(),
            reset: Mutex::default(),
            reports: Reports::default(),
            produced: Mutex::new(None),
            arrived: Notify::new(),
            pressure: Mutex::default(),
        });
        Registered::enter(name, world)
    }

    /// The world registered as `name`.
    pub(crate) fn named(name: &str) -> Option<Arc<Self>> {
        WORLDS.lock().get(name).cloned()
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
        if let Some(objects) = &*self.objects.lock() {
            // The object store's draws are the disk's, mixed apart.
            let draws = draws
                .clone()
                .map(|mut rng| SplitMix64::new(rng.next_u64() ^ OBJECTS));
            objects.set_faults(draws);
        }
        self.wal.set_faults(draws);
    }

    /// Gives `env` the world's write-ahead logs: in an object store where the world keeps them
    /// there, its options drawn from `seed` apart from every other draw, else in the simulation's
    /// own store.
    pub(crate) async fn keep_logs(&self, env: &SimEnv, seed: Seed) {
        let features = self.workload.features;
        if features.wal && features.objects {
            let drawn = SplitMix64::new(seed.value() ^ OBJECTS);
            *self.objects.lock() = Some(Arc::new(ObjectLogs::open(drawn).await));
        }
        let logs = match &*self.objects.lock() {
            Some(objects) => Arc::clone(&objects.wal) as Arc<dyn rdlt_engine::WalStore>,
            None => Arc::clone(&self.wal) as _,
        };
        env.keep_logs(logs);
    }

    /// Whether the logs hold any log: an open one, or what a removal left.
    pub(crate) async fn holds_logs(&self) -> bool {
        let objects = self.objects.lock().clone();
        match objects {
            Some(objects) => objects.holds_logs().await,
            None => self.wal.holds_logs(),
        }
    }

    /// Crashes the worker running `pipeline`: every chunk its logs staged and did not publish is
    /// lost.
    pub(crate) fn crash_logs(&self, pipeline: &rdlt_connector::PipelineId) {
        self.wal.crash(pipeline);
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
    let max_len = [63, 32, 16][usize::try_from(rng.below(3)).unwrap_or(0)];
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
