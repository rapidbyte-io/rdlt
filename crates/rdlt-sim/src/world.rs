//! The world one simulation shares: its workload, faults, destination store and findings.

mod capabilities;
pub(crate) mod ends;
mod hangs;
mod named;
mod pressure;
mod pushes;
mod reports;
#[cfg(test)]
mod tests;
mod told;

use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{Capabilities, ConnectorError, ConnectorErrorKind};
use tokio::sync::Notify;

use crate::changes::ChangeWorkload;
use crate::destination::Store;
use crate::owners::Owners;
use crate::reached::{Reach, Reached};
use crate::rng::SplitMix64;
use crate::source::Plans;
use crate::swarm::Features;
use crate::workload::{End, Workload};
use capabilities::capabilities;
use hangs::Hangs;
pub(crate) use named::{Named, Naming};
pub(crate) use pressure::Pressure;
pub(crate) use pushes::Largest;
use pushes::Pushes;
pub(crate) use reports::Reports;
pub(crate) use told::Heard;
pub use told::Told;
#[cfg(test)]
pub(crate) use told::{FORMS, quoting};

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
    /// Opening a table writer.
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
    /// The streams, by index, whose source an operator mended to checkpoint after its reads'
    /// last batch: after a read that sealed none of its rows was refused, or once no phase is
    /// left to read again the rows its reads left after their last checkpoint.
    mended: Mutex<BTreeSet<usize>>,
    /// What the first phase's reads of each stream, by index, ended on.
    ended: Mutex<BTreeSet<(usize, End)>>,
    phase: AtomicUsize,
    faulty: AtomicBool,
    rng: Mutex<SplitMix64>,
    /// Which calls hang and which placements stall, drawn apart from `rng`.
    hangs: Mutex<Hangs>,
    /// Which tries of commits answered transiently are left running, drawn apart from `rng`.
    delays: Mutex<SplitMix64>,
    /// The point whose next call a test has fail, faults on or off.
    failing: Mutex<Option<FaultPoint>>,
    pub(crate) store: Mutex<Store>,
    violations: Mutex<Vec<String>>,
    /// The furthest offset acknowledged to each partition of a stream that cannot read again,
    /// by stream and partition: what it no longer holds.
    pub(crate) acknowledged: Mutex<BTreeMap<(String, String), u64>>,
    /// The furthest offset a checkpoint sent of each partition of a stream that cannot read
    /// again, by stream and partition.
    pub(crate) served: Mutex<BTreeMap<(String, String), u64>>,
    /// The streams a reset is clearing while runs race it, whose committed positions move back:
    /// a row may land below where its partition was held.
    pub(crate) reset: Mutex<BTreeSet<String>>,
    /// What the source remembers of the reports it may hear, and what it heard.
    pub(crate) reports: Reports,
    /// How many rows of each followed partition have arrived, by stream and partition index,
    /// while a streaming phase produces them; every row has arrived when there is none.
    produced: Mutex<Option<BTreeMap<(usize, usize), usize>>>,
    /// Wakes reads that follow a partition when more of its rows arrive.
    pub(crate) arrived: Notify,
    /// Which partitions of its followed streams the source names.
    pub(crate) named: Named,
    /// What each connection to the source made since the oracle last looked planned and read.
    connections: Mutex<Vec<Arc<Plans>>>,
    /// How hard the source presses on the engine's memory budget.
    pressure: Mutex<Pressure>,
    /// What the source holds of the pushes it made and has not yet sent on.
    pub(crate) pushes: Pushes,
    /// The sessions, table writers and reads its connectors hold now.
    pub(crate) owners: Owners,
    /// What the connectors were told by their configurations, which their faults quote.
    pub(crate) heard: Heard,
    /// The paths its runs reached.
    reached: Mutex<Reached>,
}

/// What the draws of hangs mix into the fault draws' seed: "hangs" in ASCII.
const HANGS: u64 = 0x0068_616e_6773;

/// What the draws of tries left running mix into the fault draws' seed: "delays" in ASCII.
const DELAYS: u64 = 0x6465_6c61_7973;

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
            mended: Mutex::default(),
            ended: Mutex::default(),
            phase: AtomicUsize::new(0),
            faulty: AtomicBool::new(false),
            hangs: Mutex::new(Hangs::new(rng.clone().next_u64() ^ HANGS)),
            delays: Mutex::new(SplitMix64::new(rng.clone().next_u64() ^ DELAYS)),
            failing: Mutex::new(None),
            rng: Mutex::new(SplitMix64::new(rng.next_u64())),
            store: Mutex::new(Store::default()),
            violations: Mutex::new(Vec::new()),
            acknowledged: Mutex::default(),
            served: Mutex::default(),
            reset: Mutex::default(),
            reports: Reports::default(),
            produced: Mutex::new(None),
            arrived: Notify::new(),
            named: Named::default(),
            connections: Mutex::default(),
            pressure: Mutex::default(),
            pushes: Pushes::default(),
            owners: Owners::default(),
            heard: Heard::default(),
            reached: Mutex::default(),
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
            mended: Mutex::default(),
            ended: Mutex::default(),
            phase: AtomicUsize::new(0),
            faulty: AtomicBool::new(false),
            hangs: Mutex::new(Hangs::new(rng.clone().next_u64() ^ HANGS)),
            delays: Mutex::new(SplitMix64::new(rng.clone().next_u64() ^ DELAYS)),
            failing: Mutex::new(None),
            rng: Mutex::new(SplitMix64::new(rng.next_u64())),
            store: Mutex::new(Store::default()),
            violations: Mutex::new(Vec::new()),
            acknowledged: Mutex::default(),
            served: Mutex::default(),
            reset: Mutex::default(),
            reports: Reports::default(),
            produced: Mutex::new(None),
            arrived: Notify::new(),
            named: Named::default(),
            connections: Mutex::default(),
            pressure: Mutex::default(),
            pushes: Pushes::default(),
            owners: Owners::default(),
            heard: Heard::default(),
            reached: Mutex::default(),
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

    /// Has the source of stream `stream` checkpoint after its reads' last batch from now on.
    pub(crate) fn mend(&self, stream: usize) {
        self.mended.lock().insert(stream);
    }

    /// Whether the source of stream `stream` was mended to checkpoint after its reads' last batch.
    pub(crate) fn mended(&self, stream: usize) -> bool {
        self.mended.lock().contains(&stream)
    }

    /// Notes that a read of stream `stream` ended on `end`, where it is the first phase's.
    pub(crate) fn ended(&self, stream: usize, end: End) {
        if self.phase() == 0 {
            self.ended.lock().insert((stream, end));
        }
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

    /// A new connection's record of what its plans name and its reads do, which the oracle holds
    /// its run to.
    pub(crate) fn connected(&self) -> Arc<Plans> {
        let plans = Arc::new(Plans::default());
        self.connections.lock().push(Arc::clone(&plans));
        plans
    }

    /// The records of every connection made since this was last asked.
    pub(crate) fn connections(&self) -> Vec<Arc<Plans>> {
        mem::take(&mut *self.connections.lock())
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

    /// Turns fault injection on or off.
    pub fn set_faulty(&self, faulty: bool) {
        self.faulty.store(faulty, Ordering::SeqCst);
    }

    /// Records that a run reached `reach`.
    pub(crate) fn reach(&self, reach: Reach) {
        self.reached.lock().add(reach);
    }

    /// Records that the checks held how the workload's stream `stream` is read and written, as
    /// each way its first phase's reads ended.
    pub(crate) fn reach_combination(&self, stream: &str) {
        let streams = &self.workload.streams;
        let Some(index) = streams.iter().position(|found| found.name == stream) else {
            return;
        };
        let ended = self.ended.lock();
        let ends = ended.range((index, End::Checkpoint)..=(index, End::Unmarked));
        let mut reached = self.reached.lock();
        for (_, end) in ends {
            reached.add_combination(streams[index].combination(*end));
        }
    }

    /// Records that a run reached every path of `reached`.
    pub(crate) fn reach_all(&self, reached: Reached) {
        let mut held = self.reached.lock();
        *held = held.with(reached);
    }

    /// The paths its runs reached.
    pub(crate) fn reached(&self) -> Reached {
        *self.reached.lock()
    }

    /// A failure at `point`, where a test failed it, or when faults are on and the draw says so:
    /// mostly transient or rate-limited, sometimes permanent, and now and then a panic.
    ///
    /// # Panics
    ///
    /// Panics when the draw says the connector panics.
    pub(crate) fn fault(&self, point: FaultPoint) -> Option<ConnectorError> {
        if self
            .failing
            .lock()
            .take_if(|failing| *failing == point)
            .is_some()
        {
            let message = format!("a test failed the call at {point:?}");
            return Some(ConnectorError::new(ConnectorErrorKind::Data, message));
        }
        if !self.faulty.load(Ordering::SeqCst) {
            return None;
        }
        let mut rng = self.rng.lock();
        if !rng.chance(point.per_mille()) {
            return None;
        }
        let (message, cause, line_alone) =
            self.heard.quoted(format!("injected fault at {point:?}"));
        if line_alone {
            self.reach(Reach::LineSaid);
        }
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
        Some(match cause {
            Some(cause) => fault.with_source(std::io::Error::other(cause)),
            None => fault,
        })
    }

    /// Has the next call at `point` fail for good, faults on or off.
    #[cfg(test)]
    pub(crate) fn fail_next(&self, point: FaultPoint) {
        *self.failing.lock() = Some(point);
    }

    /// How many of its pipeline's commit calls a try answered transiently outlasts, where its
    /// transaction still runs: in three draws of four, none, one or two alike, drawn apart from
    /// every other fault, so the faults a seed met before fall as they did.
    pub(crate) fn outlasts(&self) -> Option<u8> {
        let drawn = self.delays.lock().below(4);
        u8::try_from(drawn)
            .ok()
            .and_then(|drawn| drawn.checked_sub(1))
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

/// A world registered as `name` whose source churns a stream runs follow, with that stream's
/// index.
#[cfg(test)]
pub(crate) fn churning(name: &str) -> (Registered, usize) {
    for seed in 0.. {
        let registered = World::register(name, &mut SplitMix64::new(seed));
        let world = registered.world();
        let followed = world
            .workload
            .streams
            .iter()
            .position(|stream| stream.follows);
        if let Some(index) = followed.filter(|_| world.workload.features.churn) {
            return (registered, index);
        }
    }
    unreachable!("some seed churns a stream runs follow")
}
