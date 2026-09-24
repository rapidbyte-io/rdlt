//! The world one simulation shares: its workload, faults, destination store and findings.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{Capabilities, ConnectorError, IdentifierCase, SchemaChanges, TypeKind};

use crate::destination::Store;
use crate::rng::SplitMix64;
use crate::workload::Workload;

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
}

impl FaultPoint {
    fn per_mille(self) -> u64 {
        match self {
            Self::Open | Self::Acknowledge => 30,
            Self::Read => 15,
            Self::Write | Self::Flush => 10,
            Self::CommitBefore | Self::CommitAfter => 40,
        }
    }
}

/// Everything one simulation's connectors share.
#[derive(Debug)]
pub struct World {
    /// What the source serves.
    pub workload: Workload,
    /// What the destination can store.
    pub capabilities: Capabilities,
    phase: AtomicUsize,
    faulty: AtomicBool,
    rng: Mutex<SplitMix64>,
    pub(crate) store: Mutex<Store>,
    violations: Mutex<Vec<String>>,
}

static WORLDS: LazyLock<Mutex<BTreeMap<String, Arc<World>>>> = LazyLock::new(Mutex::default);

impl World {
    /// A world whose workload and faults derive from `rng`, registered as `name`.
    pub fn register(name: &str, rng: &mut SplitMix64) -> Arc<Self> {
        let world = Arc::new(Self {
            workload: Workload::generate(rng),
            capabilities: capabilities(rng),
            phase: AtomicUsize::new(0),
            faulty: AtomicBool::new(false),
            rng: Mutex::new(SplitMix64::new(rng.next_u64())),
            store: Mutex::new(Store::default()),
            violations: Mutex::new(Vec::new()),
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

    /// A transient or rate-limited failure at `point`, when faults are on and the draw says so.
    pub(crate) fn fault(&self, point: FaultPoint) -> Option<ConnectorError> {
        if !self.faulty.load(Ordering::SeqCst) {
            return None;
        }
        let mut rng = self.rng.lock();
        if !rng.chance(point.per_mille()) {
            return None;
        }
        let message = format!("injected fault at {point:?}");
        Some(if rng.chance(250) {
            let after = Duration::from_millis(1 + rng.below(500));
            ConnectorError::rate_limited(message, Some(after))
        } else {
            ConnectorError::new(rdlt_connector::ConnectorErrorKind::Transient, message)
        })
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

/// Destination capabilities drawn from `rng`: whether it stores JSON, which widenings and nested
/// types it stores, and the identifier rules it names columns under.
fn capabilities(rng: &mut SplitMix64) -> Capabilities {
    let mut capabilities = Capabilities::minimal();
    capabilities.write_modes.replace = true;
    capabilities.write_modes.merge = true;
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
        1 => std::collections::BTreeSet::new(),
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
    capabilities.max_parallel_writers =
        std::num::NonZeroU16::new(u16::try_from(1 + rng.below(4)).unwrap_or(1))
            .expect("writer counts are positive");
    capabilities
}
