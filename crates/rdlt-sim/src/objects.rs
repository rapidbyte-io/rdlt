//! A world's write-ahead logs kept in an object store in memory, through the engine's
//! [`ObjectStoreWal`], whose requests fail, stall, race, lose their answers and list stale as the
//! seed draws.

#[cfg(test)]
mod tests;

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
use object_store::memory::InMemory;
use object_store::path::Path;
use parking_lot::Mutex;
use rdlt_engine::{Clock, ObjectStoreOptions, ObjectStoreWal, Sleep};
use rdlt_testkit::objects::{Call, Fault, Faulty, Op, Plan};

use crate::rng::SplitMix64;

/// Failures per thousand requests, while faulty.
const FAULTS: u64 = 40;

/// The prefix the logs are kept beneath.
const PREFIX: &str = "logs";

/// Logs in an object store in memory, which outlive runs as a bucket does.
#[derive(Debug)]
pub(crate) struct ObjectLogs {
    pub(crate) wal: Arc<ObjectStoreWal>,
    objects: Arc<Faulty<InMemory>>,
    /// The draws deciding which requests fail, while faulty.
    faults: Arc<Mutex<Option<SplitMix64>>>,
}

/// The store's clock: the simulation's paused one, and draws of its own.
#[derive(Debug)]
struct SimClock {
    rng: Mutex<SplitMix64>,
}

impl Clock for SimClock {
    fn sleep(&self, duration: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(duration))
    }

    fn random(&self) -> u64 {
        self.rng.lock().next_u64()
    }
}

impl ObjectLogs {
    /// Logs whose parts, attempts, deadlines and clock `rng` draws: parts of 4 KiB to a MiB, so
    /// commits are uploaded whole and in parts, each part count bounding a log well past what a
    /// world's loads log, and one to four attempts a request, so a fault sometimes outlasts them.
    ///
    /// # Panics
    ///
    /// Panics where the probe refuses the store, which an unfaulted store in memory never is.
    pub(crate) async fn open(mut rng: SplitMix64) -> Self {
        let part = [4 << 10, 64 << 10, 1 << 20][usize::try_from(rng.below(3)).unwrap_or(0)];
        let attempts = u32::try_from(1 + rng.below(4)).unwrap_or(1);
        let options = ObjectStoreOptions::default()
            .with_part_bytes(NonZeroUsize::new(part).expect("parts are not empty"))
            .with_attempts(NonZeroU32::new(attempts).expect("attempts are not none"))
            .with_backoff(Duration::from_millis(10), Duration::from_millis(200))
            .with_deadline(Duration::from_secs(1 + rng.below(4)), Duration::ZERO);
        let faults = Arc::new(Mutex::new(None));
        let objects = Arc::new(Faulty::new(InMemory::new(), plan(Arc::clone(&faults))));
        let clock = Arc::new(SimClock {
            rng: Mutex::new(SplitMix64::new(rng.next_u64())),
        });
        let wal = ObjectStoreWal::open(Arc::clone(&objects) as _, PREFIX, clock, options)
            .await
            .expect("an object store in memory does what a log needs");
        Self {
            wal: Arc::new(wal),
            objects,
            faults,
        }
    }

    /// Makes requests fail now and then, as `faults` draws, or never again.
    pub(crate) fn set_faults(&self, faults: Option<SplitMix64>) {
        *self.faults.lock() = faults;
    }

    /// Whether it holds any log: an open one, or what a removal left.
    pub(crate) async fn holds_logs(&self) -> bool {
        let pipelines = Path::from(PREFIX);
        let listed: Vec<_> = self
            .objects
            .inner()
            .list(Some(&pipelines))
            .try_collect()
            .await
            .expect("a store in memory lists");
        listed
            .iter()
            .any(|meta| meta.location.as_ref().starts_with("logs/p."))
    }
}

/// The plan drawing each request's fault from `faults`, while there are draws.
fn plan(faults: Arc<Mutex<Option<SplitMix64>>>) -> Plan {
    Box::new(move |call| {
        let mut faults = faults.lock();
        let Some(rng) = faults.as_mut() else {
            return Fault::None;
        };
        if rng.chance(FAULTS) {
            fault(call, rng)
        } else {
            Fault::None
        }
    })
}

/// A fault `call` may meet, drawn from `rng`: a listing of a log's chunks may miss the newest,
/// but no listing of the open logs does, which the store requires of its listings.
fn fault(call: &Call, rng: &mut SplitMix64) -> Fault {
    let slow = Fault::Slow(u32::try_from(1 + rng.below(8)).unwrap_or(1));
    let choices = match call.op {
        Op::Put { create: true } => vec![
            Fault::Fail,
            slow,
            Fault::Hang,
            Fault::Raced,
            Fault::Answerless,
        ],
        Op::List if call.key.contains("/logs/") => {
            vec![Fault::Fail, slow, Fault::Hang, Fault::Stale]
        }
        Op::List | Op::Abort => vec![Fault::Fail, slow, Fault::Hang],
        _ => vec![Fault::Fail, slow, Fault::Hang, Fault::Answerless],
    };
    let count = u64::try_from(choices.len()).unwrap_or(1);
    let index = usize::try_from(rng.below(count)).unwrap_or(0);
    choices[index]
}
