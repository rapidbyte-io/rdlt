//! The simulated environment: virtual time, seeded randomness, inline compute.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rdlt_engine::{ComputePool, Env, Job, Sleep};

use crate::rng::SplitMix64;
use crate::seed::Seed;

/// Seconds from the Unix epoch to the simulated start of time, 2026-01-01T00:00:00Z.
const SIM_EPOCH_SECS: u64 = 1_767_225_600;

/// An [`Env`] for deterministic simulation.
///
/// Time comes from the tokio clock, which [`run`](crate::run) pauses so it advances only when
/// every task is idle. Randomness comes from a generator seeded by the run's [`Seed`], and compute
/// jobs run inline.
#[derive(Debug)]
pub struct SimEnv {
    rng: Mutex<SplitMix64>,
    start: tokio::time::Instant,
    compute: InlinePool,
}

impl SimEnv {
    /// Creates an environment seeded by `seed`, whose wall clock reads 2026-01-01T00:00:00Z now.
    pub fn new(seed: Seed) -> Self {
        Self {
            rng: Mutex::new(SplitMix64::new(seed.value())),
            start: tokio::time::Instant::now(),
            compute: InlinePool,
        }
    }
}

impl Env for SimEnv {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(SIM_EPOCH_SECS) + self.start.elapsed()
    }

    fn instant(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(duration))
    }

    fn random(&self) -> u64 {
        self.rng.lock().next_u64()
    }

    fn compute(&self) -> &dyn ComputePool {
        &self.compute
    }
}

/// A [`ComputePool`] that runs each job immediately on the calling thread.
#[derive(Clone, Copy, Debug, Default)]
pub struct InlinePool;

impl ComputePool for InlinePool {
    fn execute(&self, job: Job) {
        job();
    }
}
