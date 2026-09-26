//! The simulated environment: virtual time, seeded randomness, inline compute.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rdlt_engine::{ComputePool, Env, Job, RayonPool, Sleep};

use crate::rng::SplitMix64;
use crate::seed::Seed;

/// Seconds from the Unix epoch to the simulated start of time, 2026-01-01T00:00:00Z.
const SIM_EPOCH_SECS: u64 = 1_767_225_600;

/// An [`Env`] for deterministic simulation.
///
/// Time comes from the tokio clock, which [`run`](crate::run) pauses so it advances only when
/// every task is idle. Randomness comes from a generator seeded by the run's [`Seed`], and compute
/// jobs run inline. A perturbed environment varies the order tasks run in, as the seed says.
#[derive(Debug)]
pub struct SimEnv {
    rng: Mutex<SplitMix64>,
    start: tokio::time::Instant,
    compute: SimPool,
}

impl SimEnv {
    /// Creates an environment seeded by `seed`, whose wall clock reads 2026-01-01T00:00:00Z now.
    pub fn new(seed: Seed) -> Self {
        Self {
            rng: Mutex::new(SplitMix64::new(seed.value())),
            start: tokio::time::Instant::now(),
            compute: SimPool {
                perturbation: Mutex::new(SplitMix64::new(!seed.value())),
                perturbed: AtomicBool::new(false),
                threads: None,
            },
        }
    }

    /// Creates an environment seeded by `seed` whose compute jobs run on a pool of four threads,
    /// for a runtime of many threads on the real clock.
    ///
    /// # Panics
    ///
    /// Panics when the pool's threads fail to start.
    pub fn threaded(seed: Seed) -> Self {
        let threads = NonZeroUsize::new(4).expect("four is not zero");
        let pool = RayonPool::new(threads).expect("the compute pool's threads start");
        let mut env = Self::new(seed);
        env.compute.threads = Some(pool);
        env
    }

    /// Turns scheduling perturbation on or off: while on, sleeps last a little longer and some
    /// compute jobs run on tasks of their own after a few turns of the scheduler, both as a
    /// generator of their own draws, so the same seed perturbs alike.
    pub fn perturb(&self, on: bool) {
        self.compute.perturbed.store(on, Ordering::SeqCst);
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
        let jitter = self.compute.jitter(duration);
        Box::pin(tokio::time::sleep(duration + jitter))
    }

    fn random(&self) -> u64 {
        self.rng.lock().next_u64()
    }

    fn compute(&self) -> &dyn ComputePool {
        &self.compute
    }
}

/// The simulation's [`ComputePool`]: jobs run on the calling thread, or when perturbed, one in four
/// on a task of its own after up to eight turns of the scheduler.
#[derive(Debug)]
struct SimPool {
    perturbation: Mutex<SplitMix64>,
    perturbed: AtomicBool,
    /// Threads jobs run on instead, where there are any.
    threads: Option<RayonPool>,
}

impl SimPool {
    /// How much longer a sleep of `duration` lasts: when perturbed, up to a tenth of it and a
    /// millisecond more.
    fn jitter(&self, duration: Duration) -> Duration {
        if !self.perturbed.load(Ordering::SeqCst) {
            return Duration::ZERO;
        }
        let most = u64::try_from(duration.as_micros() / 10).unwrap_or(u64::MAX) + 1_000;
        Duration::from_micros(self.perturbation.lock().below(most))
    }

    /// How many turns of the scheduler `job` waits before it runs, if it runs on a task of its
    /// own.
    fn deferral(&self) -> Option<u64> {
        if !self.perturbed.load(Ordering::SeqCst) {
            return None;
        }
        let mut rng = self.perturbation.lock();
        rng.chance(250).then(|| rng.below(8))
    }
}

impl ComputePool for SimPool {
    fn execute(&self, job: Job) {
        if let Some(threads) = &self.threads {
            return threads.execute(job);
        }
        let Some(turns) = self.deferral() else {
            return job();
        };
        tokio::spawn(async move {
            for _ in 0..=turns {
                tokio::task::yield_now().await;
            }
            job();
        });
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
