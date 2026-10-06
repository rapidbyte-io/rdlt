use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::compute::{ComputePool, ComputePoolError, Cores, RayonPool};
use crate::env::{Clock, Env, Sleep};
use crate::wal::WalStore;

/// The production [`Env`]: the operating system's clock and random source, tokio's timers, a
/// rayon compute pool on the cores the runtime leaves and, where given one, a write-ahead log
/// store.
#[derive(Debug)]
pub struct SystemEnv {
    compute: RayonPool,
    wal: Option<Arc<dyn WalStore>>,
}

impl SystemEnv {
    /// Creates an environment within `cores` that runs CPU-bound work on a pool of the threads
    /// the runtime's workers leave, and keeps no write-ahead logs.
    pub fn try_new(cores: Cores) -> Result<Self, ComputePoolError> {
        Ok(Self {
            compute: RayonPool::try_new(cores)?,
            wal: None,
        })
    }

    /// The environment keeping write-ahead logs in `store`, usually a
    /// [`LocalWal`](crate::LocalWal).
    #[must_use]
    pub fn with_wal(self, store: Arc<dyn WalStore>) -> Self {
        Self {
            wal: Some(store),
            ..self
        }
    }
}

#[cfg(test)]
impl SystemEnv {
    /// An environment of one core: the pool's one thread beside a runtime's one worker.
    pub(crate) fn one_core() -> Self {
        let one = std::num::NonZeroUsize::MIN;
        Self::try_new(Cores::new(one, one)).expect("a one-thread pool starts")
    }
}

/// The production [`Clock`]: tokio's timers and the operating system's random source.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[expect(
        clippy::disallowed_methods,
        reason = "SystemClock is the production clock"
    )]
    fn sleep(&self, duration: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(duration))
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "SystemClock is the production random source"
    )]
    fn random(&self) -> u64 {
        getrandom::u64().expect("the operating system random source is available")
    }
}

impl Env for SystemEnv {
    #[expect(
        clippy::disallowed_methods,
        reason = "SystemEnv is the production clock"
    )]
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "SystemEnv is the production clock"
    )]
    fn instant(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        SystemClock.sleep(duration)
    }

    fn random(&self) -> u64 {
        SystemClock.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        &self.compute
    }

    fn wal(&self) -> Option<Arc<dyn WalStore>> {
        self.wal.clone()
    }
}
