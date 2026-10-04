use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::compute::{ComputePool, RayonPool};
use crate::env::{Clock, Env, Sleep};
use crate::wal::WalStore;

/// The production [`Env`]: the operating system's clock and random source, tokio's timers, a
/// rayon compute pool and, where given one, a write-ahead log store.
#[derive(Debug)]
pub struct SystemEnv {
    compute: RayonPool,
    wal: Option<Arc<dyn WalStore>>,
}

impl SystemEnv {
    /// Creates an environment that runs CPU-bound work on `compute` and keeps no write-ahead logs.
    pub fn new(compute: RayonPool) -> Self {
        Self { compute, wal: None }
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
