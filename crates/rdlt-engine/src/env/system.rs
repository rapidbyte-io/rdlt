use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::compute::{ComputePool, ComputePoolError, Cores, RayonPool};
use crate::env::{Env, Sleep};
use crate::wal::WalStore;

/// The production [`Env`]: the operating system's clock and random source, tokio's timers, a
/// rayon compute pool of a thread a core and, where given one, a write-ahead log store.
#[derive(Debug)]
pub struct SystemEnv {
    compute: RayonPool,
    cores: NonZeroUsize,
    wal: Option<Arc<dyn WalStore>>,
}

impl SystemEnv {
    /// Creates an environment within `cores` that runs CPU-bound work on a pool of a thread for
    /// each core, beside the runtime's workers, and keeps no write-ahead logs.
    pub fn try_new(cores: Cores) -> Result<Self, ComputePoolError> {
        Ok(Self {
            compute: RayonPool::try_new(cores)?,
            cores: cores.count(),
            wal: None,
        })
    }

    /// Creates an environment within the cores this process may run on, as
    /// [`try_new`](Self::try_new) does.
    ///
    /// The count of cores honours the process's CPU affinity mask and, on Linux, its cgroup's CPU
    /// quota.
    pub fn try_from_host() -> Result<Self, ComputePoolError> {
        let cores = Cores::try_from_host().map_err(|error| ComputePoolError(Box::new(error)))?;
        Self::try_new(cores)
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
        Self::try_new(Cores::new(NonZeroUsize::MIN)).expect("a one-thread pool starts")
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

    #[expect(
        clippy::disallowed_methods,
        reason = "SystemEnv is the production clock"
    )]
    fn sleep(&self, duration: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(duration))
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "SystemEnv is the production random source"
    )]
    fn random(&self) -> u64 {
        getrandom::u64().expect("the operating system random source is available")
    }

    fn compute(&self) -> &dyn ComputePool {
        &self.compute
    }

    fn cores(&self) -> NonZeroUsize {
        self.cores
    }

    fn wal(&self) -> Option<Arc<dyn WalStore>> {
        self.wal.clone()
    }
}
