use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::compute::{ComputePool, ComputePoolError, Cores, RayonPool};
use crate::env::{Env, Sleep};
use crate::event::Event;

/// What an embedder hands the events of its runs to.
type Observer = Arc<dyn Fn(&Event) + Send + Sync>;

/// The production [`Env`]: the operating system's clock and random source, tokio's timers, a
/// rayon compute pool of a thread a core, and the embedder's observer of events, if it has one.
pub struct SystemEnv {
    compute: RayonPool,
    cores: NonZeroUsize,
    observer: Option<Observer>,
}

impl SystemEnv {
    /// Creates an environment within `cores` that runs CPU-bound work on a pool of a thread for
    /// each core, beside the runtime's workers.
    pub fn try_new(cores: Cores) -> Result<Self, ComputePoolError> {
        Ok(Self {
            compute: RayonPool::try_new(cores)?,
            cores: cores.count(),
            observer: None,
        })
    }

    /// The environment, telling `observer` each event of the engine's runs.
    ///
    /// The observer is called on the engine's tasks as each event becomes true, so it must not
    /// block, panic or call into the run: it hands the event to a channel, as [`Env::observe`]
    /// says.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::sync::mpsc;
    ///
    /// use rdlt_engine::SystemEnv;
    ///
    /// let (events, received) = mpsc::channel();
    /// let env = SystemEnv::try_from_host()?.with_observer(Arc::new(move |event| {
    ///     // A receiver gone is no failure of the run.
    ///     events.send(event.clone()).ok();
    /// }));
    /// # drop((env, received));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn Fn(&Event) + Send + Sync>) -> Self {
        self.observer = Some(observer);
        self
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

    fn observe(&self, event: &Event) {
        if let Some(observer) = &self.observer {
            observer(event);
        }
    }

    fn observes(&self) -> bool {
        self.observer.is_some()
    }
}

impl fmt::Debug for SystemEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SystemEnv")
            .field("compute", &self.compute)
            .field("cores", &self.cores)
            .field("observed", &self.observer.is_some())
            .finish()
    }
}
