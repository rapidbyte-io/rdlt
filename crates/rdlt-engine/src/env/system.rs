use std::time::{Duration, Instant, SystemTime};

use crate::compute::{ComputePool, RayonPool};
use crate::env::{Env, Sleep};

/// The production [`Env`]: the operating system's clock and random source, tokio's timers and a
/// rayon compute pool.
#[derive(Debug)]
pub struct SystemEnv {
    compute: RayonPool,
}

impl SystemEnv {
    /// Creates an environment that runs CPU-bound work on `compute`.
    pub fn new(compute: RayonPool) -> Self {
        Self { compute }
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
}
