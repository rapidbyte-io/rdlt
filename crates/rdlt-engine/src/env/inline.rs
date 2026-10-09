//! An environment for tests and benches whose compute jobs run on the calling thread, so a
//! paused test clock is the only clock they read.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant, SystemTime};

use crate::compute::{ComputePool, Inline};
use crate::env::{Clock, Env, Sleep, SystemClock};

/// The system's clock and randomness, tokio's timers, and an [`Inline`] compute pool of one core.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct InlineEnv;

impl Env for InlineEnv {
    #[expect(
        clippy::disallowed_methods,
        reason = "tests and benches take the system's clock"
    )]
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "tests and benches take tokio's clock, which a test may pause"
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
        &Inline
    }

    fn cores(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }
}
