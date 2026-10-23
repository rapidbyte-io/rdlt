//! An environment for tests and benches whose compute jobs run on the calling thread, so a
//! paused test clock is the only clock they read.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant, SystemTime};

use crate::compute::{ComputePool, Inline};
use crate::env::{Env, Sleep};

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

    #[expect(
        clippy::disallowed_methods,
        reason = "tests and benches take tokio's timers, which a test may pause"
    )]
    fn sleep(&self, duration: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(duration))
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "tests and benches take the system's random source"
    )]
    fn random(&self) -> u64 {
        getrandom::u64().expect("the operating system random source is available")
    }

    fn compute(&self) -> &dyn ComputePool {
        &Inline
    }

    fn cores(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }
}
