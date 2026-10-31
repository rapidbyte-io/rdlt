//! Injected sources of time and randomness, the cores a run may use, and where its events go.

#[cfg(any(test, feature = "bench"))]
mod inline;
mod system;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime};

use crate::compute::ComputePool;
use crate::event::Event;

#[cfg(any(test, feature = "bench"))]
pub(crate) use inline::InlineEnv;
pub use system::SystemEnv;

/// A future that completes after a duration measured on an [`Env`]'s clock.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Every source of nondeterminism the engine may use, the cores it may use, and what it tells of
/// its runs.
///
/// Production code uses [`SystemEnv`]. Deterministic simulation substitutes a virtual clock, a
/// seeded random source, an inline compute pool and a fixed count of cores.
pub trait Env: Send + Sync + 'static {
    /// Wall-clock time, for timestamps that are persisted or reported.
    fn now(&self) -> SystemTime;

    /// Monotonic time, for durations and deadlines.
    fn instant(&self) -> Instant;

    /// Completes after `duration` on this environment's monotonic clock.
    fn sleep(&self, duration: Duration) -> Sleep;

    /// A uniformly distributed random value.
    fn random(&self) -> u64;

    /// The pool that runs CPU-bound work.
    fn compute(&self) -> &dyn ComputePool;

    /// The cores the engine may use: lanes default to one per core.
    fn cores(&self) -> NonZeroUsize;

    /// Told each event of the engine's runs, at once, where it becomes true: nothing holds an
    /// event for it, so nothing waits outside the memory budget; by default nothing is told.
    ///
    /// It must not block, panic or call into the run; it hands the event on, to a channel. A
    /// panic fails the attempt it was told in, as any panic of the engine's tasks does, and one
    /// told between attempts or at the run's end reaches the task awaiting the run. A run's
    /// events are told one at a time, and none after its end.
    fn observe(&self, _event: &Event) {}

    /// Whether [`Env::observe`] is told anything: where it is not, the engine builds no event; by
    /// default it is not.
    fn observes(&self) -> bool {
        false
    }

    /// Told each change of what a memory budget holds reserved: `change` bytes more, or fewer
    /// where it is negative.
    #[cfg(feature = "sim")]
    fn charged(&self, _change: i64) {}
}
