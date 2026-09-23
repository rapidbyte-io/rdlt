//! Injected sources of time and randomness.

mod system;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime};

use crate::compute::ComputePool;

pub use system::SystemEnv;

/// A future that completes after a duration measured on an [`Env`]'s clock.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Every source of nondeterminism the engine may use.
///
/// Production code uses [`SystemEnv`]. Deterministic simulation substitutes a virtual clock, a
/// seeded random source and an inline compute pool.
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
}
