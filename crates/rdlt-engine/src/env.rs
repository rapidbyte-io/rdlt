//! Injected sources of time and randomness.

mod system;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use rdlt_connector::LoadId;

use crate::compute::ComputePool;
use crate::wal::WalStore;

pub use system::SystemEnv;

/// A future that completes after a duration measured on an [`Env`]'s clock.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Every source of nondeterminism the engine may use, and where it keeps write-ahead logs.
///
/// Production code uses [`SystemEnv`]. Deterministic simulation substitutes a virtual clock, a
/// seeded random source, an inline compute pool and logs in memory.
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

    /// Where write-ahead logs are kept; none by default, so a pipeline that needs one fails to
    /// plan.
    fn wal(&self) -> Option<Arc<dyn WalStore>> {
        None
    }

    /// A new load id from the current wall-clock time and 128 random bits.
    fn load_id(&self) -> LoadId {
        let random = (u128::from(self.random()) << 64) + u128::from(self.random());
        LoadId::from_parts(self.now(), random)
    }
}
