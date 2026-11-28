//! Each partition's turn to be read on one connection: a Python read the engine stopped or let go
//! runs on until its generator next yields or returns, so the partition's next read on the same
//! connection waits for it to return, as a Rust read's future, once dropped, runs no more.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::OwnedMutexGuard;
use tokio::time::error::Elapsed;

/// How long a partition's read waits for the last read of the partition on its connection to
/// return, as long as the engine waits, unless told otherwise, for a read it stopped to end:
/// past it, the generator ignores its stop, and the read fails.
pub(crate) const EARLIER_READ_WAIT: Duration = Duration::from_secs(60);

/// The turns of one stream's partitions on one connection, by partition.
#[derive(Default)]
pub(crate) struct Turns(Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>);

impl Turns {
    /// `partition`'s turn, once the read that holds it has returned, within `wait`.
    ///
    /// # Errors
    ///
    /// [`Elapsed`] where the read before has not returned within `wait`.
    pub(crate) async fn taken(
        self: &Arc<Self>,
        partition: &str,
        wait: Duration,
    ) -> Result<Turn, Elapsed> {
        let held = Arc::clone(self.0.lock().entry(partition.to_owned()).or_default());
        let guard = tokio::time::timeout(wait, held.lock_owned()).await?;
        Ok(Turn {
            turns: Arc::clone(self),
            partition: partition.to_owned(),
            guard: Some(guard),
        })
    }
}

/// A partition's turn to be read, given back as it drops; the partition is forgotten once no read
/// holds or waits for its turn.
pub(crate) struct Turn {
    turns: Arc<Turns>,
    partition: String,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for Turn {
    fn drop(&mut self) {
        let mut held = self.turns.0.lock();
        drop(self.guard.take());
        if held
            .get(&self.partition)
            .is_some_and(|turn| Arc::strong_count(turn) == 1)
        {
            held.remove(&self.partition);
        }
    }
}
