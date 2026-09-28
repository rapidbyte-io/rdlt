//! Killing what a host started, as a certification of crash recovery does: every connector
//! process spawned with a [`Kills`], abruptly, and every stream it [severs](Kills::sever), cut.

#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::network::Stream;
use crate::remote::severed::Severed;

/// Kills the connectors a host started with it, clones sharing one.
///
/// A process spawned with it is killed with `SIGKILL`, and a stream it severs is cut. A kill
/// reaches what was started before it; what starts after it lives on until the next.
#[derive(Clone, Debug, Default)]
pub struct Kills {
    inner: Arc<Mutex<Generation>>,
}

/// The kills so far, and what the next one cancels.
#[derive(Debug, Default)]
struct Generation {
    count: u64,
    next: CancellationToken,
}

impl Kills {
    /// A handle that has killed nothing yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Kills every process spawned with this handle, and cuts every stream it severed, that is
    /// alive now.
    pub fn kill(&self) {
        let mut generation = self.lock();
        generation.count += 1;
        std::mem::take(&mut generation.next).cancel();
    }

    /// How many kills there have been.
    pub fn count(&self) -> u64 {
        self.lock().count
    }

    /// `stream`, cut by the next kill.
    pub fn sever<S: Stream>(&self, stream: S) -> Box<dyn Stream> {
        Box::new(Severed::new(stream, self.next()))
    }

    /// What the next kill cancels.
    pub(crate) fn next(&self) -> CancellationToken {
        self.lock().next.clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Generation> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
