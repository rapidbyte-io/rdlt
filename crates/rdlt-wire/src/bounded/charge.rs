//! What decoding a message holds, charged before it is decoded to whoever holds the memory.
//!
//! A call made within [`charging`] charges each message of its answer, at what its scan counts
//! decoding it holds, before the message is passed on to be decoded, and holds the charge until
//! the message is decoded: until whoever reads the answer releases it, the next message is
//! passed on, or the answer ends.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tonic::Status;

use crate::aligned::Spare;
use crate::limits::Class;

/// What holds a charge until it is dropped.
pub type Held = Box<dyn Any + Send>;

/// A charge being taken.
pub type Charging = Pin<Box<dyn Future<Output = Result<Held, Status>> + Send>>;

/// Whoever holds the memory decoding takes.
pub trait Charge: Send + Sync {
    /// Waits until `bytes`, what decoding a message of `class` holds, may be held, and returns
    /// what holds them.
    ///
    /// # Errors
    ///
    /// A status the call fails with, where the bytes may not be held.
    fn charge(&self, class: Class, bytes: usize) -> Charging;

    /// Keeps `spare`, the allocation of a frame nothing holds any more, for a later frame to
    /// arrive in, where its bytes may be held charged without anyone waiting; frees it otherwise.
    fn keep_spare(&self, spare: Spare) {
        drop(spare);
    }

    /// The spare kept for a frame needing `room` bytes, where one [fits](Spare::fits) it: taken,
    /// it is charged no more.
    fn take_spare(&self, _room: usize) -> Option<Spare> {
        None
    }
}

tokio::task_local! {
    static CHARGE: Arc<dyn Charge>;
}

/// Runs `call`, whose calls' answers are charged to `charge` before they are decoded.
pub async fn charging<F: Future>(charge: Arc<dyn Charge>, call: F) -> F::Output {
    CHARGE.scope(charge, call).await
}

/// What the calls made now are charged to, where they are made within [`charging`].
pub fn current() -> Option<Arc<dyn Charge>> {
    CHARGE.try_with(Arc::clone).ok()
}
