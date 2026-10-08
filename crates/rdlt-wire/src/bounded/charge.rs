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
