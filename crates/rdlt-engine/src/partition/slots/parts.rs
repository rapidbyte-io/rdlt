//! The parts what reads keep is divided into: a read that is not followed holds one whole while it
//! reads, and followed unbounded reads draw on the rest together as they keep.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The parts of what reads keep, each the most one read keeps beside its events.
///
/// A read that is not followed holds a whole part while it reads, so it is refused nothing within
/// it. Followed unbounded reads hold none of their own: together they hold as many parts as what
/// they keep takes, one more as they keep more and fewer as they keep less, and never all of
/// them, so a read that is not followed always comes to hold one.
#[derive(Debug)]
pub(crate) struct Parts {
    free: Arc<Semaphore>,
    /// Bytes: a part.
    bytes: u64,
    /// The most parts followed reads hold together: all but one.
    most: usize,
    followed: Mutex<Followed>,
}

/// What followed reads keep together, and the parts they hold for it.
#[derive(Debug, Default)]
struct Followed {
    /// Bytes.
    kept: u64,
    held: Option<OwnedSemaphorePermit>,
}

/// A draw for followed reads that needed a part and found none they may take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Full {
    /// Bytes followed reads would have kept together.
    pub(crate) kept: u64,
    /// Bytes: what the parts they hold and those free they may take hold together.
    pub(crate) room: u64,
}

impl Parts {
    /// `count` parts of `bytes` each.
    pub(crate) fn new(count: NonZeroUsize, bytes: u64) -> Self {
        Self {
            free: Arc::new(Semaphore::new(count.get())),
            bytes,
            most: count.get() - 1,
            followed: Mutex::default(),
        }
    }

    /// A whole part for a read that is not followed, where one is free now.
    pub(crate) fn try_take(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.free).try_acquire_owned().ok()
    }

    /// A whole part for a read that is not followed, once one is free.
    pub(crate) async fn take(&self) -> OwnedSemaphorePermit {
        let part = Arc::clone(&self.free).acquire_owned().await;
        part.expect("the parts are never closed")
    }

    /// Draws `bytes` more for what followed reads keep together, taking another part where they
    /// need one.
    ///
    /// # Errors
    ///
    /// [`Full`], drawing nothing, where they need another part and would then hold all, or none is
    /// free.
    pub(crate) fn draw(&self, bytes: u64) -> Result<(), Full> {
        let mut followed = self.followed.lock();
        let kept = followed.kept.saturating_add(bytes);
        let held = followed
            .held
            .as_ref()
            .map_or(0, OwnedSemaphorePermit::num_permits);
        let needed = self.needed(kept);
        if let Some(more) = needed.checked_sub(held).and_then(NonZeroUsize::new) {
            let more = u32::try_from(more.get()).unwrap_or(u32::MAX);
            let taken = (needed <= self.most)
                .then(|| Arc::clone(&self.free).try_acquire_many_owned(more).ok())
                .flatten();
            let Some(taken) = taken else {
                let may = held
                    .saturating_add(self.free.available_permits())
                    .min(self.most);
                let room = u64::try_from(may).unwrap_or(u64::MAX);
                return Err(Full {
                    kept,
                    room: room.saturating_mul(self.bytes),
                });
            };
            match &mut followed.held {
                Some(held) => held.merge(taken),
                None => followed.held = Some(taken),
            }
        }
        followed.kept = kept;
        Ok(())
    }

    /// Gives back `bytes` followed reads kept, and the parts they no longer need.
    pub(crate) fn give_back(&self, bytes: u64) {
        let mut followed = self.followed.lock();
        debug_assert!(
            bytes <= followed.kept,
            "followed reads give back {bytes} bytes of the {} they keep",
            followed.kept
        );
        followed.kept = followed.kept.saturating_sub(bytes);
        let needed = self.needed(followed.kept);
        let surplus = followed.held.as_mut().and_then(|held| {
            let surplus = held.num_permits().saturating_sub(needed);
            held.split(surplus)
        });
        // The parts go back before the lock does, so a draw that sees the lower count finds them.
        drop(surplus);
        drop(followed);
    }

    /// Parts: what `kept` bytes take.
    fn needed(&self, kept: u64) -> usize {
        usize::try_from(kept.div_ceil(self.bytes.max(1))).unwrap_or(usize::MAX)
    }
}
