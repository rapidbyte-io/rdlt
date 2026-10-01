//! The connections a listening connector holds before they have authenticated: a set of
//! bounded size that closes one of its members, drawn at random, for each newcomer once full.
//!
//! A newcomer is never refused, so peers that connect and then say nothing delay no host: each
//! new connection takes the place of one of theirs, more likely than of the host's.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::hash::{BuildHasher as _, Hasher as _};
use std::pin::Pin;
use std::task::Poll;

/// The futures of the connections not yet authenticated, at most `limit` of them.
pub(super) struct Unauthenticated<F: ?Sized> {
    members: Vec<Pin<Box<F>>>,
    limit: usize,
    draws: Draws,
}

impl<F: Future + ?Sized> Unauthenticated<F> {
    /// An empty set of at most `limit` members, one at least, which draws whom it closes from
    /// `seed`.
    pub(super) fn new(limit: usize, seed: u64) -> Self {
        Self {
            members: Vec::new(),
            limit: limit.max(1),
            draws: Draws(seed),
        }
    }

    /// Admits `member`; where the set was full, first drops a member drawn at random, which
    /// closes its connection, and says so.
    pub(super) fn admit(&mut self, member: Pin<Box<F>>) -> Admitted {
        let admitted = if self.members.len() >= self.limit {
            let closed = self.draws.below(self.members.len());
            drop(self.members.swap_remove(closed));
            Admitted::InPlaceOfAnother
        } else {
            Admitted::Freely
        };
        self.members.push(member);
        admitted
    }

    /// What the next member to end ends with, as it leaves the set; pending while none has.
    pub(super) fn next(&mut self) -> impl Future<Output = F::Output> + '_ {
        std::future::poll_fn(|context| {
            for index in 0..self.members.len() {
                if let Poll::Ready(ended) = self.members[index].as_mut().poll(context) {
                    drop(self.members.swap_remove(index));
                    return Poll::Ready(ended);
                }
            }
            Poll::Pending
        })
    }

    /// How many members the set holds.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.members.len()
    }
}

/// How a member was admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Admitted {
    /// The set had room.
    Freely,
    /// The set was full: another member was closed for it.
    InPlaceOfAnother,
}

/// A seed no peer can predict, so none can tell which member a newcomer closes.
pub(super) fn seed() -> u64 {
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish()
}

/// Draws by `SplitMix64`.
struct Draws(u64);

impl Draws {
    /// A draw below `bound`, which is not zero.
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        let bound = u64::try_from(bound).unwrap_or(u64::MAX).max(1);
        usize::try_from(mixed % bound).unwrap_or(0)
    }
}
