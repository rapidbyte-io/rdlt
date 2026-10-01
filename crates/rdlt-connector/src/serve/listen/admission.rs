//! The connections a listening connector holds before they have authenticated: a set of
//! bounded size that, once full, closes one of its members for each newcomer.
//!
//! A newcomer is never refused. The member closed is the oldest of the origin that holds the
//! most of them, the newcomer counted, so peers that connect and say nothing mostly close their
//! own: a host connecting from elsewhere keeps its place while any origin holds more than one.
//! Origins that hold as many are drawn among at random.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::future::Future;
use std::hash::{BuildHasher as _, Hasher as _};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::task::Poll;

/// Where a connection comes from, as far as a peer cannot choose it: its IPv4 address, or the
/// first 64 bits of its IPv6 address, which one network assigns as a whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Origin {
    V4([u8; 4]),
    V6([u8; 8]),
}

impl Origin {
    pub(super) fn of(peer: SocketAddr) -> Self {
        match peer.ip().to_canonical() {
            IpAddr::V4(address) => Self::V4(address.octets()),
            IpAddr::V6(address) => {
                let mut prefix = [0; 8];
                prefix.copy_from_slice(&address.octets()[..8]);
                Self::V6(prefix)
            }
        }
    }
}

/// A member: where it comes from, when it was admitted among the others, and its future.
struct Member<F: ?Sized> {
    origin: Origin,
    admitted: u64,
    future: Pin<Box<F>>,
}

/// The futures of the connections not yet authenticated, at most `limit` of them.
pub(super) struct Unauthenticated<F: ?Sized> {
    members: Vec<Member<F>>,
    limit: usize,
    admitted: u64,
    draws: Draws,
}

impl<F: Future + ?Sized> Unauthenticated<F> {
    /// An empty set of at most `limit` members, one at least, which draws between origins that
    /// hold as many from `seed`.
    pub(super) fn new(limit: usize, seed: u64) -> Self {
        Self {
            members: Vec::new(),
            limit: limit.max(1),
            admitted: 0,
            draws: Draws(seed),
        }
    }

    /// Admits `member`, which comes from `origin`; where the set was full, first drops a member,
    /// which closes its connection, and says so.
    pub(super) fn admit(&mut self, origin: Origin, member: Pin<Box<F>>) -> Admitted {
        let admitted = if self.members.len() >= self.limit {
            let closed = self.closed_for(origin);
            drop(self.members.swap_remove(closed));
            Admitted::InPlaceOfAnother
        } else {
            Admitted::Freely
        };
        self.admitted += 1;
        self.members.push(Member {
            origin,
            admitted: self.admitted,
            future: member,
        });
        admitted
    }

    /// The member to close for a newcomer from `newcomer`: the oldest of the origin that holds
    /// the most, the newcomer counted, drawn at random among origins that hold as many.
    fn closed_for(&mut self, newcomer: Origin) -> usize {
        let mut held: BTreeMap<Origin, usize> = BTreeMap::new();
        for member in &self.members {
            *held.entry(member.origin).or_default() += 1;
        }
        // The newcomer counts towards its origin's, where that origin has a member to close.
        if let Some(count) = held.get_mut(&newcomer) {
            *count += 1;
        }
        let most = held.values().copied().max().unwrap_or(0);
        let holding: Vec<Origin> = held
            .into_iter()
            .filter(|(_, count)| *count == most)
            .map(|(origin, _)| origin)
            .collect();
        let origin = holding[self.draws.below(holding.len())];
        let oldest = self
            .members
            .iter()
            .enumerate()
            .filter(|(_, member)| member.origin == origin)
            .min_by_key(|(_, member)| member.admitted);
        oldest.map_or(0, |(index, _)| index)
    }

    /// What the next member to end ends with, as it leaves the set; pending while none has.
    pub(super) fn next(&mut self) -> impl Future<Output = F::Output> + '_ {
        std::future::poll_fn(|context| {
            for index in 0..self.members.len() {
                if let Poll::Ready(ended) = self.members[index].future.as_mut().poll(context) {
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

/// A seed no peer can predict, so none can tell which origin a newcomer closes a member of.
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
