use std::collections::BTreeSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::oneshot;

use super::{Admitted, Draws, Origin, Unauthenticated, seed};

type Member = dyn Future<Output = usize> + Send;

/// A member that ends with `id` once `end` fires, and counts down `alive` when dropped.
fn member(id: usize, alive: &Arc<AtomicUsize>) -> (Pin<Box<Member>>, oneshot::Sender<()>) {
    struct Alive(Arc<AtomicUsize>);
    impl Drop for Alive {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    alive.fetch_add(1, Ordering::SeqCst);
    let alive = Alive(Arc::clone(alive));
    let (end, ended) = oneshot::channel();
    let member = Box::pin(async move {
        let _alive = alive;
        ended.await.ok();
        id
    });
    (member, end)
}

fn origin(address: &str) -> Origin {
    let peer: SocketAddr = address.parse().expect("an address and port");
    Origin::of(peer)
}

/// The origin of the `n`th of many IPv4 addresses.
fn nth(n: usize) -> Origin {
    let n = u16::try_from(n).expect("few origins");
    Origin::V4([10, 0, n.to_be_bytes()[0], n.to_be_bytes()[1]])
}

/// A set of `limit` members drawing from `seed`, admitting each of `origins` in turn: the ids,
/// in admission order, of the members still in it, found by ending every member.
async fn survivors(limit: usize, seed: u64, origins: &[Origin]) -> Vec<usize> {
    let alive = Arc::new(AtomicUsize::new(0));
    let mut set = Unauthenticated::<Member>::new(limit, seed);
    let limit = limit.max(1);
    let mut ends = Vec::new();
    for (id, origin) in origins.iter().enumerate() {
        let (member, end) = member(id, &alive);
        let expected = if id < limit {
            Admitted::Freely
        } else {
            Admitted::InPlaceOfAnother
        };
        assert_eq!(set.admit(*origin, member), expected);
        ends.push(end);
        assert_eq!(set.len(), (id + 1).min(limit));
        // The member closed is dropped at once: nothing of it is held.
        assert_eq!(alive.load(Ordering::SeqCst), set.len());
    }
    for end in ends {
        end.send(()).ok();
    }
    let mut survivors = Vec::new();
    for _ in 0..set.len() {
        survivors.push(set.next().await);
    }
    survivors.sort_unstable();
    assert_eq!((set.len(), alive.load(Ordering::SeqCst)), (0, 0));
    survivors
}

#[tokio::test]
async fn a_flood_from_one_origin_closes_its_own_and_never_another_origins() {
    let (flood, host) = (origin("192.0.2.7:1"), origin("198.51.100.1:9"));
    for seed in 0..32 {
        // The host connects into a set the flood has filled, and the flood goes on.
        let mut origins = vec![flood; 8];
        origins.push(host);
        origins.extend(vec![flood; 2000]);
        let kept = survivors(8, seed, &origins).await;
        assert!(kept.contains(&8), "seed {seed}: the host was closed");
        // The flood keeps its newest, its oldest closed first.
        assert_eq!(kept, [8, 2002, 2003, 2004, 2005, 2006, 2007, 2008]);
    }
}

#[tokio::test]
async fn the_origin_holding_the_most_loses_its_oldest_the_newcomer_counted() {
    let (a, b, c) = (nth(1), nth(2), nth(3));
    // Three of one and two of another: the newcomer of a third closes the first of the three.
    assert_eq!(survivors(5, 1, &[a, b, a, b, a, c]).await, [1, 2, 3, 4, 5]);
    // Two and two: a newcomer of either makes its own origin hold the most.
    for seed in 0..32 {
        assert_eq!(survivors(4, seed, &[a, b, a, b, b]).await, [0, 2, 3, 4]);
        assert_eq!(survivors(4, seed, &[a, b, a, b, a]).await, [1, 2, 3, 4]);
    }
}

#[tokio::test]
async fn origins_holding_as_many_are_drawn_among_and_the_newcomer_is_never_closed() {
    // Every origin holds one: any may lose its member, the newcomer's origin holding none to lose.
    let origins: Vec<Origin> = (0..5).map(nth).collect();
    let mut closed = BTreeSet::new();
    for seed in 0..256 {
        let kept = survivors(4, seed, &origins).await;
        assert!(kept.contains(&4), "seed {seed}: the newcomer was closed");
        closed.insert((0..4).find(|id| !kept.contains(id)).expect("one closed"));
    }
    assert_eq!(closed, BTreeSet::from([0, 1, 2, 3]));
    // Two origins hold two each and a third a single member: the third is never drawn.
    let (a, b, c, d) = (nth(1), nth(2), nth(3), nth(4));
    let mut closed = BTreeSet::new();
    for seed in 0..256 {
        let kept = survivors(5, seed, &[a, b, c, a, b, d]).await;
        closed.insert((0..5).find(|id| !kept.contains(id)).expect("one closed"));
    }
    assert_eq!(closed, BTreeSet::from([0, 1]));
}

#[test]
fn an_origin_is_an_ipv4_address_or_an_ipv6_network() {
    assert_eq!(origin("192.0.2.7:1"), origin("192.0.2.7:65535"));
    assert_ne!(origin("192.0.2.7:1"), origin("192.0.2.8:1"));
    // An IPv6 network is one origin, whatever addresses a peer takes within it.
    assert_eq!(
        origin("[2001:db8:1:2::1]:1"),
        origin("[2001:db8:1:2:ffff:ffff:ffff:ffff]:2")
    );
    assert_ne!(origin("[2001:db8:1:2::1]:1"), origin("[2001:db8:1:3::1]:1"));
    // An IPv4 address is itself, however it is written.
    assert_eq!(origin("[::ffff:192.0.2.7]:1"), origin("192.0.2.7:1"));
    assert_ne!(origin("[::1]:1"), origin("127.0.0.1:1"));
}

#[tokio::test(start_paused = true)]
async fn a_set_with_no_member_ended_is_pending_and_each_ended_member_leaves_once() {
    let alive = Arc::new(AtomicUsize::new(0));
    let mut set = Unauthenticated::<Member>::new(8, 1);
    let waited = tokio::time::timeout(std::time::Duration::from_secs(1), set.next()).await;
    assert!(waited.is_err(), "an empty set ended a member");
    let mut ends = Vec::new();
    for id in 0..3 {
        let (member, end) = member(id, &alive);
        set.admit(nth(id), member);
        ends.push(end);
    }
    let waited = tokio::time::timeout(std::time::Duration::from_secs(1), set.next()).await;
    assert!(waited.is_err(), "no member had ended");
    ends.remove(1).send(()).ok();
    assert_eq!(set.next().await, 1);
    assert_eq!((set.len(), alive.load(Ordering::SeqCst)), (2, 2));
    for end in ends {
        end.send(()).ok();
    }
    let rest = BTreeSet::from([set.next().await, set.next().await]);
    assert_eq!(rest, BTreeSet::from([0, 2]));
    assert_eq!((set.len(), alive.load(Ordering::SeqCst)), (0, 0));
}

#[tokio::test]
async fn a_set_holds_one_member_at_least() {
    assert_eq!(survivors(0, 3, &[nth(0), nth(1)]).await, [1]);
    assert_eq!(survivors(1, 3, &[nth(0), nth(0)]).await, [1]);
}

#[test]
fn draws_fall_below_their_bound_and_cover_it() {
    let mut draws = Draws(42);
    let mut seen = BTreeSet::new();
    for _ in 0..1000 {
        let draw = draws.below(7);
        assert!(draw < 7);
        seen.insert(draw);
    }
    assert_eq!(seen.len(), 7);
    assert_eq!(draws.below(1), 0);
}

#[test]
fn draws_are_splitmix64_s() {
    // SplitMix64's first outputs from a state of zero, below no bound but the widest.
    let mut source = Draws(0);
    let outputs: Vec<usize> = (0..3).map(|_| source.below(usize::MAX)).collect();
    let known = [
        0xe220_a839_7b1d_cdaf_u64,
        0x6e78_9e6a_a1b9_65f4,
        0x06c4_5d18_8009_454f,
    ]
    .map(|value| usize::try_from(value).expect("a 64-bit target"));
    assert_eq!(outputs, known);
}

#[test]
fn seeds_differ_between_listeners() {
    let seeds: BTreeSet<u64> = (0..8).map(|_| seed()).collect();
    assert!(seeds.len() > 1);
}
