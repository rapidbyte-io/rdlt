use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::oneshot;

use super::{Admitted, Draws, Unauthenticated, seed};

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

#[test]
fn a_full_set_closes_one_member_for_each_newcomer_and_never_grows() {
    let alive = Arc::new(AtomicUsize::new(0));
    let mut set = Unauthenticated::<Member>::new(4, 7);
    let mut ends = Vec::new();
    for id in 0..4 {
        let (member, end) = member(id, &alive);
        assert_eq!(set.admit(member), Admitted::Freely);
        ends.push(end);
    }
    for id in 4..1000 {
        let (member, end) = member(id, &alive);
        assert_eq!(set.admit(member), Admitted::InPlaceOfAnother);
        ends.push(end);
        assert_eq!(set.len(), 4);
        // The member closed is dropped at once: nothing of it is held.
        assert_eq!(alive.load(Ordering::SeqCst), 4);
    }
}

#[tokio::test]
async fn the_newcomer_is_never_the_member_closed() {
    let alive = Arc::new(AtomicUsize::new(0));
    for seed in 0..64 {
        let mut set = Unauthenticated::<Member>::new(3, seed);
        let mut ends = Vec::new();
        for id in 0..10 {
            let (member, end) = member(id, &alive);
            set.admit(member);
            ends.push(end);
        }
        // The last admitted is a member still: it ends with its id.
        ends.pop().expect("ten members").send(()).ok();
        assert_eq!(set.next().await, 9, "seed {seed}");
        assert_eq!(set.len(), 2);
    }
}

#[tokio::test]
async fn every_member_may_be_the_one_closed() {
    let alive = Arc::new(AtomicUsize::new(0));
    let mut survivors = BTreeSet::new();
    for seed in 0..256 {
        let mut set = Unauthenticated::<Member>::new(4, seed);
        let mut ends = Vec::new();
        for id in 0..5 {
            let (member, end) = member(id, &alive);
            set.admit(member);
            ends.push(end);
        }
        // Whichever of the first four was closed ends no more; the others end in turn.
        let mut survived = BTreeSet::new();
        for end in ends {
            end.send(()).ok();
        }
        for _ in 0..4 {
            survived.insert(set.next().await);
        }
        let closed = (0..4)
            .find(|id| !survived.contains(id))
            .expect("one closed");
        survivors.insert(closed);
    }
    assert_eq!(survivors, BTreeSet::from([0, 1, 2, 3]));
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
        set.admit(member);
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

#[test]
fn a_set_holds_one_member_at_least() {
    let alive = Arc::new(AtomicUsize::new(0));
    let mut set = Unauthenticated::<Member>::new(0, 3);
    let (first, _end) = member(0, &alive);
    assert_eq!(set.admit(first), Admitted::Freely);
    let (second, _end) = member(1, &alive);
    assert_eq!(set.admit(second), Admitted::InPlaceOfAnother);
    assert_eq!(set.len(), 1);
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
fn seeds_differ_between_listeners() {
    let seeds: BTreeSet<u64> = (0..8).map(|_| seed()).collect();
    assert!(seeds.len() > 1);
}
