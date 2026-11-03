use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_wire::aligned::{ALIGNMENT, Aligned, Spare};

use super::{BUDGET, bounded, waits};
use crate::budget::{MemoryBudget, spare};

/// The bytes a spare of `room` bytes' room takes.
fn taking(room: usize) -> u64 {
    u64::try_from(spare(room).bytes()).unwrap()
}

/// Where `spare`'s allocation starts, and the spare, whole.
fn located(spare: Spare) -> (usize, Spare) {
    let kept = Arc::new(Mutex::new(None));
    let home = Arc::clone(&kept);
    let room = spare.bytes() - (ALIGNMENT - 1);
    let frozen = Aligned::in_spare(spare, room).freeze_returning(move |spare| {
        *home.lock() = Some(spare);
    });
    let start = frozen.allocation.as_ptr().addr();
    drop(frozen);
    let spare = kept.lock().take();
    (start, spare.expect("handed back"))
}

#[tokio::test]
async fn a_spare_is_reserved_of_what_pushes_may_take_until_a_frame_it_fits_takes_it() {
    let budget = bounded(BUDGET);
    budget.keep_spare(spare(1_000));
    assert_eq!(budget.reserved(), taking(1_000));
    assert_eq!(budget.peak(), taking(1_000), "reserved as any bytes are");
    assert!(budget.take_spare(499).is_none(), "less than half its room");
    assert!(budget.take_spare(1_001).is_none(), "more than its room");
    assert_eq!(budget.reserved(), taking(1_000));
    let taken = budget.take_spare(1_000).expect("it fits");
    assert_eq!(u64::try_from(taken.bytes()).unwrap(), taking(1_000));
    assert_eq!(
        budget.reserved(),
        0,
        "a frame holding it is charged as any frame"
    );
    assert!(budget.take_spare(1_000).is_none(), "taken once");
    // What pushes may take holds it beside them.
    let pushes = budget.acquire(1_000).await.unwrap();
    budget.keep_spare(spare(1_000));
    assert_eq!(budget.reserved(), 1_000 + taking(1_000));
    drop(pushes);
}

#[tokio::test(start_paused = true)]
async fn a_spare_is_kept_only_where_it_fits_and_no_request_for_the_data_waits() {
    let budget = bounded(BUDGET);
    // Pushes leave too little of their share.
    let pushes = budget.acquire(2_500 - taking(1_000) + 1).await.unwrap();
    budget.keep_spare(spare(1_000));
    assert!(budget.take_spare(1_000).is_none(), "it did not fit");
    drop(pushes);
    // Room enough for it, but a request for lowering waits.
    let held = (
        budget.acquire(2_400).await.unwrap(),
        budget.acquire_working(1_600).await.unwrap(),
    );
    let piece = budget.acquire_working(200);
    tokio::pin!(piece);
    assert!(waits(&mut piece).await);
    budget.keep_spare(spare(30));
    assert!(
        budget.take_spare(30).is_none(),
        "kept while a request waited"
    );
    drop(held);
    drop(piece.await.unwrap());
    budget.keep_spare(spare(30));
    assert!(budget.take_spare(30).is_some(), "kept once nothing waits");
}

#[tokio::test]
async fn a_larger_spare_takes_a_smaller_one_s_place_and_a_smaller_is_freed() {
    let budget = bounded(BUDGET);
    budget.keep_spare(spare(100));
    budget.keep_spare(spare(1_000));
    assert_eq!(budget.reserved(), taking(1_000));
    budget.keep_spare(spare(600));
    assert_eq!(budget.reserved(), taking(1_000), "the larger stays");
    assert!(budget.take_spare(100).is_none(), "the smallest is gone");
    let (kept, larger) = located(budget.take_spare(1_000).expect("the larger"));
    budget.keep_spare(larger);
    budget.keep_spare(spare(1_000));
    let (taken, _) = located(budget.take_spare(1_000).expect("one of them"));
    assert_eq!(taken, kept, "one as large leaves the kept one in place");
    // One the share holds room for only in place of the smaller takes its place.
    budget.keep_spare(spare(100));
    let pushes = budget.acquire(2_500 - taking(1_000)).await.unwrap();
    budget.keep_spare(spare(1_000));
    assert_eq!(
        budget.reserved(),
        2_500,
        "the larger took the smaller's place"
    );
    drop(pushes);
    assert!(budget.take_spare(1_000).is_some());
    // One the share holds no room for even in place of the smaller: the smaller is freed first.
    budget.keep_spare(spare(100));
    let pushes = budget.acquire(2_500 - taking(1_000) + 1).await.unwrap();
    budget.keep_spare(spare(1_000));
    assert_eq!(budget.reserved(), 2_500 - taking(1_000) + 1);
    assert!(budget.take_spare(100).is_none() && budget.take_spare(1_000).is_none());
    drop(pushes);
}

/// The requests for the data: a push, a frame waiting to be decoded and lowering, each of
/// `bytes`.
async fn asked(budget: &MemoryBudget, kind: usize, bytes: u64) -> Option<super::Reservation> {
    match kind {
        0 => budget.acquire(bytes).await.ok(),
        1 => budget.acquire_decoding(bytes).await.ok(),
        2 => budget.acquire_working(bytes).await.ok(),
        _ => budget.try_acquire_working(bytes),
    }
}

#[tokio::test(start_paused = true)]
async fn a_request_for_the_data_takes_the_spare_s_room_before_it_would_wait() {
    for kind in 0..4 {
        let budget = bounded(BUDGET);
        budget.keep_spare(spare(1_000));
        // The data is full but for the spare's room.
        let held = (
            budget.acquire(2_500 - taking(1_000)).await.unwrap(),
            budget.acquire_working(1_600).await.unwrap(),
        );
        let started = tokio::time::Instant::now();
        let request = asked(&budget, kind, 1_000).await.expect("admitted");
        assert_eq!(started.elapsed(), Duration::ZERO, "{kind}: it did not wait");
        assert!(budget.take_spare(1_000).is_none(), "{kind}: the spare went");
        assert_eq!(budget.reserved(), 4_100 - taking(1_000) + 1_000, "{kind}");
        drop((request, held));
        assert_eq!(budget.reserved(), 0, "{kind}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_request_for_the_data_that_fits_beside_the_spare_leaves_it() {
    for kind in 0..4 {
        let budget = bounded(BUDGET);
        budget.keep_spare(spare(1_000));
        let request = asked(&budget, kind, 1_000).await.expect("admitted");
        assert!(
            budget.take_spare(1_000).is_some(),
            "{kind}: the spare stays"
        );
        drop(request);
    }
}

#[tokio::test(start_paused = true)]
async fn a_request_of_another_share_leaves_the_spare() {
    let budget = bounded(BUDGET);
    budget.keep_spare(spare(1_000));
    let answers = budget.acquire_control(400).await.unwrap();
    let another = budget.acquire_control(1);
    tokio::pin!(another);
    assert!(waits(&mut another).await, "the share of answers is full");
    assert!(
        budget.take_spare(1_000).is_some(),
        "the spare holds no answer's room"
    );
    drop(answers);
}
