#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::time::Duration;

use super::{Exhausted, MemoryBudget};

#[tokio::test]
async fn requests_that_fit_are_admitted_at_once() {
    let budget = MemoryBudget::new(100);
    let first = budget.acquire(60).await.unwrap();
    let second = budget.acquire(40).await.unwrap();
    assert_eq!((first.bytes(), second.bytes()), (60, 40));
    assert_eq!(budget.reserved(), 100);
    drop(first);
    assert_eq!(budget.reserved(), 40);
    drop(second);
    assert_eq!(budget.reserved(), 0);
    assert_eq!(budget.peak(), 100);
}

#[tokio::test]
async fn a_request_larger_than_the_budget_takes_all_of_it_when_nothing_is_reserved() {
    // The engine works through such a request a slice at a time, within the budget.
    let budget = MemoryBudget::new(100);
    let huge = budget.acquire(1_000).await.unwrap();
    assert_eq!(budget.reserved(), 100);
    assert_eq!(budget.peak(), 100);
    drop(huge);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_request_waits_until_enough_is_released() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(70).await.unwrap();
    let waiting = budget.acquire(50);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("50 bytes do not fit beside 70 of 100"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    drop(held);
    let admitted = waiting.await.unwrap();
    assert_eq!(admitted.bytes(), 50);
    assert_eq!(budget.reserved(), 50);
}

#[tokio::test(start_paused = true)]
async fn waiting_requests_are_admitted_in_arrival_order() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(100).await.unwrap();
    let order = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let large = {
        let (budget, order) = (budget.clone(), std::sync::Arc::clone(&order));
        async move {
            let reservation = budget.acquire(90).await.unwrap();
            order.lock().push("large");
            reservation
        }
    };
    let small = {
        let (budget, order) = (budget.clone(), std::sync::Arc::clone(&order));
        async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let reservation = budget.acquire(5).await.unwrap();
            order.lock().push("small");
            reservation
        }
    };
    let release = async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(held);
    };
    let (large, small, ()) = tokio::join!(large, small, release);
    assert_eq!(*order.lock(), ["large", "small"]);
    assert_eq!(budget.reserved(), large.bytes() + small.bytes());
}

#[tokio::test(start_paused = true)]
async fn an_abandoned_request_releases_what_it_was_granted() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(100).await.unwrap();
    {
        let abandoned = budget.acquire(60);
        tokio::pin!(abandoned);
        tokio::select! {
            biased;
            _ = &mut abandoned => panic!("nothing fits yet"),
            () = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
    }
    let waiting = budget.acquire(80);
    drop(held);
    let admitted = waiting.await.unwrap();
    assert_eq!(budget.reserved(), 80);
    drop(admitted);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test]
async fn debug_output_shows_capacity_and_use() {
    let budget = MemoryBudget::new(10);
    let reservation = budget.acquire(3).await.unwrap();
    assert!(format!("{budget:?}").contains("capacity: 10"));
    assert!(format!("{budget:?}").contains("reserved: 3"));
    assert_eq!(format!("{reservation:?}"), "Reservation(3)");
}

#[tokio::test(start_paused = true)]
async fn a_waiter_that_gave_up_never_counts_toward_the_peak() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(40).await.unwrap();
    {
        let abandoned = budget.acquire(70);
        tokio::pin!(abandoned);
        tokio::select! {
            biased;
            _ = &mut abandoned => panic!("70 bytes do not fit beside 40 of 100"),
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
    drop(held);
    assert_eq!(budget.reserved(), 0);
    assert_eq!(budget.peak(), 40);
}

#[tokio::test(start_paused = true)]
async fn growth_is_charged_at_once_and_later_requests_wait_for_it() {
    let budget = MemoryBudget::new(100);
    let admitted = budget.acquire(80).await.unwrap();
    let growth = budget.charge(50);
    assert_eq!((growth.bytes(), budget.reserved()), (50, 130));
    assert_eq!(budget.peak(), 130);
    let waiting = budget.acquire(30);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("30 bytes do not fit beside 130 of 100"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    drop(growth);
    drop(admitted);
    let late = waiting.await.unwrap();
    assert_eq!(late.bytes(), 30);
    assert_eq!(budget.reserved(), 30);
}

/// Whether `budget` signals pressure within a second.
async fn felt(budget: &MemoryBudget) -> bool {
    tokio::select! {
        biased;
        () = budget.pressed() => true,
        () = tokio::time::sleep(Duration::from_secs(1)) => false,
    }
}

#[tokio::test(start_paused = true)]
async fn pressure_is_felt_while_a_request_waits_and_eases_once_it_is_admitted() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(70).await.unwrap();
    assert!(!felt(&budget).await, "nothing waits");
    let waiting = budget.acquire(50);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("50 bytes do not fit beside 70 of 100"),
        pressed = felt(&budget) => assert!(pressed, "a request waits"),
    }
    drop(held);
    let admitted = waiting.await.unwrap();
    assert!(!felt(&budget).await, "the request was admitted");
    drop(admitted);
}

#[tokio::test(start_paused = true)]
async fn pressure_is_felt_while_charges_exceed_the_budget_and_eases_once_they_fit() {
    let budget = MemoryBudget::new(100);
    let admitted = budget.acquire(60).await.unwrap();
    let growth = budget.charge(40);
    assert!(!felt(&budget).await, "the budget is full, not exceeded");
    let beyond = budget.charge(1);
    assert!(felt(&budget).await, "charges exceed the budget");
    drop(growth);
    assert!(!felt(&budget).await, "the charges fit again");
    drop(beyond);
    drop(admitted);
}

#[tokio::test(start_paused = true)]
async fn bytes_a_commit_releases_never_keep_a_request_larger_than_the_budget_out() {
    let budget = MemoryBudget::new(100);
    let cursor = budget.acquire_kept(10).await.unwrap();
    // A request of the whole budget fits beside nothing in flight, whatever a commit holds.
    let whole = budget.acquire(1_000).await.unwrap();
    assert_eq!((whole.bytes(), budget.reserved()), (100, 110));
    // Beside bytes in flight it waits, as beside any others.
    let waiting = budget.acquire(100);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("a request does not fit beside a full budget in flight"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    drop(whole);
    let admitted = waiting.await.unwrap();
    assert_eq!(budget.reserved(), 110);
    drop(cursor);
    assert_eq!(budget.reserved(), 100);
    drop(admitted);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn bytes_a_commit_releases_wait_for_room_like_any_request() {
    let budget = MemoryBudget::new(100);
    let first = budget.acquire_kept(60).await.unwrap();
    let second = budget.acquire_kept(40).await.unwrap();
    assert_eq!(budget.reserved(), 100);
    // The budget is full of what only a commit releases: more of it waits for one.
    let waiting = budget.acquire_kept(1);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("kept bytes never exceed the budget"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    drop(first);
    let third = waiting.await.unwrap();
    assert_eq!(budget.reserved(), 41);
    drop((second, third));
    assert_eq!(budget.reserved(), 0);
    // One larger than the budget takes the whole budget once nothing is reserved.
    let large = budget.acquire_kept(500).await.unwrap();
    assert_eq!(large.bytes(), 100);
}

#[tokio::test(start_paused = true)]
async fn bytes_a_commit_releases_press_no_one_to_flush() {
    let budget = MemoryBudget::new(100);
    let cursor = budget.acquire_kept(50).await.unwrap();
    let admitted = budget.acquire(100).await.unwrap();
    assert_eq!(budget.reserved(), 150);
    assert!(!felt(&budget).await, "what is in flight fits the budget");
    let growth = budget.charge(1);
    assert!(felt(&budget).await, "what is in flight exceeds the budget");
    drop(growth);
    drop(admitted);
    drop(cursor);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn what_a_read_keeps_is_charged_at_once_and_keeps_no_request_out() {
    let budget = MemoryBudget::new(100);
    let dictionaries = budget.keep(150);
    assert_eq!((dictionaries.bytes(), budget.reserved()), (150, 150));
    assert_eq!(budget.peak(), 150);
    assert!(!felt(&budget).await, "no write releases what a read keeps");
    // Nothing is in flight, so a request is admitted beside it.
    let admitted = budget.acquire(60).await.unwrap();
    assert_eq!(budget.reserved(), 210);
    drop(dictionaries);
    assert_eq!(budget.reserved(), 60);
    drop(admitted);
    assert_eq!(budget.reserved(), 0);
}

/// A budget of `capacity` whose requests wait an hour at most.
fn bounded(capacity: u64) -> MemoryBudget {
    let pool = crate::compute::RayonPool::new(std::num::NonZeroUsize::MIN).unwrap();
    let env = std::sync::Arc::new(crate::env::SystemEnv::new(pool));
    MemoryBudget::new(capacity).within(env, HOUR)
}

const HOUR: Duration = Duration::from_secs(3600);

#[tokio::test(start_paused = true)]
async fn a_request_waits_no_longer_than_the_deadline_and_says_what_held_the_budget() {
    let budget = bounded(100);
    let dictionaries = budget.keep(70);
    let cursor = budget.acquire_kept(20).await.unwrap();
    let flight = budget.acquire(10).await.unwrap();
    let started = tokio::time::Instant::now();
    let exhausted = budget.acquire_kept(30).await.unwrap_err();
    assert_eq!(started.elapsed(), HOUR);
    assert_eq!(
        exhausted,
        Exhausted {
            asked: 30,
            capacity: 100,
            in_flight: 10,
            commit: 20,
            read: 70,
            waited: HOUR,
        }
    );
    let said = exhausted.to_string();
    assert!(said.contains("70 are kept by reads"), "{said}");
    // The request left the queue: nothing was reserved for it, and nothing waits behind it.
    assert_eq!(budget.reserved(), 100);
    drop(flight);
    let next = budget.acquire(10).await.unwrap();
    drop((dictionaries, cursor, next));
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_request_admitted_before_the_deadline_is_not_failed_by_it() {
    let budget = bounded(100);
    let held = budget.acquire(100).await.unwrap();
    let release = async {
        tokio::time::sleep(Duration::from_secs(3_599)).await;
        drop(held);
    };
    let (admitted, ()) = tokio::join!(budget.acquire(100), release);
    assert_eq!(admitted.unwrap().bytes(), 100);
}

#[tokio::test(start_paused = true)]
async fn a_request_nobody_waits_for_leaves_the_queue_at_once() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(80).await.unwrap();
    {
        let abandoned = budget.acquire(60);
        tokio::pin!(abandoned);
        tokio::select! {
            biased;
            _ = &mut abandoned => panic!("60 bytes do not fit beside 80 of 100"),
            pressed = felt(&budget) => assert!(pressed, "a request waits"),
        }
    }
    // Nothing waits any more: no one is pressed, and a request that fits is not behind it.
    assert!(!felt(&budget).await);
    let admitted = tokio::time::timeout(Duration::from_secs(1), budget.acquire(20)).await;
    let admitted = admitted.unwrap().unwrap();
    assert_eq!((admitted.bytes(), budget.reserved()), (20, 100));
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn work_begun_waits_only_for_bytes_a_write_releases() {
    let budget = MemoryBudget::new(100);
    // A unit beyond the budget holds all of it until its last piece is written.
    let unit = budget.acquire(1_000).await.unwrap();
    let plain = budget.acquire(50);
    tokio::pin!(plain);
    tokio::select! {
        biased;
        _ = &mut plain => panic!("a request waits for the unit's bytes"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    // A piece of the unit waits for no bytes a unit holds, and goes before the request.
    let mut first = budget.acquire_working(50, false).await.unwrap();
    assert_eq!(budget.reserved(), 150);
    // Another unit's piece is not waited for either while it is lowered: neither is released
    // before its work is done.
    let other = budget.try_acquire_working(10, false).unwrap();
    assert_eq!(budget.reserved(), 160);
    drop(other);
    // Queued for a write, the piece is waited for: a flush releases it whatever else waits.
    first.stage();
    assert!(budget.try_acquire_working(50, false).is_none());
    let second = budget.acquire_working(50, false);
    tokio::pin!(second);
    tokio::select! {
        biased;
        _ = &mut second => panic!("two pieces beyond the budget are not held at once"),
        pressed = felt(&budget) => assert!(pressed, "the piece waits for bytes in flight"),
    }
    drop(first);
    let second = second.await.unwrap();
    assert_eq!((budget.reserved(), budget.peak()), (150, 160));
    drop((second, unit));
    assert_eq!(plain.await.unwrap().bytes(), 50);
}

#[tokio::test(start_paused = true)]
async fn a_piece_that_fits_is_admitted_whatever_is_queued_for_a_write() {
    let budget = MemoryBudget::new(100);
    let mut staged = budget.acquire(40).await.unwrap();
    staged.stage();
    let push = budget.acquire(70);
    tokio::pin!(push);
    tokio::select! {
        biased;
        _ = &mut push => panic!("70 bytes do not fit beside 40 of 100"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    // A piece that fits goes before the push that waits.
    let piece = budget.try_acquire_working(60, false).unwrap();
    assert_eq!(budget.reserved(), 100);
    assert!(budget.try_acquire_working(1, false).is_none());
    drop((staged, piece));
    assert_eq!(push.await.unwrap().bytes(), 70);
}

#[tokio::test(start_paused = true)]
async fn rows_beyond_a_piece_are_held_beyond_the_budget_one_at_a_time() {
    let budget = MemoryBudget::new(100);
    let unit = budget.acquire(80).await.unwrap();
    // A row of most of the budget does not fit beside its unit: it is reserved all the same.
    let first = budget.acquire_working(70, true).await.unwrap();
    assert_eq!(budget.reserved(), 150);
    // An ordinary piece beyond the budget is too, while nothing is queued for a write.
    let piece = budget.try_acquire_working(30, false).unwrap();
    // A second such row waits for the first to be written, queued or not.
    assert!(budget.try_acquire_working(70, true).is_none());
    let second = budget.acquire_working(70, true);
    tokio::pin!(second);
    tokio::select! {
        biased;
        _ = &mut second => panic!("two rows beyond a piece are not held at once"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    drop(first);
    let second = second.await.unwrap();
    assert_eq!(budget.reserved(), 180);
    // One that fits needs no turn.
    drop((unit, piece));
    let fitting = budget.try_acquire_working(30, true).unwrap();
    assert_eq!(budget.reserved(), 100);
    drop((second, fitting));
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_request_waiting_for_bytes_no_write_releases_presses_no_one() {
    let budget = MemoryBudget::new(100);
    let dictionaries = budget.keep(100);
    let cursor = budget.acquire_kept(10);
    tokio::pin!(cursor);
    tokio::select! {
        biased;
        _ = &mut cursor => panic!("a cursor does not fit beside a budget reads keep"),
        pressed = felt(&budget) => assert!(!pressed, "no write releases what the cursor waits for"),
    }
    drop(dictionaries);
    assert_eq!(cursor.await.unwrap().bytes(), 10);
}

#[tokio::test(start_paused = true)]
async fn a_reservation_resized_releases_or_charges_the_difference_at_once() {
    let budget = MemoryBudget::new(100);
    let mut piece = budget.acquire(80).await.unwrap();
    let waiting = budget.acquire(60);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("60 bytes do not fit beside 80 of 100"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    piece.resize(30);
    let admitted = waiting.await.unwrap();
    assert_eq!(admitted.bytes(), 60);
    assert_eq!((budget.reserved(), budget.peak()), (90, 90));
    piece.resize(70);
    assert_eq!(piece.bytes(), 70);
    assert_eq!((budget.reserved(), budget.peak()), (130, 130));
    drop((piece, admitted));
    assert_eq!(budget.reserved(), 0);
}
