#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::time::Duration;

use super::MemoryBudget;

#[tokio::test]
async fn requests_that_fit_are_admitted_at_once() {
    let budget = MemoryBudget::new(100);
    let first = budget.acquire(60).await;
    let second = budget.acquire(40).await;
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
    let huge = budget.acquire(1_000).await;
    assert_eq!(budget.reserved(), 100);
    assert_eq!(budget.peak(), 100);
    drop(huge);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_request_waits_until_enough_is_released() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(70).await;
    let waiting = budget.acquire(50);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("50 bytes do not fit beside 70 of 100"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    drop(held);
    let admitted = waiting.await;
    assert_eq!(admitted.bytes(), 50);
    assert_eq!(budget.reserved(), 50);
}

#[tokio::test(start_paused = true)]
async fn waiting_requests_are_admitted_in_arrival_order() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(100).await;
    let order = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let large = {
        let (budget, order) = (budget.clone(), std::sync::Arc::clone(&order));
        async move {
            let reservation = budget.acquire(90).await;
            order.lock().push("large");
            reservation
        }
    };
    let small = {
        let (budget, order) = (budget.clone(), std::sync::Arc::clone(&order));
        async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let reservation = budget.acquire(5).await;
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
    let held = budget.acquire(100).await;
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
    let admitted = waiting.await;
    assert_eq!(budget.reserved(), 80);
    drop(admitted);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test]
async fn debug_output_shows_capacity_and_use() {
    let budget = MemoryBudget::new(10);
    let reservation = budget.acquire(3).await;
    assert!(format!("{budget:?}").contains("capacity: 10"));
    assert!(format!("{budget:?}").contains("reserved: 3"));
    assert_eq!(format!("{reservation:?}"), "Reservation(3)");
}

#[tokio::test(start_paused = true)]
async fn a_waiter_that_gave_up_never_counts_toward_the_peak() {
    let budget = MemoryBudget::new(100);
    let held = budget.acquire(40).await;
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
    let admitted = budget.acquire(80).await;
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
    let late = waiting.await;
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
    let held = budget.acquire(70).await;
    assert!(!felt(&budget).await, "nothing waits");
    let waiting = budget.acquire(50);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("50 bytes do not fit beside 70 of 100"),
        pressed = felt(&budget) => assert!(pressed, "a request waits"),
    }
    drop(held);
    let admitted = waiting.await;
    assert!(!felt(&budget).await, "the request was admitted");
    drop(admitted);
}

#[tokio::test(start_paused = true)]
async fn pressure_is_felt_while_charges_exceed_the_budget_and_eases_once_they_fit() {
    let budget = MemoryBudget::new(100);
    let admitted = budget.acquire(60).await;
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
    let cursor = budget.acquire_kept(10).await;
    // A request of the whole budget fits beside nothing in flight, whatever a commit holds.
    let whole = budget.acquire(1_000).await;
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
    let admitted = waiting.await;
    assert_eq!(budget.reserved(), 110);
    drop(cursor);
    assert_eq!(budget.reserved(), 100);
    drop(admitted);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn bytes_a_commit_releases_wait_for_room_like_any_request() {
    let budget = MemoryBudget::new(100);
    let first = budget.acquire_kept(60).await;
    let second = budget.acquire_kept(40).await;
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
    let third = waiting.await;
    assert_eq!(budget.reserved(), 41);
    drop((second, third));
    assert_eq!(budget.reserved(), 0);
    // One larger than the budget takes the whole budget once nothing is reserved.
    let large = budget.acquire_kept(500).await;
    assert_eq!(large.bytes(), 100);
}

#[tokio::test(start_paused = true)]
async fn bytes_a_commit_releases_press_no_one_to_flush() {
    let budget = MemoryBudget::new(100);
    let cursor = budget.acquire_kept(50).await;
    let admitted = budget.acquire(100).await;
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
    let admitted = budget.acquire(60).await;
    assert_eq!(budget.reserved(), 210);
    drop(dictionaries);
    assert_eq!(budget.reserved(), 60);
    drop(admitted);
    assert_eq!(budget.reserved(), 0);
}
