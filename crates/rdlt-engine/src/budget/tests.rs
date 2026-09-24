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
async fn a_request_larger_than_the_budget_is_admitted_when_nothing_is_reserved() {
    let budget = MemoryBudget::new(100);
    let huge = budget.acquire(1_000).await;
    assert_eq!(budget.reserved(), 1_000);
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
