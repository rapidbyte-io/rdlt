#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock directly"
)]

use std::cell::Cell;
use std::num::NonZeroUsize;
use std::time::Duration;

use super::in_order;

#[tokio::test(start_paused = true)]
async fn outputs_keep_the_futures_order_whatever_order_they_finish_in() {
    let waits = [3, 1, 2];
    let futures = waits
        .iter()
        .enumerate()
        .map(|(index, seconds)| async move {
            tokio::time::sleep(Duration::from_secs(*seconds)).await;
            index
        })
        .take(waits.len())
        .collect();
    let started = tokio::time::Instant::now();
    let outputs = in_order(futures, NonZeroUsize::new(3).unwrap()).await;
    assert_eq!(outputs, [0, 1, 2]);
    assert_eq!(started.elapsed(), Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn no_more_than_the_bound_run_at_once_and_each_starts_as_one_ends() {
    let (running, most) = (Cell::new(0_usize), Cell::new(0_usize));
    let futures = (0..5)
        .map(|index| {
            let (running, most) = (&running, &most);
            async move {
                running.set(running.get() + 1);
                most.set(most.get().max(running.get()));
                tokio::time::sleep(Duration::from_secs(1)).await;
                running.set(running.get() - 1);
                index
            }
        })
        .take(5)
        .collect();
    let started = tokio::time::Instant::now();
    let outputs = in_order(futures, NonZeroUsize::new(2).unwrap()).await;
    assert_eq!(outputs, [0, 1, 2, 3, 4]);
    assert_eq!(most.get(), 2);
    // Five one-second futures two at a time: three seconds.
    assert_eq!(started.elapsed(), Duration::from_secs(3));
}

#[tokio::test]
async fn no_futures_give_no_outputs() {
    let futures: Vec<std::future::Ready<u8>> = Vec::new();
    assert!(in_order(futures, NonZeroUsize::MIN).await.is_empty());
}
