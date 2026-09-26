#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use super::channel;

/// What `future` gives if it completes within a second of paused time.
async fn soon<F: Future>(future: F) -> Option<F::Output> {
    tokio::select! {
        biased;
        output = future => Some(output),
        () = tokio::time::sleep(Duration::from_secs(1)) => None,
    }
}

/// The order eight tasks waiting on one channel wake in when it changes.
fn woken() -> Vec<usize> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime builds");
    runtime.block_on(async {
        let (sender, receiver) = channel(0_u64);
        let order = Arc::new(Mutex::new(Vec::new()));
        let tasks: Vec<_> = (0..8)
            .map(|index| {
                let (mut receiver, order) = (receiver.clone(), Arc::clone(&order));
                tokio::spawn(async move {
                    receiver.changed().await.expect("the sender lives");
                    order.lock().push(index);
                })
            })
            .collect();
        // Every task starts waiting before the change.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        sender.send_replace(1);
        for task in tasks {
            task.await.expect("the task completes");
        }
        order.lock().clone()
    })
}

#[test]
fn waiters_wake_in_the_same_order_every_time() {
    let first = woken();
    assert_eq!(first.len(), 8);
    for _ in 0..20 {
        assert_eq!(woken(), first);
    }
}

#[tokio::test(start_paused = true)]
async fn a_change_sent_before_waiting_is_seen_once() {
    let (sender, mut receiver) = channel(0_u64);
    sender.send_replace(1);
    assert!(
        soon(receiver.changed())
            .await
            .is_some_and(|changed| changed.is_ok())
    );
    assert!(
        soon(receiver.changed()).await.is_none(),
        "the change was seen"
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_receiver_waits_for_the_next_change() {
    let (sender, _first) = channel(0_u64);
    sender.send_replace(1);
    let mut receiver = sender.subscribe();
    assert!(soon(receiver.changed()).await.is_none());
    sender.send_replace(2);
    assert!(
        soon(receiver.changed())
            .await
            .is_some_and(|changed| changed.is_ok())
    );
}

#[tokio::test(start_paused = true)]
async fn a_receiver_that_took_the_value_waits_for_the_next() {
    let (sender, mut receiver) = channel(0_u64);
    sender.send_replace(1);
    let _ = receiver.borrow_and_update();
    assert!(soon(receiver.changed()).await.is_none());
}

#[tokio::test(start_paused = true)]
async fn waiting_for_a_value_ends_once_it_holds() {
    let (sender, mut receiver) = channel(true);
    assert!(soon(receiver.wait_for(|value| *value)).await.is_some());
    sender.send_replace(false);
    assert!(soon(receiver.wait_for(|value| *value)).await.is_none());
    sender.send_replace(true);
    assert!(
        soon(receiver.wait_for(|value| *value))
            .await
            .is_some_and(|held| held.is_ok())
    );
}

#[tokio::test(start_paused = true)]
async fn receivers_learn_the_sender_is_gone() {
    let (sender, mut receiver) = channel(false);
    let mut other = receiver.clone();
    drop(sender);
    assert!(
        soon(receiver.changed())
            .await
            .is_some_and(|changed| changed.is_err())
    );
    assert!(
        soon(other.wait_for(|value| *value))
            .await
            .is_some_and(|held| held.is_err())
    );
}
