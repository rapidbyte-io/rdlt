use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use super::Ended;

#[tokio::test(start_paused = true)]
async fn waiters_wake_in_the_order_they_began_to_wait_and_a_value_is_set_once() {
    let ended = Arc::new(Ended::<u32>::default());
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut waiting = Vec::new();
    for waiter in 0..8 {
        let (ended, order) = (Arc::clone(&ended), Arc::clone(&order));
        waiting.push(tokio::spawn(async move {
            let value = ended.wait().await;
            order.lock().push((waiter, value));
        }));
        tokio::task::yield_now().await;
    }
    ended.set(7);
    ended.set(9);
    for waiter in waiting {
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("each waiter wakes")
            .expect("it ends");
    }
    let woken: Vec<(u32, u32)> = (0..8).map(|waiter| (waiter, 7)).collect();
    assert_eq!(*order.lock(), woken);
    assert_eq!(ended.get(), Some(7));
}
