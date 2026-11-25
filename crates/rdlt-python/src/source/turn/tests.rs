use std::sync::Arc;
use std::time::Duration;

use super::Turns;

/// What `waited` comes to on a runtime of its own, on a paused clock.
fn ran<T>(waited: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
        .block_on(waited)
}

const SHORT: Duration = Duration::from_millis(50);

#[test]
fn a_partition_s_turn_is_taken_once_the_read_before_gives_it_back() {
    ran(async {
        let turns = Arc::new(Turns::default());
        let held = turns.taken("p", SHORT).await.expect("a turn no read holds");
        assert!(
            turns.taken("p", SHORT).await.is_err(),
            "a turn another read holds is refused once its wait ends"
        );
        let other = turns.taken("q", SHORT).await;
        assert!(other.is_ok(), "each partition has a turn of its own");
        drop(other);
        let waiting = tokio::spawn({
            let turns = Arc::clone(&turns);
            async move { turns.taken("p", Duration::from_secs(30)).await.is_ok() }
        });
        tokio::time::sleep(SHORT).await;
        drop(held);
        assert!(
            waiting.await.unwrap(),
            "the waiting read takes the turn given back"
        );
    });
}

#[test]
fn a_partition_no_read_holds_or_waits_for_is_forgotten() {
    ran(async {
        let turns = Arc::new(Turns::default());
        let first = turns.taken("p", SHORT).await.unwrap();
        let second = turns.taken("q", SHORT).await.unwrap();
        assert_eq!(turns.0.lock().len(), 2);
        drop(first);
        assert_eq!(turns.0.lock().keys().collect::<Vec<_>>(), ["q"]);
        drop(second);
        assert!(turns.0.lock().is_empty());
    });
}
