use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::Sessions;
use crate::serve::live::CountedSession;

/// Bounds every wait of a test: an Open or a Close that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

fn session(live: &Arc<AtomicUsize>) -> Box<dyn crate::destination::DestinationSession> {
    CountedSession::new(live, Duration::ZERO)
}

/// Holds `count` sessions in `sessions`, with ids from 1, each counted in `live`.
async fn holding(sessions: &Sessions, count: u64, live: &Arc<AtomicUsize>) {
    for id in 1..=count {
        let opening = tokio::time::timeout(WAIT, sessions.open())
            .await
            .expect("there is room");
        assert_eq!(opening.hold(session(live)), id);
    }
}

/// Whether `sessions` refuses the session `id` as closed.
async fn is_closed(sessions: &Sessions, id: u64) -> bool {
    match sessions.session(id).await {
        Ok(_) => false,
        Err(refused) => refused.code() == Some("no_session"),
    }
}

/// Opens a session in a task of its own, counted in `live`, and answers its id.
fn opening(sessions: &Arc<Sessions>, live: &Arc<AtomicUsize>) -> tokio::task::JoinHandle<u64> {
    let (sessions, live) = (Arc::clone(sessions), Arc::clone(live));
    tokio::spawn(async move { sessions.open().await.hold(session(&live)) })
}

#[tokio::test(start_paused = true)]
async fn an_open_beyond_the_sessions_held_waits_for_the_call_on_the_oldest_then_closes_it() {
    let sessions = Arc::new(Sessions::holding(3));
    let live = Arc::new(AtomicUsize::new(0));
    holding(&sessions, 3, &live).await;
    // A call in flight on session 1 holds its slot.
    let slot = sessions.session(1).await.expect("session 1 is open");
    let call = slot.lock_owned().await;
    let fourth = opening(&sessions, &live);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !fourth.is_finished(),
        "the open waits for the call on the oldest"
    );
    assert_eq!(
        live.load(Ordering::SeqCst),
        3,
        "nothing is closed under a call"
    );
    drop(call);
    let id = tokio::time::timeout(WAIT, fourth)
        .await
        .expect("the open ends")
        .expect("the open does not panic");
    assert_eq!(id, 4);
    assert_eq!(
        live.load(Ordering::SeqCst),
        3,
        "session 1 is closed for session 4"
    );
    assert!(is_closed(&sessions, 1).await);
    for id in 2..=4 {
        assert!(!is_closed(&sessions, id).await, "session {id} is open");
    }
}

#[tokio::test(start_paused = true)]
async fn a_session_being_closed_holds_its_room_until_its_close_ends() {
    let sessions = Arc::new(Sessions::holding(1));
    let live = Arc::new(AtomicUsize::new(0));
    let opened = tokio::time::timeout(WAIT, sessions.open())
        .await
        .expect("there is room");
    let slow = CountedSession::new(&live, Duration::from_secs(10));
    assert_eq!(opened.hold(slow), 1);
    let closing = tokio::spawn({
        let sessions = Arc::clone(&sessions);
        async move { sessions.close(1).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let second = opening(&sessions, &live);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!second.is_finished(), "the open waits for the close");
    assert_eq!(live.load(Ordering::SeqCst), 1);
    tokio::time::timeout(WAIT, closing)
        .await
        .expect("the close ends")
        .expect("the close does not panic")
        .expect("the session closes");
    let id = tokio::time::timeout(WAIT, second)
        .await
        .expect("the open ends")
        .expect("the open does not panic");
    assert_eq!(id, 2);
    assert_eq!(live.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn an_open_that_fails_or_is_cancelled_takes_no_room_from_the_sessions_held() {
    let sessions = Sessions::holding(2);
    let live = Arc::new(AtomicUsize::new(0));
    holding(&sessions, 1, &live).await;
    // An open whose destination failed, or whose call was cancelled, drops its room.
    let failed = tokio::time::timeout(WAIT, sessions.open())
        .await
        .expect("there is room");
    drop(failed);
    let next = tokio::time::timeout(WAIT, sessions.open())
        .await
        .expect("the failed open left its room");
    assert_eq!(next.hold(session(&live)), 3);
    assert_eq!(live.load(Ordering::SeqCst), 2);
    assert!(!is_closed(&sessions, 1).await, "session 1 stays open");
}

#[tokio::test(start_paused = true)]
async fn two_opens_waiting_on_the_oldest_close_the_two_oldest() {
    let sessions = Arc::new(Sessions::holding(3));
    let live = Arc::new(AtomicUsize::new(0));
    holding(&sessions, 3, &live).await;
    let slot = sessions.session(1).await.expect("session 1 is open");
    let call = slot.lock_owned().await;
    let opens = [opening(&sessions, &live), opening(&sessions, &live)];
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(opens.iter().all(|open| !open.is_finished()));
    drop(call);
    let mut ids = Vec::new();
    for open in opens {
        let id = tokio::time::timeout(WAIT, open)
            .await
            .expect("the open ends")
            .expect("the open does not panic");
        ids.push(id);
    }
    ids.sort_unstable();
    assert_eq!(ids, [4, 5]);
    assert_eq!(live.load(Ordering::SeqCst), 3);
    for id in 1..=5 {
        assert_eq!(is_closed(&sessions, id).await, id <= 2, "session {id}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_open_cancelled_while_it_waits_for_the_oldest_closes_nothing() {
    let sessions = Sessions::holding(3);
    let live = Arc::new(AtomicUsize::new(0));
    holding(&sessions, 3, &live).await;
    let slot = sessions.session(1).await.expect("session 1 is open");
    let call = slot.lock_owned().await;
    let waited = tokio::time::timeout(Duration::from_secs(1), sessions.open()).await;
    assert!(waited.is_err(), "the open waits for the call");
    drop(call);
    assert_eq!(live.load(Ordering::SeqCst), 3);
    let slot = sessions.session(1).await.expect("session 1 stays open");
    assert!(slot.lock().await.is_some(), "session 1 holds its session");
}
