use rdlt::StopMode;

use super::Stops;

#[test]
fn a_first_signal_stops_after_the_next_commit_though_the_count_asked_first() {
    let mut stops = Stops::default();
    assert!(stops.counted(3, Some(3)));
    assert_eq!(stops.signalled(130), StopMode::AfterCommit);
    assert_eq!(stops.heard, Some(130));
}

#[test]
fn a_later_signal_stops_at_once_and_the_first_status_is_kept() {
    let mut stops = Stops::default();
    assert_eq!(stops.signalled(143), StopMode::AfterCommit);
    assert_eq!(stops.signalled(130), StopMode::Now);
    assert_eq!(stops.signalled(130), StopMode::Now);
    assert_eq!(stops.heard, Some(143));
}

#[test]
fn the_count_asks_once_and_never_after_a_signal() {
    let mut stops = Stops::default();
    assert!(!stops.counted(1, Some(3)));
    assert!(stops.counted(3, Some(3)));
    assert!(!stops.counted(3, Some(3)));
    let mut signalled = Stops::default();
    assert_eq!(signalled.signalled(130), StopMode::AfterCommit);
    assert!(!signalled.counted(3, Some(3)));
    // Without a count, no commit asks for the stop.
    assert!(!Stops::default().counted(3, None));
}
