use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{Log, Refusals, Refused};

const EVERY: [Refused; 8] = [
    Refused::Accept,
    Refused::Displaced,
    Refused::Handshake,
    Refused::Slow,
    Refused::HostFull,
    Refused::QueueFull,
    Refused::Waited,
    Refused::Transport,
];

#[test]
fn nothing_refused_reports_nothing() {
    assert_eq!(Refusals::default().report(Duration::from_secs(10)), None);
}

#[test]
fn each_reason_is_counted_apart_and_forgotten_once_reported() {
    for (index, why) in EVERY.into_iter().enumerate() {
        let mut refusals = Refusals::default();
        for _ in 0..=index {
            refusals.count(why);
        }
        let line = refusals
            .report(Duration::from_secs(10))
            .expect("refusals to report");
        // The total, then the single reason counted: the same number twice, and no other.
        let numbers: Vec<u64> = line
            .split(|c: char| !c.is_ascii_digit())
            .filter_map(|number| number.parse().ok())
            .collect();
        let count = u64::try_from(index).expect("a small index") + 1;
        assert_eq!(numbers, [count, 10, count], "{line}");
        assert!(!line.contains('\n'), "{line}");
        assert_eq!(refusals, Refusals::default());
        assert_eq!(refusals.report(Duration::from_secs(10)), None);
    }
}

#[test]
fn every_reason_refused_is_one_line_with_their_total() {
    let mut refusals = Refusals::default();
    for why in EVERY {
        refusals.count(why);
        refusals.count(why);
    }
    let line = refusals
        .report(Duration::from_secs(3))
        .expect("refusals to report");
    let numbers: Vec<u64> = line
        .split(|c: char| !c.is_ascii_digit())
        .filter_map(|number| number.parse().ok())
        .collect();
    assert_eq!(numbers, [16, 3, 2, 2, 2, 2, 2, 2, 2, 2], "{line}");
    assert_eq!(line.lines().count(), 1);
}

#[test]
fn a_log_writes_each_line_where_it_was_told() {
    let lines = Arc::new(Mutex::new(Vec::new()));
    let written = Arc::clone(&lines);
    let log = Log::new(move |line| written.lock().expect("no panic").push(line.to_owned()));
    log.line("one");
    log.clone().line("two");
    assert_eq!(*lines.lock().expect("no panic"), ["one", "two"]);
    assert!(format!("{log:?}").starts_with("Log"));
    Log::stderr().line("a listening connector's line, written by a test");
}
