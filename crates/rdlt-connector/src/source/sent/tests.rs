use bytes::Bytes;

use super::{POSITION_UNSENT, Sent};
use crate::cursor::Cursor;
use crate::error::ConnectorErrorKind;
use crate::id::{PartitionId, StreamName};

fn stream(name: &str) -> StreamName {
    StreamName::new(name).expect("a valid stream name")
}

fn partition(id: &str) -> PartitionId {
    PartitionId::parse(id).expect("a valid partition id")
}

fn cursor(version: u16, bytes: &'static [u8]) -> Cursor {
    Cursor::new(version, Bytes::from_static(bytes)).expect("a cursor within its limit")
}

#[test]
fn a_position_is_its_hosts_its_streams_its_partitions_and_its_formats() {
    let sent = Sent::default();
    let (orders, p0, at) = (stream("orders"), partition("p0"), cursor(1, b"10"));
    assert!(!sent.knows(Some("host"), &orders, &p0, &at));
    sent.note(Some("host"), &orders, &p0, &at);
    assert!(sent.knows(Some("host"), &orders, &p0, &at));
    assert!(!sent.knows(Some("other"), &orders, &p0, &at));
    assert!(!sent.knows(None, &orders, &p0, &at));
    assert!(!sent.knows(Some("host"), &orders, &partition("p1"), &at));
    assert!(!sent.knows(Some("host"), &stream("users"), &p0, &at));
    let namespaced = StreamName::with_namespace("shop", "orders").expect("a valid stream name");
    assert!(!sent.knows(Some("host"), &namespaced, &p0, &at));
    assert!(!sent.knows(Some("host"), &orders, &p0, &cursor(2, b"10")));
    assert!(!sent.knows(Some("host"), &orders, &p0, &cursor(1, b"11")));
    assert!(!sent.knows(Some("host"), &orders, &p0, &cursor(1, b"")));
    // What a spawned connector's host was sent is that host's, which has no name.
    sent.note(None, &orders, &p0, &at);
    assert!(sent.knows(None, &orders, &p0, &at));
}

#[test]
fn a_report_is_admitted_whole_or_refused_whole_as_transient() {
    let sent = Sent::default();
    let (orders, p0, p1) = (stream("orders"), partition("p0"), partition("p1"));
    let (first, second) = (cursor(1, b"1"), cursor(1, b"2"));
    sent.note(None, &orders, &p0, &first);
    sent.note(None, &orders, &p1, &second);
    let known = [(p0.clone(), first.clone()), (p1.clone(), second.clone())];
    assert!(sent.admit(None, &orders, &known).is_ok());
    assert!(sent.admit(None, &orders, &[]).is_ok());
    for unknown in [
        vec![(p0.clone(), second.clone())],
        vec![(p0.clone(), first.clone()), (p1.clone(), first.clone())],
        vec![(p1.clone(), first.clone()), (p0.clone(), first.clone())],
    ] {
        let refused = sent.admit(None, &orders, &unknown).expect_err("refused");
        assert_eq!(refused.kind(), ConnectorErrorKind::Transient);
        assert_eq!(refused.code(), Some(POSITION_UNSENT));
    }
    let another = sent
        .admit(Some("host"), &orders, &known)
        .expect_err("refused");
    assert_eq!(another.code(), Some(POSITION_UNSENT));
}

#[test]
fn the_oldest_positions_of_a_host_are_forgotten_beyond_its_limit_and_no_other_hosts() {
    let sent = Sent::remembering(3);
    let (orders, p0) = (stream("orders"), partition("p0"));
    let kept = cursor(1, b"kept");
    sent.note(Some("other"), &orders, &p0, &kept);
    let positions: [&'static [u8]; 5] = [b"0", b"1", b"2", b"3", b"4"];
    for position in positions {
        sent.note(Some("host"), &orders, &p0, &cursor(1, position));
    }
    let remembered =
        positions.map(|position| sent.knows(Some("host"), &orders, &p0, &cursor(1, position)));
    assert_eq!(remembered, [false, false, true, true, true]);
    assert!(sent.knows(Some("other"), &orders, &p0, &kept));
}

#[test]
fn a_position_noted_again_is_remembered_until_each_noting_is_forgotten() {
    let sent = Sent::remembering(3);
    let (orders, p0) = (stream("orders"), partition("p0"));
    let (again, filler) = (cursor(1, b"again"), cursor(1, b"filler"));
    // Noted, a filler, noted again: forgetting the first noting leaves the second.
    sent.note(None, &orders, &p0, &again);
    sent.note(None, &orders, &p0, &filler);
    sent.note(None, &orders, &p0, &again);
    sent.note(None, &orders, &p0, &filler);
    assert!(sent.knows(None, &orders, &p0, &again));
    sent.note(None, &orders, &p0, &filler);
    assert!(sent.knows(None, &orders, &p0, &again));
    sent.note(None, &orders, &p0, &filler);
    assert!(!sent.knows(None, &orders, &p0, &again));
    assert!(sent.knows(None, &orders, &p0, &filler));
}

#[test]
fn a_set_remembers_one_position_at_least() {
    let sent = Sent::remembering(0);
    let (orders, p0) = (stream("orders"), partition("p0"));
    sent.note(None, &orders, &p0, &cursor(1, b"a"));
    assert!(sent.knows(None, &orders, &p0, &cursor(1, b"a")));
    sent.note(None, &orders, &p0, &cursor(1, b"b"));
    assert!(!sent.knows(None, &orders, &p0, &cursor(1, b"a")));
    assert!(sent.knows(None, &orders, &p0, &cursor(1, b"b")));
}

#[test]
fn two_sets_hash_a_position_apart() {
    // Keyed apart: what passes in one process tells a host nothing of another.
    let (orders, p0, at) = (stream("orders"), partition("p0"), cursor(1, b"10"));
    let hashes: std::collections::BTreeSet<u64> = (0..8)
        .map(|_| Sent::default().hash(&orders, &p0, &at))
        .collect();
    assert!(hashes.len() > 1);
    assert!(format!("{:?}", Sent::default()).starts_with("Sent"));
}
