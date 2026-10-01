use std::sync::Arc;

use super::{Acknowledgeable, Read};
use crate::wire::v1;

fn read(stream: &str, partition: &str) -> Read {
    Read {
        stream: (None, stream.to_owned()),
        partition: partition.to_owned(),
    }
}

fn cursor(version: u32, bytes: &[u8]) -> v1::Cursor {
    v1::Cursor {
        version,
        bytes: bytes.to_vec().into(),
    }
}

#[test]
fn a_checkpoint_is_its_hosts_its_streams_its_partitions_and_its_formats() {
    let set = Acknowledgeable::default();
    let host: Arc<str> = "host".into();
    let checkpoint = (read("orders", "p0"), cursor(1, b"10"));
    assert!(!set.was_sent(Some(&host), &checkpoint.0, &checkpoint.1));
    set.sent(Some(&host), &checkpoint.0, &checkpoint.1);
    assert!(set.was_sent(Some(&host), &checkpoint.0, &checkpoint.1));
    let other: Arc<str> = "other".into();
    assert!(!set.was_sent(Some(&other), &checkpoint.0, &checkpoint.1));
    assert!(!set.was_sent(None, &checkpoint.0, &checkpoint.1));
    assert!(!set.was_sent(Some(&host), &read("orders", "p1"), &checkpoint.1));
    assert!(!set.was_sent(Some(&host), &read("users", "p0"), &checkpoint.1));
    let namespaced = Read {
        stream: (Some("shop".to_owned()), "orders".to_owned()),
        ..read("orders", "p0")
    };
    assert!(!set.was_sent(Some(&host), &namespaced, &checkpoint.1));
    assert!(!set.was_sent(Some(&host), &checkpoint.0, &cursor(2, b"10")));
    assert!(!set.was_sent(Some(&host), &checkpoint.0, &cursor(1, b"11")));
    assert!(!set.was_sent(Some(&host), &checkpoint.0, &cursor(1, b"")));
    // What a spawned connector's host was sent is that host's, which has no name.
    set.sent(None, &checkpoint.0, &checkpoint.1);
    assert!(set.was_sent(None, &checkpoint.0, &checkpoint.1));
}

#[test]
fn the_oldest_checkpoints_of_a_host_are_forgotten_beyond_its_limit_and_no_other_hosts() {
    let set = Acknowledgeable::remembering(3);
    let (host, other): (Arc<str>, Arc<str>) = ("host".into(), "other".into());
    let orders = read("orders", "p0");
    let kept = cursor(1, b"kept");
    set.sent(Some(&other), &orders, &kept);
    for position in 0_u8..5 {
        set.sent(Some(&host), &orders, &cursor(1, &[position]));
    }
    let remembered: Vec<bool> = (0_u8..5)
        .map(|position| set.was_sent(Some(&host), &orders, &cursor(1, &[position])))
        .collect();
    assert_eq!(remembered, [false, false, true, true, true]);
    assert!(set.was_sent(Some(&other), &orders, &kept));
}

#[test]
fn a_checkpoint_sent_again_is_remembered_until_each_sending_is_forgotten() {
    let set = Acknowledgeable::remembering(3);
    let orders = read("orders", "p0");
    let (again, filler) = (cursor(1, b"again"), cursor(1, b"filler"));
    // Sent, a filler, sent again: forgetting the first sending leaves the second.
    set.sent(None, &orders, &again);
    set.sent(None, &orders, &filler);
    set.sent(None, &orders, &again);
    set.sent(None, &orders, &filler);
    assert!(set.was_sent(None, &orders, &again));
    set.sent(None, &orders, &filler);
    assert!(set.was_sent(None, &orders, &again));
    set.sent(None, &orders, &filler);
    assert!(!set.was_sent(None, &orders, &again));
    assert!(set.was_sent(None, &orders, &filler));
}

#[test]
fn a_set_remembers_one_checkpoint_at_least() {
    let set = Acknowledgeable::remembering(0);
    let orders = read("orders", "p0");
    set.sent(None, &orders, &cursor(1, b"a"));
    assert!(set.was_sent(None, &orders, &cursor(1, b"a")));
    set.sent(None, &orders, &cursor(1, b"b"));
    assert!(!set.was_sent(None, &orders, &cursor(1, b"a")));
    assert!(set.was_sent(None, &orders, &cursor(1, b"b")));
}

#[test]
fn two_sets_hash_a_checkpoint_apart() {
    // Keyed apart: what passes in one process tells a host nothing of another.
    let orders = read("orders", "p0");
    let hashes: std::collections::BTreeSet<u64> = (0..8)
        .map(|_| Acknowledgeable::default().hash(&orders, &cursor(1, b"10")))
        .collect();
    assert!(hashes.len() > 1);
}
