use std::time::UNIX_EPOCH;

use rdlt_connector::{CommitSeq, Horizon, LoadId};

use super::earliest;

fn load(id: u128) -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, id)
}

fn seq(number: u64) -> CommitSeq {
    (1..number).fold(CommitSeq::FIRST, |seq, _| seq.next())
}

fn at(id: u128, number: u64) -> Horizon {
    Horizon {
        load_id: load(id),
        commit_seq: seq(number),
    }
}

#[test]
fn with_no_other_log_the_horizon_is_the_oldest_commit_the_load_s_own_may_repeat() {
    assert_eq!(earliest(load(5), seq(7), &[]), at(5, 7));
    assert_eq!(earliest(load(5), seq(7), &[load(5)]), at(5, 7));
}

#[test]
fn an_older_load_s_log_holds_the_horizon_at_its_first_commit() {
    assert_eq!(
        earliest(load(5), seq(7), &[load(9), load(3), load(4)]),
        at(3, 1)
    );
}

#[test]
fn a_newer_load_s_log_leaves_the_horizon_where_the_load_s_own_holds_it() {
    assert_eq!(earliest(load(5), seq(7), &[load(6), load(5)]), at(5, 7));
}
