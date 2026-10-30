use std::collections::BTreeMap;

use rdlt_connector::{CommitKey, Epoch, PipelineId, Receipt, StreamName};

use super::{Credit, Ledger};
use crate::report::{AttemptLog, AttemptRecord, Pending, StreamReport};

/// The key of commit `seq` of the session opened at `epoch`.
fn key(epoch: u64, seq: u64) -> CommitKey {
    CommitKey {
        epoch: Epoch(epoch),
        seq,
    }
}

/// The receipt of the commit keyed `key`, of `rows` rows, as the destination keeps it.
fn landed(key: CommitKey, rows: u64) -> Receipt {
    Receipt {
        key,
        committed_at: std::time::UNIX_EPOCH,
        rows,
        bytes: rows * 10,
    }
}

/// An attempt whose open found `landed` landed at `epoch`, or that opened nothing, and which
/// failed with the commit keyed `pending` in flight.
fn attempt(opened: Option<(u64, Option<Receipt>)>, pending: Option<CommitKey>) -> AttemptRecord {
    let streams = BTreeMap::from([(
        StreamName::new("orders").unwrap(),
        StreamReport {
            rows: 3,
            commits: 1,
            ..StreamReport::default()
        },
    )]);
    AttemptRecord {
        started_at: std::time::UNIX_EPOCH,
        ended_at: std::time::UNIX_EPOCH,
        log: AttemptLog {
            opened: opened.map(|(epoch, landed)| (Epoch(epoch), landed)),
            pending: pending.map(|key| Pending {
                key,
                streams: streams.clone(),
            }),
            ..AttemptLog::default()
        },
        error: None,
    }
}

#[test]
fn a_lost_answer_is_credited_once_the_next_open_finds_its_key_landed() {
    let mut ledger = Ledger::new(PipelineId::parse("orders").unwrap());
    ledger
        .record(attempt(Some((2, None)), Some(key(2, 1))), true)
        .unwrap();
    // The destination counted the rows it published; the report takes its count.
    let found = landed(key(2, 1), 4);
    let (_, credited) = ledger
        .record(attempt(Some((3, Some(found))), None), false)
        .unwrap();
    let credit = Credit { rows: 4, bytes: 40 };
    assert_eq!(credited, Some(credit), "the run tells what it credited");
    let report = ledger.report();
    assert_eq!((report.commits, report.rows, report.bytes), (1, 4, 40));
    assert_eq!(report.streams["orders"].rows, 3);
    assert_eq!(
        (report.attempts[0].epoch, report.attempts[0].commits),
        (Some(Epoch(2)), 1),
        "credited to the attempt whose epoch its key names"
    );
}

#[test]
fn a_commit_in_flight_whose_key_did_not_land_is_never_credited() {
    for found in [None, Some(landed(key(2, 0), 9)), Some(landed(key(1, 7), 9))] {
        let mut ledger = Ledger::new(PipelineId::parse("orders").unwrap());
        ledger
            .record(attempt(Some((2, None)), Some(key(2, 1))), true)
            .unwrap();
        ledger
            .record(attempt(Some((3, found.clone())), None), false)
            .unwrap();
        let report = ledger.report();
        assert_eq!((report.commits, report.rows), (0, 0), "{found:?}");
    }
}

#[test]
fn a_commit_in_flight_waits_past_an_attempt_that_opened_nothing() {
    let mut ledger = Ledger::new(PipelineId::parse("orders").unwrap());
    ledger
        .record(attempt(Some((2, None)), Some(key(2, 1))), true)
        .unwrap();
    ledger.record(attempt(None, None), true).unwrap();
    ledger
        .record(attempt(Some((3, Some(landed(key(2, 1), 4)))), None), false)
        .unwrap();
    let report = ledger.report();
    assert_eq!((report.attempted, report.commits, report.rows), (3, 1, 4));
}
