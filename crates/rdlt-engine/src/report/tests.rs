use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use rdlt_connector::{CommitSeq, LoadId, PipelineId, Receipt, StreamName};

use super::{
    AttemptLog, AttemptRecord, CommitRecord, Committed, REPORTED_ATTEMPTS, Report, RunStatus,
    StreamReport,
};
use crate::error::{Error, ErrorKind};

fn commit(load: LoadId, rows: u64, streams: &[(&str, u64, u64)]) -> CommitRecord {
    CommitRecord {
        receipt: Receipt {
            load_id: load,
            commit_seq: CommitSeq::FIRST,
            committed_at: UNIX_EPOCH,
            rows,
            bytes: rows * 10,
        },
        streams: streams
            .iter()
            .map(|(name, rows, swapped)| {
                let report = StreamReport {
                    rows: *rows,
                    bytes: rows * 10,
                    commits: 1,
                    generations_swapped: *swapped,
                    discarded_rows: 1,
                    discarded_values: 2,
                    deletes_ignored: 0,
                    truncates_ignored: 0,
                    behind: None,
                    retention_resets: 0,
                };
                (StreamName::new(name).unwrap(), report)
            })
            .collect::<BTreeMap<_, _>>(),
    }
}

fn attempt(load: LoadId, commits: Vec<CommitRecord>, error: Option<&Error>) -> AttemptRecord {
    AttemptRecord {
        load_id: load,
        started_at: UNIX_EPOCH,
        ended_at: UNIX_EPOCH + Duration::from_secs(1),
        log: AttemptLog {
            committed: commits
                .into_iter()
                .fold(Committed::default(), |mut committed, commit| {
                    committed.add(commit).unwrap();
                    committed
                }),
            ..AttemptLog::default()
        },
        error: error.map(Error::report),
    }
}

#[test]
fn a_report_folds_every_attempt_from_receipts() {
    let first = LoadId::from_parts(UNIX_EPOCH, 1);
    let second = LoadId::from_parts(UNIX_EPOCH, 2);
    let failure = Error::new(ErrorKind::Source, "reading");
    let attempts = vec![
        attempt(
            first,
            vec![commit(first, 5, &[("a", 5, 0)])],
            Some(&failure),
        ),
        attempt(
            second,
            vec![
                commit(second, 7, &[("a", 3, 0), ("b", 4, 1)]),
                commit(second, 2, &[("b", 2, 0)]),
            ],
            None,
        ),
    ];
    let pipeline = PipelineId::parse("p").unwrap();
    let report = Report::fold(
        pipeline.clone(),
        RunStatus::Succeeded,
        attempts,
        Duration::from_secs(9),
        42,
    );
    assert_eq!(report.pipeline, pipeline);
    assert_eq!(report.status, RunStatus::Succeeded);
    assert_eq!((report.rows, report.bytes, report.commits), (14, 140, 3));
    assert_eq!(report.elapsed, Duration::from_secs(9));
    assert_eq!(report.peak_memory, 42);
    assert_eq!(report.attempts.len(), 2);
    assert_eq!(
        (report.attempts[0].rows, report.attempts[0].commits),
        (5, 1)
    );
    assert_eq!(
        report.attempts[0].error.as_ref().map(|error| error.kind),
        Some(ErrorKind::Source)
    );
    assert_eq!(
        (
            report.attempts[1].rows,
            report.attempts[1].bytes,
            report.attempts[1].commits
        ),
        (9, 90, 2)
    );
    assert_eq!(report.attempts[1].error, None);
    let a = &report.streams["a"];
    assert_eq!(
        (a.rows, a.bytes, a.commits, a.generations_swapped),
        (8, 80, 2, 0)
    );
    let b = &report.streams["b"];
    assert_eq!((b.rows, b.commits, b.generations_swapped), (6, 2, 1));
}

#[test]
fn a_report_lists_the_latest_attempts_and_counts_every_one() {
    let attempts: Vec<AttemptRecord> = (0..REPORTED_ATTEMPTS as u128 + 5)
        .map(|load| {
            let load = LoadId::from_parts(UNIX_EPOCH, load);
            attempt(load, vec![commit(load, 2, &[("orders", 2, 16)])], None)
        })
        .collect();
    let first_listed = attempts[5].load_id;
    let pipeline = PipelineId::parse("orders").unwrap();
    let report = Report::fold(pipeline, RunStatus::Stopped, attempts, Duration::ZERO, 0);
    assert_eq!(report.attempts.len(), REPORTED_ATTEMPTS);
    assert_eq!(report.attempts[0].load_id, first_listed);
    assert_eq!(report.attempted, REPORTED_ATTEMPTS as u64 + 5);
    // What each attempt committed still counts, listed or not.
    assert_eq!(report.commits, REPORTED_ATTEMPTS as u64 + 5);
    assert_eq!(report.rows, 2 * (REPORTED_ATTEMPTS as u64 + 5));
}

#[test]
fn a_commit_credited_to_a_folded_attempt_counts_toward_it_and_the_run() {
    let pipeline = PipelineId::parse("orders").unwrap();
    let failed = LoadId::from_parts(UNIX_EPOCH, 1);
    let mut report = Report::new(pipeline);
    report.absorb(attempt(failed, vec![], None)).unwrap();
    report
        .credit(commit(failed, 3, &[("orders", 3, 24)]))
        .unwrap();
    assert_eq!((report.commits, report.rows, report.bytes), (1, 3, 30));
    assert_eq!(report.streams["orders"].rows, 3);
    let listed = &report.attempts[0];
    assert_eq!((listed.commits, listed.rows, listed.bytes), (1, 3, 30));
}

#[test]
fn a_report_totals_each_stream_s_resets_and_keeps_its_latest_known_lag() {
    let pipeline = PipelineId::parse("orders").unwrap();
    let mut report = Report::new(pipeline);
    let orders = || StreamName::new("orders").unwrap();
    let signalled = |load, behind: Option<u64>, resets: u64| {
        let mut signalled = attempt(LoadId::from_parts(UNIX_EPOCH, load), vec![], None);
        signalled
            .log
            .behind
            .extend(behind.map(|behind| (orders(), behind)));
        signalled.log.retention_resets.insert(orders(), resets);
        signalled
    };
    report.absorb(signalled(1, Some(100), 1)).unwrap();
    // An attempt whose reads never said how far behind they were leaves the last it knew.
    report.absorb(signalled(2, None, 2)).unwrap();
    assert_eq!(report.streams["orders"].behind, Some(100));
    assert_eq!(report.streams["orders"].retention_resets, 3);
    report.absorb(signalled(3, Some(7), 0)).unwrap();
    assert_eq!(report.streams["orders"].behind, Some(7));
    assert_eq!(report.streams["orders"].retention_resets, 3);
}

/// A commit of `load` whose receipt counts `rows` and `bytes`.
fn counted(load: LoadId, rows: u64, bytes: u64) -> CommitRecord {
    CommitRecord {
        receipt: Receipt {
            rows,
            bytes,
            ..commit(load, 0, &[]).receipt
        },
        streams: BTreeMap::new(),
    }
}

fn overflowed(result: Result<(), Error>) {
    let error = result.unwrap_err();
    assert_eq!(
        (error.kind(), error.code(), error.is_retryable()),
        (ErrorKind::Destination, Some("receipt_overflow"), false)
    );
}

#[test]
fn receipts_counting_past_a_total_are_refused_and_change_nothing() {
    let load = LoadId::from_parts(UNIX_EPOCH, 1);
    for (rows, bytes) in [(u64::MAX, 0), (0, u64::MAX)] {
        let mut committed = Committed::default();
        committed.add(counted(load, rows, bytes)).unwrap();
        overflowed(committed.add(counted(load, 1, 1)));
        assert_eq!(
            (committed.commits, committed.rows, committed.bytes),
            (1, rows, bytes)
        );
        let mut report = Report::new(PipelineId::parse("orders").unwrap());
        report
            .absorb(attempt(load, vec![counted(load, rows, bytes)], None))
            .unwrap();
        overflowed(report.absorb(attempt(load, vec![counted(load, 1, 1)], None)));
        overflowed(report.credit(counted(load, 1, 1)));
        assert_eq!(
            (report.attempted, report.commits, report.rows, report.bytes),
            (1, 1, rows, bytes)
        );
        let listed = &report.attempts[0];
        assert_eq!((listed.rows, listed.bytes), (rows, bytes));
    }
}
