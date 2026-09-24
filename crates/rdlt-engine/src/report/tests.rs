use std::collections::BTreeMap;
use std::time::{Duration, UNIX_EPOCH};

use rdlt_connector::{CommitSeq, LoadId, PipelineId, Receipt, StreamName};

use super::{AttemptLog, AttemptRecord, CommitRecord, Report, RunStatus, StreamReport};
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
            commits,
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
