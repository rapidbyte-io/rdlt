use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use rdlt_connector::{Epoch, PipelineId};
use rdlt_engine::{
    Acks, AttemptReport, CommitPhases, Counters, ErrorKind, ErrorReport, Event, EventKind,
    MemoryHeld, OpenWaits, Origin, Partitions, Phase, Report, RunStatus, Scope, Status, Trigger,
};

use super::{Told, disagreements};

fn pipeline() -> PipelineId {
    PipelineId::parse("told").unwrap()
}

/// An attempt of 5 rows of 40 bytes, failed where `failed` says.
fn attempt(failed: bool) -> AttemptReport {
    let error = ErrorReport {
        kind: ErrorKind::Source,
        origin: Origin::Source,
        stream: None,
        code: None,
        connector_code: None,
        message: "the read failed".to_owned(),
        causes: Vec::new(),
        retryable: true,
    };
    AttemptReport {
        epoch: Some(Epoch(1)),
        started_at: SystemTime::UNIX_EPOCH,
        ended_at: SystemTime::UNIX_EPOCH,
        commits: 1,
        rows: 5,
        bytes: 40,
        error: failed.then_some(error),
    }
}

/// The report of a run of two attempts, the first failed, each committing 5 rows of 40 bytes.
fn report() -> Report {
    Report {
        pipeline: pipeline(),
        status: RunStatus::Succeeded,
        attempts: vec![attempt(true), attempt(false)],
        attempted: 2,
        elapsed: Duration::from_secs(3),
        rows: 10,
        bytes: 80,
        commits: 2,
        peak_memory: 0,
        counters: Counters::default(),
        streams: BTreeMap::new(),
    }
}

/// The status of a run that ended with `report`.
fn ended(report: &Report) -> Status {
    Status {
        phase: Phase::Ended {
            status: report.status,
        },
        since: SystemTime::UNIX_EPOCH,
        partitions: Partitions::default(),
        quietest: None,
        waiting: OpenWaits::default(),
        memory: MemoryHeld::default(),
        writers: Vec::new(),
        streams: BTreeMap::new(),
        report: report.clone(),
    }
}

/// An event of `attempt`, whose place [`told`] gives it among those it is handed.
fn event(attempt: u64, kind: EventKind) -> Event {
    Event {
        seq: 0,
        at: SystemTime::UNIX_EPOCH,
        pipeline: pipeline(),
        attempt,
        kind,
    }
}

fn committed(rows: u64, bytes: u64) -> EventKind {
    EventKind::Committed {
        commit: 1,
        trigger: Trigger::End,
        rows,
        bytes,
        took: Duration::ZERO,
        phases: CommitPhases::default(),
        streams: BTreeMap::new(),
        acks: Acks::default(),
    }
}

/// What a run of [`report`] tells: its first attempt's commit's answer lost, credited once the
/// second found it, then the second's commit, and its end.
fn agreeing() -> Vec<Event> {
    let retry = EventKind::Retrying {
        scope: Scope::Attempt,
        error: attempt(true).error.unwrap(),
        retry_in: Duration::from_secs(1),
    };
    let end = EventKind::RunEnded {
        status: RunStatus::Succeeded,
    };
    vec![
        event(1, EventKind::AttemptStarted),
        event(1, retry),
        event(2, EventKind::AttemptStarted),
        event(2, EventKind::Credited { rows: 5, bytes: 40 }),
        event(2, committed(5, 40)),
        event(2, end),
    ]
}

/// What a run that told `events`, numbered in their order, comes to.
fn told(events: &[Event]) -> Told {
    let mut told = Told::default();
    for (seq, event) in (1..).zip(events) {
        told.tell(&Event {
            seq,
            ..event.clone()
        });
    }
    told
}

#[test]
fn events_that_agree_with_their_run_s_end_are_no_violation() {
    let report = report();
    let wrong = disagreements(&told(&agreeing()), &report, &ended(&report));
    assert_eq!(wrong, Vec::<String>::new());
}

#[test]
fn each_disagreement_of_a_run_s_events_with_its_end_is_a_violation() {
    let report = report();
    let without = |dropped: fn(&Event) -> bool| {
        let events: Vec<Event> = agreeing()
            .into_iter()
            .filter(|event| !dropped(event))
            .collect();
        disagreements(&told(&events), &report, &ended(&report)).len()
    };
    let credit = |event: &Event| matches!(event.kind, EventKind::Credited { .. });
    assert_eq!(
        without(credit),
        1,
        "a credit not told leaves the rows short"
    );
    let retry = |event: &Event| matches!(event.kind, EventKind::Retrying { .. });
    assert_eq!(
        without(retry),
        1,
        "a failed attempt is retried or ends the run"
    );
    let end = |event: &Event| matches!(event.kind, EventKind::RunEnded { .. });
    assert_eq!(without(end), 1, "the end is told");
    let mut late = agreeing();
    late.push(event(2, EventKind::AttemptStarted));
    let wrong = disagreements(&told(&late), &report, &ended(&report));
    assert_eq!(wrong.len(), 1, "nothing is told after the end");
    let mut stale = ended(&report);
    stale.report.rows = 5;
    let wrong = disagreements(&told(&agreeing()), &report, &stale);
    assert_eq!(
        wrong.len(),
        1,
        "the status once ended carries the outcome's report"
    );
}

#[test]
fn a_number_skipped_or_repeated_is_a_violation() {
    let report = report();
    let numbered = |numbers: [u64; 6]| {
        let mut told = Told::default();
        for (seq, event) in numbers.into_iter().zip(agreeing()) {
            told.tell(&Event { seq, ..event });
        }
        told
    };
    for numbers in [[1, 2, 3, 5, 6, 7], [1, 2, 3, 3, 4, 5]] {
        let wrong = disagreements(&numbered(numbers), &report, &ended(&report));
        assert_eq!(wrong.len(), 1, "{numbers:?}: {wrong:?}");
        assert_eq!(
            numbered(numbers).gaps,
            1,
            "{numbers:?} breaks the numbers once"
        );
    }
}

#[test]
fn the_failed_attempt_that_ended_the_run_needs_no_retry() {
    let mut report = report();
    report.status = RunStatus::Failed;
    report.attempts[1] = attempt(true);
    let mut events = agreeing();
    events.pop();
    let end = EventKind::RunEnded {
        status: RunStatus::Failed,
    };
    events.push(event(2, end));
    let wrong = disagreements(&told(&events), &report, &ended(&report));
    assert_eq!(wrong, Vec::<String>::new());
}
