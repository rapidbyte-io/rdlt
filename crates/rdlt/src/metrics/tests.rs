use std::time::{Duration, SystemTime};

use metrics::{SharedString, Unit};
use metrics_util::CompositeKey;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rdlt_connector::PipelineId;
use rdlt_engine::{
    Acks, CommitPhases, ErrorKind, ErrorReport, Event, EventKind, Listed, Origin, RunStatus, Scope,
    StreamReport, Trigger,
};
use tokio::sync::broadcast;

use super::{approx, drained, recorded};

/// What a recorder snapshot holds, which reading takes from the recorder.
type Taken = Vec<(CompositeKey, Option<Unit>, Option<SharedString>, DebugValue)>;

fn event(kind: EventKind) -> Event {
    Event {
        seq: 1,
        at: SystemTime::now(),
        pipeline: PipelineId::parse("metered").unwrap(),
        attempt: 1,
        kind,
    }
}

/// The count `name` holds in `taken`, whatever its labels, where it is a counter.
fn counted(taken: &Taken, name: &str, stream: Option<&str>) -> Option<u64> {
    taken.iter().find_map(|(key, _, _, value)| {
        let labelled = stream.is_none_or(|stream| {
            key.key()
                .labels()
                .any(|label| label.key() == "stream" && label.value() == stream)
        });
        match value {
            DebugValue::Counter(count) if key.key().name() == name && labelled => Some(*count),
            _ => None,
        }
    })
}

fn retrying(scope: Scope) -> EventKind {
    EventKind::Retrying {
        scope,
        error: ErrorReport {
            kind: ErrorKind::Destination,
            origin: Origin::Destination,
            stream: None,
            code: None,
            connector_code: None,
            message: "failed".to_owned(),
            causes: Vec::new(),
            retryable: true,
        },
        retry_in: Duration::from_secs(1),
    }
}

/// A commit of three rows of `users`, two rows and four values of them discarded.
fn committed() -> EventKind {
    EventKind::Committed {
        commit: 1,
        trigger: Trigger::Interval,
        rows: 3,
        bytes: 30,
        took: Duration::from_millis(40),
        phases: CommitPhases::default(),
        streams: [(
            "users".to_owned(),
            StreamReport {
                rows: 3,
                bytes: 30,
                discarded_rows: 2,
                discarded_values: 4,
                ..StreamReport::default()
            },
        )]
        .into(),
        acks: Acks::default(),
    }
}

#[test]
fn each_kind_of_event_counts_what_it_tells() {
    let debugging = DebuggingRecorder::new();
    let snapshotter = debugging.snapshotter();
    let _recording = metrics::set_default_local_recorder(&debugging);
    recorded(
        "metered",
        &event(EventKind::Credited { rows: 7, bytes: 70 }),
    );
    recorded("metered", &event(retrying(Scope::Attempt)));
    recorded("metered", &event(retrying(Scope::Commit)));
    recorded("metered", &event(retrying(Scope::Commit)));
    recorded(
        "metered",
        &event(EventKind::SchemaChanged {
            table: "t".to_owned(),
            version: 2,
            changes: Listed {
                entries: Vec::new(),
                unlisted: 0,
            },
        }),
    );
    recorded("metered", &event(EventKind::AttemptStarted));
    recorded(
        "metered",
        &event(EventKind::RunEnded {
            status: RunStatus::Succeeded,
        }),
    );
    recorded("metered", &event(committed()));
    let taken = snapshotter.snapshot().into_vec();
    for (name, count) in [
        ("rdlt_rows_credited_total", 7),
        ("rdlt_attempt_retries_total", 1),
        ("rdlt_commit_retries_total", 2),
        ("rdlt_schema_changes_total", 1),
        ("rdlt_commits_total", 1),
    ] {
        assert_eq!(counted(&taken, name, None), Some(count), "{name}");
    }
    for (name, count) in [
        ("rdlt_rows_committed_total", 3),
        ("rdlt_bytes_committed_total", 30),
        ("rdlt_rows_discarded_total", 2),
        ("rdlt_values_discarded_total", 4),
    ] {
        assert_eq!(counted(&taken, name, Some("users")), Some(count), "{name}");
    }
}

#[test]
fn a_metering_receiver_that_fell_behind_counts_the_events_it_missed() {
    let debugging = DebuggingRecorder::new();
    let snapshotter = debugging.snapshotter();
    let _recording = metrics::set_default_local_recorder(&debugging);
    let (events, mut metered) = broadcast::channel(2);
    for _ in 0..5 {
        events
            .send(event(EventKind::Credited { rows: 1, bytes: 1 }))
            .unwrap();
    }
    drained("metered", &mut metered);
    let taken = snapshotter.snapshot().into_vec();
    assert_eq!(
        counted(&taken, "rdlt_events_unmetered_total", None),
        Some(3)
    );
    assert_eq!(counted(&taken, "rdlt_rows_credited_total", None), Some(2));
}

#[test]
fn a_gauge_holds_the_value_it_measures() {
    assert!((approx(0) - 0.0).abs() < f64::EPSILON);
    assert!((approx(4_096) - 4_096.0).abs() < f64::EPSILON);
}
