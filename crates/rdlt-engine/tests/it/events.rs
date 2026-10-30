//! What a run tells its environment as it happens: each attempt, retry, commit, credit and
//! schema change, then its end.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Date64Array, Int64Array, StructArray};
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::CommitMeta;
use rdlt_connector::limits::MAX_ERROR_TEXT_BYTES;
use rdlt_connector::text::CUT;
use rdlt_engine::{
    ColumnChange, Discard, DiscardReason, Event, EventKind, Origin, Phase, REPORTED_LISTED, Report,
    RunStatus, SchemaPolicy, SchemaSettings, Scope, Status, Step as CommitStep, StopMode,
};

use crate::schema::{batch, ints, text};
use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::{Step, failing};
use crate::support::faults::{Failure, Fault, Rule, failing_commits};
use crate::support::script::{Script, ScriptStream};
use crate::support::{
    Told, commit_every, generator, memory, observed_engine, pipeline, stream, told_engine,
};

/// The rows and bytes `told`'s commits and credits counted.
fn counted(told: &Told) -> (u64, u64) {
    let mut counted = (0, 0);
    for kind in told.kinds() {
        if let EventKind::Committed { rows, bytes, .. } | EventKind::Credited { rows, bytes } = kind
        {
            counted = (counted.0 + rows, counted.1 + bytes);
        }
    }
    counted
}

/// The schema changes `events` tell of, in order.
fn changes(events: &[Event]) -> Vec<ColumnChange> {
    let changed = events.iter().filter_map(|event| match &event.kind {
        EventKind::SchemaChanged { changes, .. } => Some(changes.entries.clone()),
        _ => None,
    });
    changed.flatten().collect()
}

/// Checks that `told` counted what `report` did, each event of its pipeline, and ended last with
/// the report's status.
fn agrees(told: &Told, report: &Report) {
    let events = told.events();
    assert_eq!(counted(told), (report.rows, report.bytes));
    let commits = events.iter().filter(|event| {
        matches!(
            event.kind,
            EventKind::Committed { .. } | EventKind::Credited { .. }
        )
    });
    assert_eq!(u64::try_from(commits.count()).ok(), Some(report.commits));
    assert!(events.iter().all(|event| event.pipeline == report.pipeline));
    let ended = events
        .iter()
        .filter(|event| matches!(event.kind, EventKind::RunEnded { .. }))
        .count();
    assert_eq!(ended, 1, "the run ends once");
    let last = events.last().expect("a run tells of itself");
    let status = report.status;
    assert_eq!(last.kind, EventKind::RunEnded { status });
    assert_eq!(last.attempt, report.attempted);
}

#[tokio::test(start_paused = true)]
async fn a_run_tells_each_attempt_its_retry_its_commits_and_its_end_in_order() {
    let (engine, told) = told_engine(commit_every(10));
    let destination = failing(memory("events_order").await, Step::CommitOnce);
    let source = generator(&[("events", 40, 2, 10)]).await;
    let outcome = engine
        .run(
            pipeline("events_order", [stream("events")]),
            source,
            destination,
        )
        .await;
    let report = &outcome.report;
    assert_eq!(report.status, RunStatus::Succeeded, "{:?}", outcome.error);
    assert_eq!(report.attempted, 2);
    agrees(&told, report);
    let events = told.events();
    let started: Vec<(usize, u64)> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.kind == EventKind::AttemptStarted)
        .map(|(place, event)| (place, event.attempt))
        .collect();
    assert_eq!(
        started
            .iter()
            .map(|(_, attempt)| *attempt)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(started[0].0, 0, "the first attempt is told first");
    let retry = events
        .iter()
        .position(|event| matches!(event.kind, EventKind::Retrying { .. }))
        .expect("the failed attempt is retried");
    let EventKind::Retrying { scope, error, .. } = &events[retry].kind else {
        unreachable!("found as a retry");
    };
    assert_eq!(*scope, Scope::Attempt);
    assert_eq!(events[retry].attempt, 1);
    assert_eq!(Some(error), report.attempts[0].error.as_ref());
    assert!(
        retry < started[1].0,
        "the retry is told before the attempt it starts"
    );
    for pair in events.windows(2) {
        assert!(pair[0].at <= pair[1].at, "events are told in their order");
    }
}

#[tokio::test(start_paused = true)]
async fn a_commit_whose_answer_was_lost_is_credited_once_a_later_attempt_finds_it_landed() {
    let (engine, told) = told_engine(commit_every(10));
    let destination = failing(memory("events_credit").await, Step::LoseResponse);
    let source = generator(&[("events", 40, 1, 10)]).await;
    let outcome = engine
        .run(
            pipeline("events_credit", [stream("events")]),
            source,
            destination,
        )
        .await;
    let report = &outcome.report;
    assert_eq!(report.status, RunStatus::Succeeded, "{:?}", outcome.error);
    agrees(&told, report);
    let credited: Vec<(u64, EventKind)> = told
        .events()
        .into_iter()
        .filter(|event| matches!(event.kind, EventKind::Credited { .. }))
        .map(|event| (event.attempt, event.kind))
        .collect();
    let [(attempt, EventKind::Credited { rows, .. })] = credited.as_slice() else {
        panic!("one credit: {credited:?}");
    };
    assert_eq!(*attempt, 2, "the attempt that found it landed tells of it");
    assert!(*rows > 0);
}

#[tokio::test(start_paused = true)]
async fn a_schema_change_tells_an_added_column_a_variant_column_and_a_widening() {
    let small = Arc::new(arrow_array::Int32Array::from(vec![1])) as ArrayRef;
    let stream_batches = vec![
        batch(vec![("id", small)]),
        batch(vec![("id", ints(&[1 << 40])), ("note", text(&["n"]))]),
        batch(vec![("id", text(&["x"]))]),
    ];
    let source = batches(
        "events_schema",
        vec![BatchStream::new("events", stream_batches)],
    )
    .await;
    let (engine, told) = told_engine(commit_every(10));
    let outcome = engine
        .run(
            pipeline("events_schema", [stream("events")]),
            source,
            memory("events_schema").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    let events = told.events();
    let tables: Vec<(String, u32)> = events
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::SchemaChanged { table, version, .. } => Some((table.clone(), *version)),
            _ => None,
        })
        .collect();
    assert_eq!(tables.first(), Some(&("events".to_owned(), 1)));
    let changes = changes(&events);
    let added = |column: &str, logical_type: &str, variant| ColumnChange::Added {
        column: column.to_owned(),
        logical_type: logical_type.to_owned(),
        variant,
    };
    assert_eq!(
        changes.first(),
        Some(&added("id", "int32", false)),
        "{changes:?}"
    );
    for change in [
        ColumnChange::Widened {
            column: "id".to_owned(),
            from: "int32".to_owned(),
            to: "int64".to_owned(),
        },
        added("note", "utf8", false),
        added("id__json", "json", true),
    ] {
        assert!(changes.contains(&change), "{change:?} in {changes:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_change_s_text_is_cut_to_its_bound_and_its_list_counts_what_it_leaves_out() {
    // A struct of many long field names, whose type's text passes the bound, then more columns
    // than a list names.
    let fields: Vec<(Arc<ArrowField>, ArrayRef)> = (0..300)
        .map(|index| {
            let field = ArrowField::new(
                format!("a_field_of_a_long_name_{index}"),
                DataType::Int64,
                true,
            );
            (
                Arc::new(field),
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            )
        })
        .collect();
    let mut columns: Vec<(String, ArrayRef)> =
        vec![("wide".to_owned(), Arc::new(StructArray::from(fields)))];
    let extra = REPORTED_LISTED + 10;
    columns.extend((0..extra).map(|index| (format!("c{index}"), ints(&[1]))));
    let first = arrow_array::RecordBatch::try_from_iter(columns).expect("a valid batch");
    let source = batches("events_cut", vec![BatchStream::new("events", vec![first])]).await;
    let (engine, told) = told_engine(commit_every(10));
    let outcome = engine
        .run(
            pipeline("events_cut", [stream("events")]),
            source,
            memory("events_cut").await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let events = told.events();
    let created = events
        .iter()
        .find_map(|event| match &event.kind {
            EventKind::SchemaChanged { changes, .. } => Some(changes.clone()),
            _ => None,
        })
        .expect("the table's creation is told");
    assert_eq!(created.entries.len(), REPORTED_LISTED);
    assert_eq!(created.unlisted, 11, "a column and ten more than are named");
    let ColumnChange::Added { logical_type, .. } = &created.entries[0] else {
        panic!(
            "a created table's columns are added: {:?}",
            created.entries[0]
        );
    };
    assert!(
        logical_type.len() <= MAX_ERROR_TEXT_BYTES,
        "{} bytes",
        logical_type.len()
    );
    assert!(logical_type.ends_with(CUT));
}

/// What the stream `events` discarded of `column` for `reason`, its rows and values.
fn discard(column: &str, reason: DiscardReason, (rows, values): (u64, u64)) -> Discard {
    Discard {
        table: "events".to_owned(),
        column: column.to_owned(),
        reason,
        rows,
        values,
    }
}

#[tokio::test(start_paused = true)]
async fn a_commit_tells_what_each_policy_discarded_by_table_column_and_reason() {
    // A date no date holds among two that are held, then a column the table lacks.
    let dates: ArrayRef = Arc::new(Date64Array::from(vec![0, i64::MAX, 86_400_000]));
    let stream_batches = || {
        vec![
            batch(vec![("id", ints(&[1, 2, 3])), ("d", Arc::clone(&dates))]),
            batch(vec![("id", ints(&[4, 5])), ("extra", text(&["x", "y"]))]),
        ]
    };
    for (store, policy, discards) in [
        (
            "events_discard_rows",
            SchemaPolicy::DiscardRow,
            [
                discard("d", DiscardReason::OutOfRange, (1, 0)),
                discard("extra", DiscardReason::SchemaChange, (2, 0)),
            ],
        ),
        (
            "events_discard_values",
            SchemaPolicy::DiscardValue,
            [
                discard("d", DiscardReason::OutOfRange, (0, 1)),
                discard("extra", DiscardReason::SchemaChange, (0, 2)),
            ],
        ),
    ] {
        let source = batches(store, vec![BatchStream::new("events", stream_batches())]).await;
        let (engine, told) = told_engine(commit_every(100));
        let plan = stream("events").schema(SchemaSettings::new().policy(policy));
        let outcome = engine
            .run(pipeline(store, [plan]), source, memory(store).await)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
        let mut told: Vec<Discard> = told
            .kinds()
            .into_iter()
            .filter_map(|kind| match kind {
                EventKind::Committed { streams, .. } => Some(streams),
                _ => None,
            })
            .flat_map(BTreeMap::into_values)
            .flat_map(|stream| stream.discards.listed)
            .collect();
        told.sort_by(|left, right| left.column.cmp(&right.column));
        assert_eq!(told, discards, "{policy:?}");
        let reported = &outcome.report.streams["events"].discards;
        assert_eq!(
            reported.listed.len(),
            2,
            "{policy:?}: the run's report counts them too"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_commit_whose_report_a_stop_cuts_short_is_told_before_the_run_s_end() {
    let script = Script::new(vec![ScriptStream::new("events", 1, 10, 5)])
        .answering_after(Duration::from_secs(60));
    let (_, source) = script.connect("events_cut_short").await;
    let (engine, told) = told_engine(commit_every(10));
    let mut run = engine.run(
        pipeline("events_cut_short", [stream("events")]),
        source,
        memory("events_cut_short").await,
    );
    let reporting = |status: &Status| {
        matches!(
            status.phase,
            Phase::Committing {
                step: CommitStep::Ack,
                ..
            }
        )
    };
    while !reporting(&run.status()) {
        tokio::select! {
            biased;
            outcome = &mut run => panic!("the run ended first: {:?}", outcome.error),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    tokio::select! {
        biased;
        outcome = &mut run => panic!("the run ended first: {:?}", outcome.error),
        () = tokio::time::sleep(Duration::from_secs(5)) => {}
    }
    run.control().stop(StopMode::Now);
    let outcome = run.await;
    let report = &outcome.report;
    assert_eq!(report.status, RunStatus::Cancelled);
    assert_eq!(report.rows, 10, "the commit landed");
    agrees(&told, report);
    let cut = told.kinds().into_iter().find_map(|kind| match kind {
        EventKind::Committed { phases, acks, .. } => Some((phases.ack, acks)),
        _ => None,
    });
    let (ack, acks) = cut.expect("the commit is told");
    // The stop came five seconds after the report was seen, which polling sees within 10 ms.
    let ran = Duration::from_secs(5)..=Duration::from_millis(5_010);
    assert!(ran.contains(&ack), "its report ran until the stop: {ack:?}");
    assert_eq!(acks, rdlt_engine::Acks::default());
}

#[tokio::test(start_paused = true)]
async fn an_observer_that_panics_at_a_commit_fails_its_attempt_not_the_caller_awaiting_the_run() {
    let observer = |event: &Event| {
        assert!(
            !matches!(event.kind, EventKind::Committed { .. }),
            "the observer fails at a commit"
        );
    };
    let engine = observed_engine(commit_every(10), Arc::new(observer));
    let source = generator(&[("events", 20, 2, 10)]).await;
    let outcome = engine
        .run(
            pipeline("events_panic", [stream("events")]),
            source,
            memory("events_panic").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the attempt's failure ends the run");
    assert!(error.to_string().contains("panicked"), "{error}");
}

#[tokio::test(start_paused = true)]
async fn a_commit_answered_transiently_is_told_retried_in_place_before_it_is_told_committed() {
    let rule: Arc<Rule> = Arc::new(|tried, _: &CommitMeta| {
        if tried == 1 {
            Fault::Before(Failure::Transient)
        } else {
            Fault::None
        }
    });
    let (engine, told) = told_engine(commit_every(10));
    let destination = failing_commits(memory("events_retried").await, rule);
    let source = generator(&[("events", 10, 1, 10)]).await;
    let outcome = engine
        .run(
            pipeline("events_retried", [stream("events")]),
            source,
            destination,
        )
        .await;
    let report = &outcome.report;
    assert_eq!(report.status, RunStatus::Succeeded, "{:?}", outcome.error);
    assert_eq!((report.attempted, report.counters.commits.retried), (1, 1));
    agrees(&told, report);
    let events = told.events();
    let retried = events
        .iter()
        .position(|event| {
            matches!(
                event.kind,
                EventKind::Retrying {
                    scope: Scope::Commit,
                    ..
                }
            )
        })
        .expect("the commit is told retried");
    let EventKind::Retrying {
        error, retry_in, ..
    } = &events[retried].kind
    else {
        unreachable!("found as a retry");
    };
    assert_eq!((error.origin, error.retryable), (Origin::Destination, true));
    assert_eq!(events[retried].attempt, 1);
    let committed = events
        .iter()
        .position(|event| matches!(event.kind, EventKind::Committed { seq: 1, .. }))
        .expect("the commit lands");
    assert!(
        retried < committed,
        "the retry is told before the commit lands"
    );
    let waited = events[committed].at.duration_since(events[retried].at);
    assert!(
        waited.as_ref().is_ok_and(|waited| *waited >= *retry_in),
        "the commit is sent again once its wait passed: {waited:?}, {retry_in:?}"
    );
}
