//! What the coordinator tells of and notes for a run's status: each commit that lands, the
//! partitions it forgot, the reads a source reset, and where each partition stands.

use std::collections::BTreeMap;
use std::time::Duration;

use rdlt_connector::{PartitionId, PartitionState};

use super::{Setup, cursor, ended, name, partition, stream, until};
use crate::event::{EventKind, Listed};
use crate::limits::REPORTED_LISTED;
use crate::partition::Progress;
use crate::plan::WriteMode;
use crate::report::{Acks, CommitPhases, Trigger};
use crate::status::{Partitions, Phase, Step};

fn one_partition() -> Setup {
    Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
}

#[tokio::test(start_paused = true)]
async fn each_commit_that_lands_is_told_with_its_counts_its_phases_and_its_report() {
    let mut setup = one_partition();
    setup.pace = Duration::from_secs(1);
    let (task, harness) = setup.start().await;
    harness.send(Progress::Started { partition: 0 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.end(0, false);
    ended(task).await.unwrap();
    let told = harness.told.lock().clone();
    let [event] = told.as_slice() else {
        panic!("one commit is told: {told:?}");
    };
    let EventKind::Committed {
        seq,
        trigger,
        rows,
        bytes,
        took,
        phases,
        streams,
        acks,
    } = &event.kind
    else {
        panic!("a commit is told: {event:?}");
    };
    let log = harness.log.lock();
    assert_eq!((*seq, *trigger), (1, Trigger::End));
    assert_eq!((*rows, *bytes), (log.committed.rows, log.committed.bytes));
    let paced = CommitPhases {
        flush: Duration::ZERO,
        commit: Duration::from_secs(1),
        ack: Duration::from_secs(2),
    };
    assert_eq!((*phases, *took), (paced, Duration::from_secs(3)));
    assert_eq!(streams[&name().to_string()], log.committed.streams[&name()]);
    assert_eq!(*acks, Acks::default());
    assert_eq!(event.attempt, 1);
}

#[tokio::test(start_paused = true)]
async fn a_commit_whose_attempt_ends_while_it_is_reported_is_left_for_the_run_to_tell() {
    let mut setup = one_partition();
    setup.pace = Duration::from_secs(1);
    let (task, harness) = setup.start().await;
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.end(0, false);
    until(|| harness.commit_count() == 1).await;
    // The commit landed; its report to the source takes two seconds more.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        harness.told.lock().is_empty(),
        "the coordinator told nothing"
    );
    let log = harness.log.lock();
    let unreported = log
        .unreported
        .clone()
        .expect("the commit is left in the log");
    assert_eq!(unreported.rows, log.committed.rows);
    let EventKind::Committed { rows, acks, .. } =
        unreported.cut_short(tokio::time::Instant::now().into_std())
    else {
        panic!("a commit is told as one");
    };
    assert_eq!((rows, acks), (log.committed.rows, Acks::default()));
}

#[tokio::test(start_paused = true)]
async fn forgotten_partitions_are_told_by_stream_the_first_named_and_the_rest_counted() {
    let (coordinator, harness) = one_partition().coordinator().await;
    let ids: Vec<PartitionId> = (0..REPORTED_LISTED + 2)
        .map(|index| PartitionId::parse(format!("d{index}")).unwrap())
        .collect();
    coordinator.forgot(BTreeMap::from([(name(), ids.clone())]));
    let told: Vec<EventKind> = harness
        .told
        .lock()
        .iter()
        .map(|event| event.kind.clone())
        .collect();
    let partitions = Listed::first(ids.iter().map(ToString::to_string));
    assert_eq!(partitions.unlisted, 2);
    assert_eq!(
        told,
        [EventKind::Forgotten {
            stream: name().to_string(),
            partitions,
        }]
    );
    assert_eq!(harness.log.lock().forgotten[&name()].partitions, ids[2..]);
}

#[tokio::test(start_paused = true)]
async fn a_read_its_source_reset_is_counted_and_told() {
    let (mut coordinator, harness) = one_partition().coordinator().await;
    coordinator.observe(Progress::RetentionReset { partition: 0 });
    let told: Vec<EventKind> = harness
        .told
        .lock()
        .iter()
        .map(|event| event.kind.clone())
        .collect();
    let reset = EventKind::RetentionReset {
        stream: name().to_string(),
        partition: "p0".to_owned(),
    };
    assert_eq!(told, [reset]);
    assert_eq!(harness.log.lock().retention_resets[&name()], 1);
}

#[tokio::test(start_paused = true)]
async fn a_partition_stands_waiting_until_it_starts_then_reading_until_it_ends() {
    let partitions = vec![partition("p0", false), partition("p1", true)];
    let (mut coordinator, _harness) =
        Setup::new(vec![stream(WriteMode::Append, None, 2)], partitions)
            .coordinator()
            .await;
    let standing =
        |coordinator: &crate::coordinator::Coordinator| coordinator.parts.live.status().partitions;
    let at = |reading, waiting, ended| Partitions {
        reading,
        waiting,
        ended,
    };
    assert_eq!(standing(&coordinator), at(0, 2, 0));
    coordinator.observe(Progress::Started { partition: 0 });
    tokio::time::advance(Duration::from_secs(5)).await;
    coordinator.observe(Progress::Started { partition: 1 });
    assert_eq!(standing(&coordinator), at(2, 0, 0));
    let status = coordinator.parts.live.status();
    assert_eq!(
        status.quietest.map(|quiet| quiet.partition),
        Some("p0".to_owned())
    );
    // Rows written tell nothing of a partition's quiet: they come as often as it writes.
    tokio::time::advance(Duration::from_secs(5)).await;
    coordinator.observe(Progress::Written {
        partition: 1,
        rows: 1,
        bytes: 8,
    });
    coordinator.observe(Progress::Signalled { partition: 0 });
    let status = coordinator.parts.live.status();
    assert_eq!(
        status.quietest.map(|quiet| quiet.partition),
        Some("p1".to_owned())
    );
    coordinator.observe(Progress::Ended {
        partition: 0,
        stopped: false,
    });
    assert_eq!(standing(&coordinator), at(1, 0, 1));
    // A signal queued before its end leaves an ended partition ended.
    coordinator.observe(Progress::Signalled { partition: 0 });
    assert_eq!(standing(&coordinator), at(1, 0, 1));
}

#[tokio::test(start_paused = true)]
async fn a_commit_shows_each_of_its_steps_in_turn_since_the_step_began() {
    let mut setup = one_partition();
    setup.pace = Duration::from_secs(1);
    let (coordinator, harness) = setup.coordinator().await;
    let live = std::sync::Arc::clone(&coordinator.parts.live);
    assert_eq!(live.status().phase, Phase::Opening);
    let task = tokio::spawn(coordinator.run());
    until(|| live.status().phase == Phase::Reading).await;
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.end(0, false);
    let committing = |step, tries| Phase::Committing {
        seq: 1,
        step,
        tries,
    };
    until(|| live.status().phase == committing(Step::Commit, 1)).await;
    let commit = live.status();
    tokio::time::advance(Duration::from_millis(500)).await;
    assert_eq!(live.status().since, commit.since, "the step began once");
    until(|| live.status().phase == committing(Step::Ack, 1)).await;
    let ack = live.status();
    assert_eq!(
        ack.since,
        commit.since + Duration::from_secs(1),
        "the commit took a second"
    );
    ended(task).await.unwrap();
    assert_eq!(live.status().phase, Phase::Reading);
}

#[tokio::test(start_paused = true)]
async fn between_commits_an_attempt_shows_reading_until_it_is_asked_to_stop() {
    let (mut coordinator, _harness) = one_partition().coordinator().await;
    let live = std::sync::Arc::clone(&coordinator.parts.live);
    coordinator.step(None, coordinator.parts.env.instant());
    assert_eq!(live.status().phase, Phase::Reading);
    coordinator.stopping = true;
    coordinator.step(None, coordinator.parts.env.instant());
    assert_eq!(live.status().phase, Phase::Stopping);
}
