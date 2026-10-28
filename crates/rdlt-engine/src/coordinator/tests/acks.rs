//! What a source is told is committed, against the commits that had landed when it was told,
//! whatever order partitions seal and end in and whichever reports fail.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

use proptest::prelude::*;
use rdlt_connector::{Cursor, PartitionId, PartitionState, StateChange, StateEntry};

use super::{Answer, Harness, Setup, cursor, ended, name, partition, position, stream, until};
use crate::config::CommitPolicy;
use crate::coordinator::delta::Collected;
use crate::coordinator::{Coordinator, Phases, Template};
use crate::error::ErrorKind;
use crate::partition::Progress;
use crate::plan::WriteMode;
use crate::report::{Acks, Trigger};

/// The most partitions a case reads.
const PARTITIONS: usize = 3;

/// What a partition sends the coordinator next.
#[derive(Clone, Debug)]
enum Step {
    /// Partition `.0` seals a segment of `.1` rows at its next cursor.
    Sealed(usize, u64),
    /// Partition `.0` seals its last segment, of `.1` rows, done.
    Done(usize, u64),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (0..PARTITIONS, 0_u64..=600).prop_map(|(at, rows)| Step::Sealed(at, rows)),
        1 => (0..PARTITIONS, 0_u64..=600).prop_map(|(at, rows)| Step::Done(at, rows)),
    ]
}

/// How the source answers a report.
fn answer() -> impl Strategy<Value = Answer> {
    prop_oneof![
        3 => Just(Answer::Accept),
        2 => Just(Answer::Refuse),
        1 => Just(Answer::Unsent),
    ]
}

/// The partition `change` records, and where it records it.
fn recorded(change: &StateChange) -> Option<(PartitionId, PartitionState)> {
    let StateChange::Put(record) = change else {
        return None;
    };
    match StateEntry::from_record(record) {
        Ok(StateEntry::Partition {
            partition, state, ..
        }) => Some((partition, state)),
        _ => None,
    }
}

/// Where the first `landed` of `commits` leave partition `id`, none where they record nothing
/// of it.
fn committed(
    commits: &[rdlt_connector::CommitMeta],
    landed: usize,
    id: &PartitionId,
) -> Option<PartitionState> {
    commits[..landed]
        .iter()
        .flat_map(|commit| commit.state_delta.iter().filter_map(recorded))
        .filter(|(partition, _)| partition == id)
        .map(|(_, state)| state)
        .last()
}

/// A runtime on the paused clock, for a property's cases.
fn paused() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a runtime")
}

/// What a case's coordinator did: how it ended, its commits, every report made with the commits
/// landed when it was, the reports accepted, each partition's sealed cursors, and what it
/// counted of reports.
struct Case {
    ended: Result<(), crate::error::Error>,
    commits: Vec<rdlt_connector::CommitMeta>,
    asked: Vec<(Vec<(PartitionId, Cursor)>, usize)>,
    told: Vec<Vec<(PartitionId, Cursor)>>,
    sealed: Vec<Vec<Cursor>>,
    acks: Acks,
}

/// Runs `steps` through a coordinator of `partitions` partitions, its source answering its
/// reports as `answers` say, the run making `attempts` attempts.
fn run(partitions: usize, steps: Vec<Step>, answers: &[Answer], attempts: NonZeroU32) -> Case {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, partitions)],
        (0..partitions)
            .map(|at| partition(id(at).as_str(), false))
            .collect(),
    );
    setup.attempts = attempts;
    paused().block_on(async {
        let (task, harness) = setup.start().await;
        harness.answers.lock().extend(answers.iter().copied());
        let mut sealed: Vec<Vec<Cursor>> = vec![Vec::new(); partitions];
        let mut done = vec![false; partitions];
        for at in 0..partitions {
            harness.send(Progress::Started { partition: at });
        }
        for (segment, step) in (1_u64..).zip(steps) {
            let (at, rows, last) = match step {
                Step::Sealed(at, rows) => (at, rows, false),
                Step::Done(at, rows) => (at, rows, true),
            };
            if at >= partitions || done[at] {
                continue;
            }
            let state = if last {
                done[at] = true;
                PartitionState::Done
            } else {
                let next = cursor(u64::try_from(sealed[at].len()).expect("few") + 1);
                sealed[at].push(next.clone());
                PartitionState::Cursor(next)
            };
            harness.seal(at, segment, rows, state, None);
        }
        for at in 0..partitions {
            harness.end(at, false);
        }
        let ended = ended(task).await;
        let told = harness
            .acks
            .lock()
            .iter()
            .map(|(_, cursors)| cursors.clone())
            .collect();
        Case {
            ended,
            commits: harness.commits.lock().clone(),
            asked: harness.asked.lock().clone(),
            told,
            sealed,
            acks: harness.tally.counters().acks,
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(64)))]

    /// Every cursor a source is told is committed is one its partition sealed, and no later than
    /// where the commits that had landed when it was told leave the partition, a partition done
    /// being past every cursor, whichever reports fail; no report carries a cursor older than one
    /// an earlier report of its partition carried; and an attempt that ends well has told every
    /// partition at a cursor the last its commits recorded.
    #[test]
    fn a_source_is_told_only_cursors_a_landed_commit_covers(
        partitions in 1..=PARTITIONS,
        steps in proptest::collection::vec(step(), 0..16),
        answers in proptest::collection::vec(answer(), 0..16),
        attempts in 1_u32..=4,
    ) {
        let attempts = NonZeroU32::new(attempts).expect("one attempt at least");
        let case = run(partitions, steps, &answers, attempts);
        let ids: Vec<PartitionId> = (0..partitions).map(id).collect();
        let mut last: Vec<Option<usize>> = vec![None; partitions];
        for (cursors, landed) in &case.asked {
            for (id, cursor) in cursors {
                let at = ids.iter().position(|known| known == id).expect("a partition of the case");
                let told_at = case.sealed[at].iter().position(|sealed| sealed == cursor);
                prop_assert!(told_at.is_some(), "{id} was told a cursor it never sealed");
                prop_assert!(last[at] <= told_at, "{id} was told a cursor older than one it was told before");
                last[at] = told_at;
                match committed(&case.commits, *landed, id) {
                    Some(PartitionState::Cursor(standing)) => {
                        let standing_at = case.sealed[at].iter().position(|sealed| *sealed == standing);
                        prop_assert!(standing_at.is_some(), "{id}'s commit recorded a cursor it never sealed");
                        prop_assert!(told_at <= standing_at, "{id} was told a cursor ahead of its commit");
                    }
                    Some(PartitionState::Done) => {}
                    None => prop_assert!(false, "{id} was told before any commit recorded it"),
                }
            }
        }
        let refused = u64::try_from(case.asked.len() - case.told.len()).expect("few");
        prop_assert_eq!(case.acks.failed, refused);
        match &case.ended {
            Ok(()) => {
                for id in &ids {
                    let Some(PartitionState::Cursor(standing)) = committed(&case.commits, case.commits.len(), id) else {
                        continue;
                    };
                    let heard = case.told.iter().flatten().filter(|(told, _)| told == id).map(|(_, cursor)| cursor).last();
                    prop_assert_eq!(heard, Some(&standing), "{} was not told where its commits leave it", id);
                }
            }
            Err(error) => prop_assert_eq!(error.kind(), ErrorKind::Source),
        }
    }
}

#[tokio::test]
async fn a_commit_judged_for_progress_copies_no_cursor_it_records_again() {
    const CURSOR: usize = 1 << 20;
    let (mut coordinator, _harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .coordinator()
    .await;
    let standing = PartitionState::Cursor(Cursor::new(1, &vec![b'p'; CURSOR]).unwrap());
    let positions = BTreeMap::from([(0, standing.clone())]);
    coordinator.landed(&positions, false);
    // Sealed where it stands, the partition is recorded there again and moves nothing.
    let delta = vec![position("p0", standing)];
    let collected = Collected {
        segments: rdlt_connector::SegmentSet::new(),
        positions,
        reported: BTreeMap::new(),
        streams: BTreeMap::new(),
        held: Vec::new(),
    };
    let (peak, progressed) =
        crate::cost::tests::peak_alone(|| coordinator.progresses(&collected, &delta));
    assert!(!progressed);
    assert!(
        peak < 4 << 10,
        "judging the commit allocated {peak} bytes at its peak"
    );
}

/// A setup of one stream of `partitions` partitions, `p0` onward, that commits each seal.
fn committing_each_seal(partitions: usize) -> Setup {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, partitions)],
        (0..partitions)
            .map(|at| partition(id(at).as_str(), false))
            .collect(),
    );
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    setup
}

/// Partition `p<at>`.
fn id(at: usize) -> PartitionId {
    PartitionId::parse(format!("p{at}")).unwrap()
}

/// The cursors of every report made, accepted or not, in order.
fn asked(harness: &Harness) -> Vec<Vec<(PartitionId, Cursor)>> {
    let asked = harness.asked.lock();
    asked.iter().map(|(cursors, _)| cursors.clone()).collect()
}

#[tokio::test(start_paused = true)]
async fn a_report_that_fails_is_made_again_by_the_next_commit_and_counted() {
    let (task, harness) = committing_each_seal(1).start().await;
    harness.answers.lock().push_back(Answer::Refuse);
    harness.send(Progress::Started { partition: 0 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.seal(0, 2, 5, PartitionState::Cursor(cursor(10)), None);
    harness.end(0, false);
    ended(task)
        .await
        .expect("a report that failed ends nothing");
    // The second commit's cursor takes the place of the cursor owed.
    assert_eq!(
        asked(&harness),
        [vec![(id(0), cursor(5))], vec![(id(0), cursor(10))]]
    );
    assert_eq!(*harness.acks.lock(), [(name(), vec![(id(0), cursor(10))])]);
    let counters = harness.tally.counters();
    assert_eq!(counters.commits.size, 2);
    assert_eq!(
        counters.acks,
        Acks {
            failed: 1,
            dropped: 0
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_commit_with_nothing_to_publish_makes_the_reports_owed() {
    let mut setup = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    );
    setup.policy = CommitPolicy::new(Some(Duration::from_secs(10)), None, None).unwrap();
    let started = tokio::time::Instant::now();
    let (task, harness) = setup.start().await;
    harness.answers.lock().push_back(Answer::Refuse);
    harness.send(Progress::Started { partition: 0 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    // The interval's commit lands the seal and its report fails; the next has nothing to publish.
    until(|| !harness.acks.lock().is_empty()).await;
    assert!(
        started.elapsed() < Duration::from_secs(21),
        "told at {:?}, past the second interval",
        started.elapsed()
    );
    let asked = harness.asked.lock().clone();
    let only = vec![(id(0), cursor(5))];
    assert_eq!(asked, [(only.clone(), 1), (only, 1)]);
    harness.end(0, false);
    ended(task).await.unwrap();
    let counters = harness.tally.counters();
    assert_eq!((harness.commit_count(), counters.commits.interval), (1, 1));
    assert_eq!(counters.acks.failed, 1);
}

#[tokio::test(start_paused = true)]
async fn a_partition_at_a_cursor_owed_at_the_last_commit_ends_the_attempt() {
    let (task, harness) = Setup::new(
        vec![stream(WriteMode::Append, None, 1)],
        vec![partition("p0", false)],
    )
    .start()
    .await;
    harness.answers.lock().push_back(Answer::Unsent);
    harness.send(Progress::Started { partition: 0 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.end(0, false);
    let error = ended(task)
        .await
        .expect_err("the last commit's report failed");
    assert_eq!(error.kind(), ErrorKind::Source);
    assert!(error.is_retryable());
    assert_eq!(error.stream(), Some(&name()));
    // The commit landed, and counts so.
    let counters = harness.tally.counters();
    assert_eq!((harness.commit_count(), counters.commits.end), (1, 1));
    assert_eq!(counters.acks.failed, 1);
}

#[tokio::test(start_paused = true)]
async fn a_done_partition_owed_at_the_last_commit_is_given_up_and_counted() {
    let (task, harness) = committing_each_seal(1).start().await;
    harness.answers.lock().extend([Answer::Refuse; 3]);
    harness.send(Progress::Started { partition: 0 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.seal(0, 2, 5, PartitionState::Done, None);
    harness.end(0, false);
    ended(task)
        .await
        .expect("a done partition's cursor ends nothing");
    // The commit recording the partition done tells the cursor it stood at, as does the last
    // commit, which has nothing else to publish.
    assert_eq!(asked(&harness), vec![vec![(id(0), cursor(5))]; 3]);
    assert!(harness.acks.lock().is_empty());
    let acks = harness.tally.counters().acks;
    assert_eq!(
        acks,
        Acks {
            failed: 3,
            dropped: 1
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_done_partition_refused_as_never_sent_is_given_up_at_once() {
    let (task, harness) = committing_each_seal(1).start().await;
    harness
        .answers
        .lock()
        .extend([Answer::Accept, Answer::Unsent]);
    harness.send(Progress::Started { partition: 0 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.seal(0, 2, 5, PartitionState::Done, None);
    harness.end(0, false);
    ended(task).await.unwrap();
    // The last commit reports nothing: no source started again is sent the cursor.
    assert_eq!(asked(&harness).len(), 2);
    let acks = harness.tally.counters().acks;
    assert_eq!(
        acks,
        Acks {
            failed: 1,
            dropped: 1
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_partition_at_a_cursor_owed_across_the_attempts_ends_the_attempt() {
    let mut setup = committing_each_seal(1);
    setup.attempts = NonZeroU32::new(3).unwrap();
    let (task, harness) = setup.start().await;
    harness.answers.lock().extend([Answer::Refuse; 3]);
    harness.send(Progress::Started { partition: 0 });
    for segment in 1..=3 {
        harness.seal(
            0,
            segment,
            5,
            PartitionState::Cursor(cursor(segment * 5)),
            None,
        );
    }
    // The partition never ends: its third failed report in a row ends the attempt.
    let error = ended(task)
        .await
        .expect_err("three reports failed in a row");
    assert_eq!(error.kind(), ErrorKind::Source);
    assert_eq!(harness.commit_count(), 3);
    assert_eq!(harness.tally.counters().acks.failed, 3);
}

#[tokio::test(start_paused = true)]
async fn a_done_partition_owed_across_the_attempts_is_given_up_as_the_attempt_reads_on() {
    let mut setup = committing_each_seal(2);
    setup.attempts = NonZeroU32::new(2).unwrap();
    let (task, harness) = setup.start().await;
    harness.answers.lock().extend([Answer::Refuse; 2]);
    for partition in 0..2 {
        harness.send(Progress::Started { partition });
    }
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    harness.seal(0, 2, 5, PartitionState::Done, None);
    harness.seal(1, 3, 5, PartitionState::Cursor(cursor(5)), None);
    harness.end(0, false);
    harness.end(1, false);
    ended(task)
        .await
        .expect("a done partition's cursor ends nothing");
    // `p0`'s second failed report gives its cursor up, so `p1`'s report carries it no more.
    assert_eq!(
        asked(&harness),
        [
            vec![(id(0), cursor(5))],
            vec![(id(0), cursor(5))],
            vec![(id(1), cursor(5))]
        ]
    );
    let acks = harness.tally.counters().acks;
    assert_eq!(
        acks,
        Acks {
            failed: 2,
            dropped: 1
        }
    );
}

/// Hands `coordinator` everything its partitions sent so far, as its loop would.
fn heard_all(coordinator: &mut Coordinator) {
    while let Ok(progress) = coordinator.parts.progress.try_recv() {
        coordinator.observe(progress);
    }
}

#[tokio::test(start_paused = true)]
async fn a_done_partition_owed_as_its_phase_ends_is_told_by_a_later_commit_of_the_attempt() {
    let mut phased = stream(WriteMode::Append, None, 1);
    phased.phases = Some(Phases {
        phase: 1,
        reading: BTreeMap::from([(id(0), 0)]),
        committed: BTreeMap::new(),
        begun: None,
        settled: false,
        template: Template {
            table: 0,
            on_demand: false,
            changes: None,
            reset_retention: false,
        },
    });
    let mut setup = Setup::new(vec![phased], vec![partition("p0", false)]);
    setup.policy = CommitPolicy::new(None, Some(1), None).unwrap();
    let (mut coordinator, harness) = setup.coordinator().await;
    harness.answers.lock().extend([Answer::Refuse; 2]);
    harness.send(Progress::Started { partition: 0 });
    harness.seal(0, 1, 5, PartitionState::Cursor(cursor(5)), None);
    heard_all(&mut coordinator);
    coordinator.commit(Trigger::Size).await.unwrap();
    harness.seal(0, 2, 5, PartitionState::Done, None);
    harness.end(0, false);
    heard_all(&mut coordinator);
    // The phase has ended and another may follow, so the commit of its end is not the
    // attempt's last: the done partition's cursor stays owed.
    coordinator
        .commit(Trigger::End)
        .await
        .expect("a commit another phase may follow ends nothing");
    coordinator.commit(Trigger::Interval).await.unwrap();
    assert_eq!(asked(&harness), vec![vec![(id(0), cursor(5))]; 3]);
    assert_eq!(*harness.acks.lock(), [(name(), vec![(id(0), cursor(5))])]);
    let acks = harness.tally.counters().acks;
    assert_eq!(
        acks,
        Acks {
            failed: 2,
            dropped: 0
        }
    );
}
