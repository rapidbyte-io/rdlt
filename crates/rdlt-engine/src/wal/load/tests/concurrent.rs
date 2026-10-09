#![expect(
    clippy::disallowed_methods,
    reason = "tests drive partitions and a coordinator as tasks of their own"
)]

//! Room in a full log with partitions and a coordinator running at once, as an attempt runs
//! them: a commit takes what was sealed while the last one landed, its receipt comes late, and
//! no partition is starved of room while others checkpoint.
//!
//! A partition seals a segment only once the commit of its last seal has its receipt, so it
//! holds a segment open and at most one sealed, as [`gap`] sizes a load for, however late the
//! coordinator takes what was sealed.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use rdlt_connector::{CommitSeq, SegmentId};
use tokio::sync::{mpsc, oneshot};

use super::room::{Load, loads, started, stored};
use super::{logged, receipt, sealed_at};
use crate::budget::MemoryBudget;
use crate::env::{Clock, SystemClock};
use crate::table::testing::view;
use crate::wal::load::LoadLog;
use crate::wal::memory::MemoryWal;

/// How long each commit's frame waits after its seals, and its receipt after it, as `(the
/// first commit's, every later one's)`; how long the coordinator takes to begin the first; and
/// the turns of the scheduler the first partition gives up after each batch beyond the others'.
#[derive(Clone, Copy, Debug)]
struct Landing {
    sealed: (Duration, Duration),
    landed: (Duration, Duration),
    late: Duration,
    lagging: usize,
}

impl Landing {
    /// Receipts coming `first` and `rest` milliseconds after their commits, each commit's frame
    /// following its seals at once.
    fn after(first: u64, rest: u64) -> Self {
        Self {
            sealed: (Duration::ZERO, Duration::ZERO),
            landed: (Duration::from_millis(first), Duration::from_millis(rest)),
            late: Duration::ZERO,
            lagging: 0,
        }
    }

    /// As `self`, the coordinator beginning the first commit `late` milliseconds after the first
    /// seal comes, and the first partition giving up `lagging` more turns after each batch.
    fn late(self, late: u64, lagging: usize) -> Self {
        Self {
            late: Duration::from_millis(late),
            lagging,
            ..self
        }
    }

    /// As `self`, each commit's frame following its seals `first` and `rest` milliseconds after.
    fn sealed(self, first: u64, rest: u64) -> Self {
        Self {
            sealed: (Duration::from_millis(first), Duration::from_millis(rest)),
            ..self
        }
    }
}

/// Batches all partitions log at most before the first has sealed its rounds: past them it
/// counts as starved of room.
const STARVED: u64 = 20_000;

/// A segment sealed, and what answers once the commit taking it has its receipt.
type Seal = (u64, oneshot::Sender<()>);

/// Runs `load`'s partitions at once through a log of `limit` bytes, until the first has sealed
/// `rounds` segments, beside a coordinator committing every seal it holds: the error a partition
/// or commit failed with.
///
/// Each receipt comes as `landing` says; then a last commit lands, and the log holds what its
/// store holds.
async fn concurrent(
    limit: u64,
    load: &Load,
    rounds: u64,
    landing: Landing,
) -> Result<(), crate::Error> {
    let store = Arc::new(MemoryWal::default());
    let budget = crate::budget::budget(64 << 20);
    let (log, task) = started(&store, limit);
    let writer = tokio::spawn(task);
    let (seals, sealed) = mpsc::unbounded_channel();
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicU64::new(u64::try_from(load.gaps.len()).expect("few")));
    let total = Arc::new(AtomicU64::new(0));
    let mut partitions = Vec::new();
    for (index, gap) in load.gaps.iter().copied().enumerate() {
        let shared = (
            log.clone(),
            budget.clone(),
            seals.clone(),
            Arc::clone(&stop),
        );
        let (next, total, load) = (Arc::clone(&next), Arc::clone(&total), load.clone());
        let lagging = if index == 0 { landing.lagging } else { 0 };
        partitions.push(tokio::spawn(async move {
            partition(shared, (index, gap, rounds, lagging), (next, total), &load).await
        }));
    }
    drop(seals);
    let coordinator = tokio::spawn(coordinate(
        log.clone(),
        budget.clone(),
        sealed,
        landing,
        load.clone(),
    ));
    let mut result = Ok(());
    for partition in partitions {
        let ended = partition.await.expect("a partition ends");
        if result.is_ok() {
            result = ended;
        }
    }
    let seq = coordinator.await.expect("the coordinator ends");
    let result = result.and(seq.map(drop));
    drop(log);
    writer
        .await
        .expect("the writer ends")
        .expect("the writer ends well");
    let _ = stored(&store);
    result
}

/// Logs the partition `index`'s batches, sealing a segment each `gap` of them once the commit of
/// its last seal has its receipt, until it is stopped, or as the first, has sealed `rounds`
/// segments, giving up `lagging` more turns of the scheduler after each batch than one.
async fn partition(
    (log, budget, seals, stop): (
        LoadLog,
        MemoryBudget,
        mpsc::UnboundedSender<Seal>,
        Arc<AtomicBool>,
    ),
    (index, gap, rounds, lagging): (usize, u64, u64, usize),
    (next, total): (Arc<AtomicU64>, Arc<AtomicU64>),
    load: &Load,
) -> Result<(), crate::Error> {
    let orders = view("orders");
    let mut segment = u64::try_from(index).expect("few");
    let mut done = segment * load.stagger % load.gaps[0];
    let mut sealings = 0;
    let mut from = i64::try_from(index).expect("few") << 40;
    let mut received: Option<oneshot::Receiver<()>> = None;
    loop {
        if stop.load(Ordering::SeqCst) {
            if index == 0 {
                return Err(crate::Error::internal(
                    "the first partition was starved of room",
                ));
            }
            return Ok(());
        }
        if total.fetch_add(1, Ordering::SeqCst) > STARVED {
            stop.store(true, Ordering::SeqCst);
        }
        let batch = load.batch_of(index, from);
        from += 1_000;
        logged(&log, &budget, 0, &orders, SegmentId(segment), &batch).await?;
        for _ in 0..=lagging {
            tokio::task::yield_now().await;
        }
        done += 1;
        if done < gap {
            continue;
        }
        if let Some(received) = received.take()
            && received.await.is_err()
        {
            // The coordinator ended, as a commit failed: its error is the load's.
            return Ok(());
        }
        log.checkpointed();
        let (answer, answered) = oneshot::channel();
        seals
            .send((segment, answer))
            .expect("the coordinator listens");
        received = Some(answered);
        segment = next.fetch_add(1, Ordering::SeqCst);
        done = 0;
        sealings += u64::from(index == 0);
        if sealings == rounds {
            stop.store(true, Ordering::SeqCst);
            return Ok(());
        }
    }
}

/// Commits every seal sent, as a coordinator does, once one comes or a batch finds the log full,
/// each receipt coming as `landing` says and answering the seals it took, then a last commit:
/// the next commit's number.
async fn coordinate(
    log: LoadLog,
    budget: MemoryBudget,
    mut sealed: mpsc::UnboundedReceiver<Seal>,
    landing: Landing,
    load: Load,
) -> Result<CommitSeq, crate::Error> {
    let mut seq = CommitSeq::FIRST;
    loop {
        let mut taken = Vec::new();
        tokio::select! {
            biased;
            got = sealed.recv() => match got {
                Some(seal) => taken.push(seal),
                None => break,
            },
            () = log.full() => {}
        }
        if seq == CommitSeq::FIRST {
            SystemClock.sleep(landing.late).await;
        }
        while let Ok(seal) = sealed.try_recv() {
            taken.push(seal);
        }
        if taken.is_empty() {
            continue;
        }
        let segments: Vec<u64> = taken.iter().map(|(segment, _)| *segment).collect();
        let commit = load.commit(&segments, seq);
        let committing = log.committing();
        log.took(segments.len());
        let seals = segments.iter().copied().map(sealed_at).collect();
        let (sealed_for, landed) = if seq == CommitSeq::FIRST {
            (landing.sealed.0, landing.landed.0)
        } else {
            (landing.sealed.1, landing.landed.1)
        };
        let seals = log.seals(&budget, seals, &commit).await?;
        SystemClock.sleep(sealed_for).await;
        log.finish(&budget, seals, Vec::new(), &commit, 0).await?;
        SystemClock.sleep(landed).await;
        log.committed(&receipt(seq)).await?;
        drop(committing);
        for (_, answer) in taken {
            answer.send(()).ok();
        }
        seq = seq.next();
    }
    Ok(seq)
}

/// The first partition's gap at which `load`'s partitions hold at most three quarters of a log
/// of `limit` bytes in frames not yet committed and received, each a segment open and one sealed
/// and waiting for its receipt, beside a commit of a seal of each.
fn gap(load: &Load, limit: u64) -> u64 {
    let others: u64 = load.gaps[1..].iter().sum();
    let seals = u64::try_from(load.gaps.len()).expect("few");
    let commit = load.committed() + seals * 1_024;
    let room = (limit * 3 / 4).saturating_sub(commit) / load.frame() / 2;
    room.saturating_sub(others).max(1)
}

async fn every_load_completes(landings: &[Landing]) {
    let loads = loads().into_iter().map(|mut load| {
        load.gaps[0] = gap(&load, 60 * load.frame());
        load
    });
    completes(loads, landings).await;
}

/// Runs each of `loads` through a log of 60 of its frames, as each of `landings` says.
async fn completes(loads: impl IntoIterator<Item = Load>, landings: &[Landing]) {
    for load in loads {
        let limit = 60 * load.frame();
        for landing in landings {
            let loaded = concurrent(limit, &load, 3, *landing).await;
            assert!(
                loaded.is_ok(),
                "{load:?} of {limit}, {landing:?}: {loaded:?}"
            );
        }
    }
}

/// Two partitions of large frames, the first's checkpoints half and three fifths of a log of
/// 60 frames apart, the second's after each batch.
fn far_apart() -> Vec<Load> {
    let mut loads = Vec::new();
    for gap in [30, 36] {
        for (recorded, stagger) in [(0, 0), (0, 3), (1_500, 0)] {
            let mut load = Load::new(&[gap, 1]);
            load.rows = 400;
            load.recorded = recorded;
            load.stagger = stagger;
            loads.push(load);
        }
    }
    loads
}

#[tokio::test(start_paused = true)]
async fn partitions_running_at_once_load_three_quarters_of_a_log_whenever_receipts_come() {
    every_load_completes(&[
        Landing::after(0, 0),
        Landing::after(1, 1),
        Landing::after(50, 50),
        Landing::after(2_000, 0),
    ])
    .await;
}

#[tokio::test(start_paused = true)]
async fn partitions_running_at_once_load_three_quarters_of_a_log_however_long_a_commit_s_frame_waits()
 {
    let mut landings = Vec::new();
    for landing in [Landing::after(0, 0), Landing::after(50, 50)] {
        for (first, rest) in [(2_000, 0), (20, 20), (500, 500)] {
            landings.push(landing.sealed(first, rest));
        }
    }
    every_load_completes(&landings).await;
    completes(far_apart(), &landings).await;
}

#[tokio::test(start_paused = true)]
async fn partitions_running_at_once_load_three_quarters_of_a_log_however_late_the_first_commit_begins()
 {
    // While the coordinator is late, the others seal segment after segment and the first, at a
    // third of their pace, leaves its open frames in every chunk they fill.
    let mut landings = Vec::new();
    for landing in [Landing::after(0, 0), Landing::after(50, 50)] {
        for (late, lagging) in [(100, 0), (100, 2), (2_000, 2)] {
            landings.push(landing.late(late, lagging));
        }
    }
    every_load_completes(&landings).await;
}

/// The partitions, the coordinator and the log's writer run on threads of their own, as the system
/// schedules them; the load stays within what `gap` sizes it for whatever the schedule, as each
/// partition waits for its last seal's receipt before it seals again.
///
/// A partition checkpointing every batch and free to seal on seals some thirty segments while
/// the system holds the coordinator back. Their frames and the seals of the commit taking them
/// all, beside the open frames of a partition the system runs slower, fill the log past three
/// quarters and leave no room to carry those open frames out of the chunks the commit settles:
/// the engine refuses the next batch, as it may refuse a load past three quarters of its log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitions_running_at_once_on_the_real_clock_load_three_quarters_of_a_log() {
    every_load_completes(&[
        Landing::after(0, 0),
        Landing::after(1, 1),
        Landing::after(0, 0).sealed(5, 1),
        Landing::after(1, 1).sealed(5, 1),
    ])
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_partition_beside_many_checkpointing_every_batch_is_not_starved_of_room() {
    // Seven partitions seal after every batch of three rows and keep the log full; the first's
    // smaller batches, waiting for room, take it before batches that come after them.
    for (gap, recorded, stagger) in [(20, 0, 0), (20, 0, 3), (16, 1_500, 0), (16, 1_500, 3)] {
        let mut load = Load::new(&[gap, 1, 1, 1, 1, 1, 1, 1]);
        load.first_rows = Some(1);
        load.recorded = recorded;
        load.stagger = stagger;
        let limit = 60 * load.frame();
        let loaded = concurrent(limit, &load, 3, Landing::after(0, 0)).await;
        assert!(loaded.is_ok(), "{load:?}: {loaded:?}");
    }
}
