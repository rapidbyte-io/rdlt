//! Room in a full log: partitions whose segments span many chunks, checkpoints far apart, small
//! batches beside large commits, load through a log that holds what they leave unsealed, and the
//! log never holds more than it may.

use std::num::NonZeroU64;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{CommitSeq, SegmentId, StateChange, StateRecord};

use super::{load, logged, meta, pipeline, receipt, sealed_at};
use crate::budget::MemoryBudget;
use crate::table::testing::view;
use crate::wal::frame::{self, Frame};
use crate::wal::load::{LoadLog, Owner};
use crate::wal::memory::MemoryWal;
use crate::wal::store::WalStore;

/// A load as the tests drive one.
#[derive(Clone, Debug)]
pub(super) struct Load {
    /// Each partition's batches between its checkpoints.
    pub(super) gaps: Vec<u64>,
    /// The rows of each batch.
    pub(super) rows: usize,
    /// The rows of each batch of the first partition, where they are not `rows`.
    pub(super) first_rows: Option<usize>,
    /// Bytes each commit records of state beside its segments.
    pub(super) recorded: usize,
    /// Batches the first segment of each partition but the first is short of its gap, times
    /// its index.
    pub(super) stagger: u64,
}

impl Load {
    pub(super) fn new(gaps: &[u64]) -> Self {
        Self {
            gaps: gaps.to_vec(),
            rows: 3,
            first_rows: None,
            recorded: 0,
            stagger: 1,
        }
    }

    /// A batch of the load's rows, its ids from `from`.
    pub(super) fn batch(&self, from: i64) -> RecordBatch {
        self.batch_of(1, from)
    }

    /// A batch of partition `partition`'s rows, its ids from `from`.
    pub(super) fn batch_of(&self, partition: usize, from: i64) -> RecordBatch {
        let rows = match (partition, self.first_rows) {
            (0, Some(rows)) => rows,
            _ => self.rows,
        };
        let count = i64::try_from(rows).expect("few rows");
        let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(from..from + count));
        RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch")
    }

    /// Bytes: what the frame of one of the load's batches takes.
    pub(super) fn frame(&self) -> u64 {
        let batch = frame::Batch {
            segment: SegmentId(u64::MAX),
            table: 0,
            ordinal: u64::MAX,
            batch: self.batch(i64::MAX - 1_000),
        };
        length(Frame::Batch(batch).encode().expect("encodes").len())
    }

    /// The commit of `segments` as `seq`, recording the load's bytes of state.
    pub(super) fn commit(&self, segments: &[u64], seq: CommitSeq) -> rdlt_connector::CommitMeta {
        let mut commit = meta(segments);
        commit.commit_seq = seq;
        if self.recorded > 0 {
            commit.state_delta = vec![StateChange::Put(StateRecord {
                key: "k".to_owned(),
                value: vec![7; self.recorded].into(),
            })];
        }
        commit
    }

    /// Bytes: what one of the load's commits writes, its seal and its commit frame.
    pub(super) fn committed(&self) -> u64 {
        let commit = self.commit(&[u64::MAX], CommitSeq::FIRST);
        let frame = frame::commit(&commit, 1, 0).expect("encodes");
        // A seal frame of the tests takes well under a KiB.
        length(frame.len()) + 1_024
    }

    /// Bytes: the most the load's partitions hold of frames they have not sealed.
    fn open(&self) -> u64 {
        self.gaps.iter().sum::<u64>() * self.frame()
    }
}

fn length(bytes: usize) -> u64 {
    u64::try_from(bytes).expect("a length")
}

/// Seals `segment` and commits it as `seq`, its receipt following, as a coordinator does.
async fn sealed(
    log: &LoadLog,
    budget: &MemoryBudget,
    commit: &rdlt_connector::CommitMeta,
) -> Result<(), crate::Error> {
    log.checkpointed();
    let committing = log.committing();
    log.took(1);
    let segment = commit.segments.iter().next().expect("a segment").0;
    log.commit(budget, vec![sealed_at(segment)], Vec::new(), commit, 0)
        .await?;
    log.committed(&receipt(commit.commit_seq)).await?;
    drop(committing);
    Ok(())
}

/// Bytes: what `store` holds of the load's log.
pub(super) fn stored(store: &MemoryWal) -> u64 {
    store
        .stored(&pipeline())
        .iter()
        .map(|(_, bytes)| length(bytes.len()))
        .sum()
}

/// The log of the tests' load in `store`, of `limit` bytes, and its writer's task.
pub(super) fn started(
    store: &Arc<MemoryWal>,
    limit: u64,
) -> (
    LoadLog,
    impl Future<Output = Result<(), crate::Error>> + Send + 'static,
) {
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = Arc::clone(store) as Arc<dyn WalStore>;
    let owner = Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: rdlt_connector::Epoch(1),
        opened: None,
        origin: load(),
    };
    let bound = NonZeroU64::new(limit).expect("a limit");
    LoadLog::start(wal, owner, bound, None)
}

/// Checks neither what `log` counts nor what `store` holds passes `most` bytes.
fn bounded(store: &MemoryWal, log: &LoadLog, most: u64) {
    assert!(log.held() <= most, "{} counted of {most}", log.held());
    assert!(stored(store) <= most, "{} stored of {most}", stored(store));
}

/// Logs `load`'s partitions' batches in turn, each sealing its segment after its gap, through a
/// log of `limit` bytes, a commit taking each seal and its receipt following, until the first
/// partition has sealed `rounds` segments: the error a batch or commit was refused with.
///
/// After every step neither what the log counts nor what its store holds passes `limit`, but by
/// the frames of a commit larger than a quarter of it, and what ends a chunk.
async fn interleaved(limit: u64, load: &Load, rounds: u64) -> Result<(), crate::Error> {
    let store = Arc::new(MemoryWal::default());
    let budget = MemoryBudget::new(64 << 20);
    let (log, task) = started(&store, limit);
    let orders = view("orders");
    let past = if load.committed() > limit / 4 {
        load.committed() + 2_048
    } else {
        0
    };
    let bounded = |log: &LoadLog| bounded(&store, log, limit + past);
    let written = async {
        let partitions = u64::try_from(load.gaps.len()).expect("few partitions");
        // Each partition's segment and the batches logged of it.
        let mut open: Vec<(u64, u64)> = (0..partitions)
            .map(|partition| (partition, partition * load.stagger % load.gaps[0]))
            .collect();
        let (mut next, mut seq, mut sealings, mut from) = (partitions, CommitSeq::FIRST, 0, 0);
        let result = 'load: loop {
            for (index, (slot, gap)) in open.iter_mut().zip(&load.gaps).enumerate() {
                let segment = SegmentId(slot.0);
                let batch = load.batch(from);
                from += 1_000;
                if let Err(error) = logged(&log, &budget, 0, &orders, segment, &batch).await {
                    break 'load Err(error);
                }
                bounded(&log);
                slot.1 += 1;
                if slot.1 < *gap {
                    continue;
                }
                let commit = load.commit(&[slot.0], seq);
                if let Err(error) = sealed(&log, &budget, &commit).await {
                    break 'load Err(error);
                }
                bounded(&log);
                seq = seq.next();
                *slot = (next, 0);
                next += 1;
                sealings += u64::from(index == 0);
                if sealings == rounds {
                    break 'load Ok(());
                }
            }
        };
        drop(log);
        result
    };
    let (ended, result) = tokio::join!(task, written);
    ended.expect("the writer ends");
    result
}

/// The loads the tests drive, each partition's gap but the first's: one partition alone, one
/// beside others committing after every batch, several of unequal gaps; small batches beside
/// large commits and large batches beside small ones; partitions begun in step or apart.
pub(super) fn loads() -> Vec<Load> {
    let mut loads = Vec::new();
    for others in [&[][..], &[1], &[1, 1, 1, 1], &[2, 3], &[5, 1]] {
        for (rows, recorded) in [(1, 0), (3, 1_500), (3, 3_000), (40, 0)] {
            for stagger in [0, 1, 3] {
                let mut gaps = vec![1];
                gaps.extend_from_slice(others);
                loads.push(Load {
                    gaps,
                    rows,
                    first_rows: None,
                    recorded,
                    stagger,
                });
            }
        }
    }
    loads
}

/// The first partition's gap at which `load`'s partitions hold at most `share` of a log of
/// `limit` bytes unsealed, beside a commit's frames.
fn gap(load: &Load, limit: u64, share: (u64, u64)) -> u64 {
    let others: u64 = load.gaps[1..].iter().sum();
    let room = (limit * share.0 / share.1).saturating_sub(load.committed());
    (room / load.frame()).saturating_sub(others).max(1)
}

#[tokio::test]
async fn a_load_whose_partitions_hold_three_quarters_of_its_log_unsealed_completes() {
    for mut load in loads() {
        let limit = 60 * load.frame();
        load.gaps[0] = gap(&load, limit, (3, 4));
        assert!(load.open() + load.committed() <= limit * 3 / 4, "{load:?}");
        let loaded = interleaved(limit, &load, 3).await;
        assert!(loaded.is_ok(), "{load:?} of {limit}: {loaded:?}");
    }
}

#[tokio::test]
async fn a_load_whose_partitions_hold_more_unsealed_than_its_log_is_refused_for_good() {
    for mut load in loads() {
        let limit = 40 * load.frame();
        load.gaps[0] = 2 * limit / load.frame();
        let refused = interleaved(limit, &load, 1).await.expect_err("refused");
        assert_eq!(refused.code(), Some("log_bytes_exceeded"), "{load:?}");
        assert!(!refused.is_retryable());
    }
}

#[tokio::test]
async fn a_partition_beside_one_committing_every_batch_loads_three_quarters_of_a_large_log() {
    // Each commit's chunk holds one frame of each partition: the first's open frames are spread
    // over as many chunks as it has, which carries gather.
    let mut load = Load::new(&[1, 1]);
    let limit = 1_000 * load.frame();
    load.gaps[0] = gap(&load, limit, (3, 4));
    let loaded = interleaved(limit, &load, 2).await;
    assert!(loaded.is_ok(), "{load:?} of {limit}: {loaded:?}");
}

#[tokio::test]
async fn a_partition_beside_one_committing_every_batch_in_a_small_log_of_large_commits_loads() {
    // Each commit's chunk holds a frame of each partition beside commit frames twice as large:
    // gathering the first's open frames out of them takes room a commit alone does not leave.
    for stagger in [0, 1] {
        let mut load = Load {
            gaps: vec![1, 1],
            rows: 3,
            first_rows: None,
            recorded: 1_000,
            stagger,
        };
        let limit = 24 * load.frame();
        load.gaps[0] = gap(&load, limit, (3, 4));
        assert!(load.gaps[0] > 10, "{load:?}");
        let loaded = interleaved(limit, &load, 3).await;
        assert!(loaded.is_ok(), "{load:?} of {limit}: {loaded:?}");
    }
}

#[tokio::test]
async fn a_chunk_a_burst_between_commits_fills_with_two_partitions_is_carried_all_the_same() {
    // Both partitions log a burst, within three quarters of the log, before the first commit;
    // then the second commits after each batch: the burst's chunks hold the first's open frames
    // beside the second's settled ones, which a carry frees within the room a batch keeps.
    for burst in [5_u64, 10, 20] {
        let load = Load::new(&[1, 1]);
        let limit = 60 * load.frame();
        let store = Arc::new(MemoryWal::default());
        let budget = MemoryBudget::new(64 << 20);
        let (log, task) = started(&store, limit);
        let orders = view("orders");
        let slow = gap(&load, limit, (3, 4));
        let written = async {
            let mut from = 0;
            let mut next = || {
                from += 1_000;
                load.batch(from)
            };
            let (slow_segment, mut fast, mut seq) = (0, 1_000, CommitSeq::FIRST);
            let mut logged_slow = 0;
            for _ in 0..burst {
                logged(&log, &budget, 0, &orders, SegmentId(slow_segment), &next()).await?;
                logged(&log, &budget, 0, &orders, SegmentId(fast), &next()).await?;
                logged_slow += 1;
            }
            while logged_slow < slow {
                sealed(&log, &budget, &load.commit(&[fast], seq)).await?;
                (fast, seq) = (fast + 1, seq.next());
                logged(&log, &budget, 0, &orders, SegmentId(slow_segment), &next()).await?;
                logged(&log, &budget, 0, &orders, SegmentId(fast), &next()).await?;
                logged_slow += 1;
            }
            drop(log);
            Ok::<u64, crate::Error>(logged_slow)
        };
        let (ended, loaded) = tokio::join!(task, written);
        ended.expect("the writer ends");
        let loaded = loaded.unwrap_or_else(|error| panic!("burst {burst}: {error:?}"));
        assert_eq!(loaded, slow, "burst {burst}");
    }
}
