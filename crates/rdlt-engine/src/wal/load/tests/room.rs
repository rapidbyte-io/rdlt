//! Room in a full log: partitions whose segments span many chunks, checkpoints far apart, small
//! batches beside large commits, load through a log that holds what they leave unsealed, and the
//! log never holds more than it may.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use proptest::strategy::Strategy as _;
use rdlt_connector::{CommitSeq, Epoch, SegmentId, StateChange, StateRecord};
use rdlt_wire::limits::count;

use super::{json_value, load, logged, meta, pipeline, receipt, sealed_at};
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

    /// Bytes: the most the frame of one of the load's batches takes.
    pub(super) fn frame(&self) -> u64 {
        let framed = |partition| {
            let batch = frame::Batch {
                segment: SegmentId(u64::MAX),
                table: 0,
                ordinal: u64::MAX,
                batch: self.batch_of(partition, i64::MAX / 2),
            };
            length(Frame::Batch(batch).encode().expect("encodes").len())
        };
        framed(0).max(framed(1))
    }

    /// Whether `frames` of the load's batch frames open at once, beside a commit of `commit`
    /// bytes, fit a log of `limit` bytes, as [`fits`] counts them.
    pub(super) fn fits(&self, limit: u64, frames: u64, commit: u64) -> bool {
        fits(limit, (frames, self.frame()), schema(), commit)
    }

    /// The commit of `segments` as `seq`, recording the load's bytes of state.
    pub(super) fn commit(&self, segments: &[u64], seq: CommitSeq) -> rdlt_connector::CommitMeta {
        let mut commit = meta(segments);
        commit.commit_seq = seq;
        if self.recorded > 0 {
            commit.state_delta = vec![StateChange::Put(StateRecord {
                key: "k".to_owned(),
                value: json_value(self.recorded),
            })];
        }
        commit
    }

    /// Bytes: what one of the load's commits writes, its seal and its commit frame.
    pub(super) fn committed(&self) -> u64 {
        let commit = self.commit(&[u64::MAX], CommitSeq::FIRST);
        let mut frame = frame::Framing::default();
        frame.commit(&commit, 1, 0).expect("encodes");
        // A seal frame of the tests takes well under a KiB.
        length(frame.len()) + 1_024
    }

    /// The most frames the load's partitions hold that they have not sealed.
    fn frames(&self) -> u64 {
        self.gaps.iter().sum()
    }
}

/// Whether `frames.0` open batch frames of at most `frames.1` bytes each, of a table whose
/// schema frame takes `schema` bytes, fit a log of `limit` bytes beside a commit of `commit`.
///
/// They, the commit's frames and what the chunks holding them write beside them take at most
/// three quarters of it. A chunk holds an eighth of the log less what it writes beside its batch
/// frames, and every chunk holding open frames but two holds more than half of that, or a frame
/// alone.
pub(super) fn fits(limit: u64, frames: (u64, u64), schema: u64, commit: u64) -> bool {
    let (number, frame) = frames;
    let open = number * frame;
    let mut chunks = 1;
    let beside = loop {
        let beside = framing(chunks + 2) + schema;
        let holds = (limit / 8).saturating_sub(beside);
        let needed = if holds == 0 {
            number
        } else {
            number.min((2 * open).div_ceil(holds) + 1)
        };
        if needed <= chunks {
            break beside;
        }
        chunks = needed;
    };
    open + commit + chunks * beside <= limit * 3 / 4
}

/// Bytes: what a chunk of the tests' log writes beside its batch and schema frames where its
/// end names `chunks` chunks and as many commits: its preamble, header, relief frame and end.
fn framing(chunks: u64) -> u64 {
    let header = frame::Header {
        pipeline: pipeline(),
        load: load(),
        chunk: u64::MAX,
        epoch: Epoch(1),
        opened: None,
        origin: load(),
    };
    let named = || (0..chunks).map(|_| u64::MAX);
    count(frame::PREAMBLE)
        + frame::header_len(&header).expect("measures")
        + count(frame::HEAD)
        + frame::end_len(named(), named())
}

/// Bytes: the schema frame of the tests' table.
pub(super) fn schema() -> u64 {
    let orders = view("orders");
    let table = frame::Table {
        index: u32::MAX,
        table: orders.table.clone(),
        schema: orders.physical_schema(),
    };
    length(Frame::Schema(table).encode().expect("encodes").len())
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
        epoch: Epoch(1),
        opened: None,
        origin: load(),
    };
    let bound = NonZeroU64::new(limit).expect("a limit");
    LoadLog::start(wal, owner, bound, (None, Arc::default()))
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
///
/// The load runs twice: its segments and batch frames numbered from zero, then numbered as
/// widely as they may be, so every frame takes the most its JSON head may.
async fn interleaved(limit: u64, load: &Load, rounds: u64) -> Result<(), crate::Error> {
    interleaved_from(limit, load, rounds, 0).await?;
    interleaved_from(limit, load, rounds, WIDE).await
}

/// A number as wide as a segment's or a batch frame's may be, from which the wide runs number
/// theirs.
const WIDE: u64 = 10_000_000_000_000_000_000;

/// Runs `load` as [`interleaved`] does, its segments and batch frames numbered from `first`.
async fn interleaved_from(
    limit: u64,
    load: &Load,
    rounds: u64,
    first: u64,
) -> Result<(), crate::Error> {
    let store = Arc::new(MemoryWal::default());
    let budget = crate::budget::budget(64 << 20);
    let (log, task) = started(&store, limit);
    log.batches.store(first, Ordering::Relaxed);
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
                let segment = SegmentId(first + slot.0);
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
                let commit = load.commit(&[first + slot.0], seq);
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

/// The first partition's gap: the most of its frames that, beside a gap of each other
/// partition's, fit a log of `limit` bytes beside one of the load's commits; one where none do.
pub(super) fn gap(load: &Load, limit: u64) -> u64 {
    let others: u64 = load.gaps[1..].iter().sum();
    let mut gap = 1;
    while load.fits(limit, gap + 1 + others, load.committed()) {
        gap += 1;
    }
    gap
}

#[tokio::test]
async fn a_load_whose_partitions_hold_three_quarters_of_its_log_unsealed_completes() {
    for mut load in loads() {
        let limit = 60 * load.frame();
        load.gaps[0] = gap(&load, limit);
        assert!(
            load.fits(limit, load.frames(), load.committed()),
            "{load:?}"
        );
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
    load.gaps[0] = gap(&load, limit);
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
        load.gaps[0] = gap(&load, limit);
        assert!(load.gaps[0] > 3, "{load:?}");
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
        let budget = crate::budget::budget(64 << 20);
        let (log, task) = started(&store, limit);
        let orders = view("orders");
        let slow = gap(&load, limit);
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

#[tokio::test]
async fn a_carry_gathers_open_frames_into_chunks_no_larger_than_an_eighth_of_the_log() {
    // Two segments interleaved, one frame of the first to three of the second, over some eight
    // capped chunks: once the second commits, the next batch finds the log full, and its relief
    // carries the first's frames out of each.
    let load = Load::new(&[1]);
    let limit = 200 * load.frame();
    let store = Arc::new(MemoryWal::default());
    let budget = crate::budget::budget(64 << 20);
    let (log, task) = started(&store, limit);
    let observed = Arc::clone(&store);
    let written = async move {
        let orders = view("orders");
        let mut from = 0;
        let mut next = || {
            from += 1_000;
            load.batch(from)
        };
        for _ in 0..40 {
            logged(&log, &budget, 0, &orders, SegmentId(1), &next())
                .await
                .expect("logged");
            for _ in 0..3 {
                logged(&log, &budget, 0, &orders, SegmentId(2), &next())
                    .await
                    .expect("logged");
            }
        }
        sealed(&log, &budget, &load.commit(&[2], CommitSeq::FIRST))
            .await
            .expect("committed");
        logged(&log, &budget, 0, &orders, SegmentId(1), &next())
            .await
            .expect("logged");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends well");
    for (chunk, bytes) in observed.stored(&pipeline()) {
        assert!(
            length(bytes.len()) <= limit / 8,
            "{chunk:?}: {}",
            bytes.len()
        );
    }
    // The relief carried the first's frames out of every chunk the second's settled frames
    // were in, and those chunks went.
    let settled = super::frames(&observed)
        .into_iter()
        .filter(|frame| matches!(frame, Frame::Batch(batch) if batch.segment == SegmentId(2)))
        .count();
    assert_eq!(settled, 0);
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(rdlt_testkit::cases(48)))]

    /// A load whose open frames, a commit's and what the chunks holding the open frames write
    /// beside them take three quarters of its log loads, whatever the sizes of its log, frames,
    /// commits and partitions.
    #[test]
    fn a_load_whose_open_frames_and_their_chunks_hold_three_quarters_of_its_log_completes(
        others in proptest::collection::vec(1_u64..=3, 0..=3),
        rows in 1_usize..=200,
        first_rows in proptest::option::of(1_usize..=200),
        recorded in (0_usize..=1_500).prop_map(|half| 2 * half),
        frames in 12_u64..=96,
        stagger in 0_u64..=3,
    ) {
        let mut gaps = vec![1];
        gaps.extend(others);
        let mut load = Load { gaps, rows, first_rows, recorded, stagger };
        let limit = frames * load.frame();
        load.gaps[0] = gap(&load, limit);
        proptest::prop_assume!(load.fits(limit, load.frames(), load.committed()));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        let loaded = runtime.block_on(interleaved(limit, &load, 3));
        proptest::prop_assert!(loaded.is_ok(), "{load:?} of {limit}: {loaded:?}");
    }
}
