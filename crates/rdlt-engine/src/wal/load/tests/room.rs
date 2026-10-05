//! Room in a full log: partitions whose segments span many chunks, and checkpoints far apart,
//! load through a log that holds what they leave unsealed.

use std::num::NonZeroU64;
use std::sync::Arc;

use rdlt_connector::{CommitSeq, SegmentId};

use super::{ids, load, logged, meta, pipeline, receipt, sealed_at};
use crate::budget::MemoryBudget;
use crate::table::testing::view;
use crate::wal::frame::{self, Frame};
use crate::wal::load::{LoadLog, Owner};
use crate::wal::memory::MemoryWal;
use crate::wal::store::WalStore;

/// Bytes: what the frame of one of the tests' batches takes.
fn frame_bytes() -> u64 {
    let batch = frame::Batch {
        segment: SegmentId(u64::MAX),
        table: 0,
        ordinal: u64::MAX,
        batch: ids(0),
    };
    let frame = Frame::Batch(batch).encode().expect("encodes");
    u64::try_from(frame.len()).expect("a length")
}

/// Seals `segment` and commits it as `seq`, its receipt following, as a coordinator does.
async fn sealed(
    log: &LoadLog,
    budget: &MemoryBudget,
    segment: u64,
    seq: CommitSeq,
) -> Result<(), crate::Error> {
    log.checkpointed();
    let committing = log.committing();
    log.took(1);
    let mut commit = meta(&[segment]);
    commit.commit_seq = seq;
    log.commit(budget, vec![sealed_at(segment)], Vec::new(), &commit, 0)
        .await?;
    log.committed(&receipt(seq)).await?;
    drop(committing);
    Ok(())
}

/// Logs the batches of `partitions` partitions in turn, each sealing its segment after `every`
/// of them, the first segment of each partition begun `stagger` batches after the partition's
/// before it,
/// through a log of `limit` bytes, a commit taking each seal and its receipt following, until
/// `seals` seals: the error a batch was refused with.
async fn interleaved(
    limit: u64,
    (partitions, every, stagger): (u64, u64, u64),
    seals: u64,
) -> Result<(), crate::Error> {
    let store = Arc::new(MemoryWal::default());
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let owner = Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: rdlt_connector::Epoch(1),
        opened: None,
        origin: load(),
    };
    let limit = NonZeroU64::new(limit).expect("a limit");
    let (log, task) = LoadLog::start(wal, owner, limit, None);
    let budget = MemoryBudget::new(64 << 20);
    let orders = view("orders");
    let written = async {
        // Each partition's segment and the batches logged of it.
        let mut open: Vec<(u64, u64)> = (0..partitions)
            .map(|partition| (partition, partition * stagger % every))
            .collect();
        let mut next = partitions;
        let mut seq = CommitSeq::FIRST;
        let mut sealings = 0;
        let result = 'load: loop {
            for slot in &mut open {
                let segment = SegmentId(slot.0);
                if let Err(error) = logged(&log, &budget, 0, &orders, segment, &ids(0)).await {
                    break 'load Err(error);
                }
                slot.1 += 1;
                if slot.1 < every {
                    continue;
                }
                if let Err(error) = sealed(&log, &budget, slot.0, seq).await {
                    break 'load Err(error);
                }
                seq = seq.next();
                *slot = (next, 0);
                next += 1;
                sealings += 1;
                if sealings == seals {
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

/// Frames: the room a log keeps beside the open frames its partitions hold, in frames of the
/// tests' batches: a chunk's header and end, a seal, a commit, and a batch's frame twice over.
const BESIDE: u64 = 8;

#[tokio::test]
async fn partitions_whose_segments_span_many_chunks_load_through_a_log_their_open_frames_fit() {
    let frame = frame_bytes();
    for (partitions, every) in [(4, 3), (8, 3), (4, 6), (16, 2)] {
        let limit = (partitions * every + BESIDE) * frame;
        let loaded = interleaved(limit, (partitions, every, 1), 80).await;
        assert!(
            loaded.is_ok(),
            "{partitions} partitions sealing every {every}: {loaded:?}"
        );
    }
}

#[tokio::test]
async fn a_partition_whose_checkpoints_lie_nearly_a_log_apart_loads_through_it() {
    let frame = frame_bytes();
    let frames = 40;
    for every in 1..=frames - BESIDE {
        let loaded = interleaved(frames * frame, (1, every, 0), 6).await;
        assert!(loaded.is_ok(), "every {every}: {loaded:?}");
    }
    // A segment that alone passes the log is refused, before its partition hears of it.
    let refused = interleaved(frames * frame, (1, 2 * frames, 0), 1).await;
    let refused = refused.expect_err("refused");
    assert_eq!(refused.code(), Some("log_bytes_exceeded"));
    assert!(!refused.is_retryable());
}

#[tokio::test]
async fn a_load_whose_partitions_checkpoint_within_its_log_always_completes() {
    let frame = frame_bytes();
    for partitions in 1..=8 {
        for every in 1..=6 {
            for stagger in 0..every {
                let limit = (partitions * every + BESIDE) * frame;
                let shape = (partitions, every, stagger);
                let loaded = interleaved(limit, shape, 3 * partitions).await;
                assert!(loaded.is_ok(), "{shape:?}: {loaded:?}");
            }
        }
    }
}
