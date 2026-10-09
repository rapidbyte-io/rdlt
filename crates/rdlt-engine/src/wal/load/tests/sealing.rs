//! Batches logged while a commit's frame waits after its seals, as a commit frame waiting for
//! memory or a writer behind leaves it: they go to the chunks after the commit's, so the chunk
//! holding its seals holds no more than a chunk may beside the commit's own frames.

use std::sync::Arc;

use rdlt_connector::{CommitSeq, SegmentId};

use super::room::{Load, started};
use super::{logged, receipt, sealed_at};
use crate::budget::MemoryBudget;
use crate::table::testing::view;
use crate::wal::load::LoadLog;
use crate::wal::memory::MemoryWal;

/// Seals and commits `segments` as `seq` of `load`, its receipt following, as a coordinator does.
async fn committed(
    log: &LoadLog,
    budget: &MemoryBudget,
    load: &Load,
    (segments, seq): (&[u64], CommitSeq),
) -> Result<(), crate::Error> {
    let committing = log.committing();
    log.took(segments.len());
    let seals = segments.iter().copied().map(sealed_at).collect();
    let commit = load.commit(segments, seq);
    log.commit(budget, seals, Vec::new(), &commit, 0).await?;
    log.committed(&receipt(seq)).await?;
    drop(committing);
    Ok(())
}

/// Logs `window` frames of a slow partition and of a fast one between a commit's seal and its
/// frame, then `gap` of the slow one in all beside the fast one committing each of its own.
///
/// Before the window each logs a frame, the fast one's sealed by the commit; the fast one seals
/// each frame of the window, and those go in the next commit.
async fn windowed(window: u64, gap: u64) -> Result<(), crate::Error> {
    let mut load = Load::new(&[gap, 1]);
    load.rows = 2_000;
    let limit = 60 * load.frame();
    let store = Arc::new(MemoryWal::default());
    let budget = crate::budget::budget(64 << 20);
    let (log, task) = started(&store, limit);
    let written = async move {
        let orders = view("orders");
        let mut from = 0;
        let mut next = || {
            from += 10_000;
            load.batch(from)
        };
        let (slow, mut fast, mut seq) = (0_u64, 1_000_u64, CommitSeq::FIRST);
        let loaded: Result<(), crate::Error> = async {
            let mut frames = 1;
            logged(&log, &budget, 0, &orders, SegmentId(slow), &next()).await?;
            logged(&log, &budget, 0, &orders, SegmentId(fast), &next()).await?;
            log.checkpointed();
            let committing = log.committing();
            log.took(1);
            let first = load.commit(&[fast], seq);
            let seals = log.seals(&budget, vec![sealed_at(fast)], &first).await?;
            let mut piled = Vec::new();
            for _ in 0..window {
                fast += 1;
                logged(&log, &budget, 0, &orders, SegmentId(slow), &next()).await?;
                frames += 1;
                logged(&log, &budget, 0, &orders, SegmentId(fast), &next()).await?;
                log.checkpointed();
                piled.push(fast);
            }
            log.finish(&budget, seals, Vec::new(), &first, 0).await?;
            log.committed(&receipt(seq)).await?;
            drop(committing);
            seq = seq.next();
            committed(&log, &budget, &load, (&piled, seq)).await?;
            while frames < gap {
                seq = seq.next();
                fast += 1;
                logged(&log, &budget, 0, &orders, SegmentId(slow), &next()).await?;
                frames += 1;
                logged(&log, &budget, 0, &orders, SegmentId(fast), &next()).await?;
                log.checkpointed();
                committed(&log, &budget, &load, (&[fast], seq)).await?;
            }
            Ok(())
        }
        .await;
        drop(log);
        loaded
    };
    let (ended, loaded) = tokio::join!(task, written);
    ended.expect("the writer ends well");
    loaded
}

#[tokio::test]
async fn frames_logged_while_a_commit_s_frame_waits_after_its_seals_leave_its_chunk_carriable() {
    // A log of 60 frames: the slow partition's frames, beside the fast one's sealed and not yet
    // received and a commit's, stay within three quarters of it however long the window.
    for window in [16, 20] {
        for gap in [30, 36, 40] {
            let loaded = windowed(window, gap).await;
            assert!(loaded.is_ok(), "window {window}, gap {gap}: {loaded:?}");
        }
    }
}
