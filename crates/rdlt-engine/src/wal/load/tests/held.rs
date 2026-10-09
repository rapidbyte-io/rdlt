#![expect(
    clippy::disallowed_methods,
    reason = "a test queues a schema frame's request for memory from a task of its own"
)]

//! What a commit's staged seals hold back, and what they do not: schema frames sent meanwhile
//! hold none of the memory the commit's own frames need, and a close that comes without their
//! commit fails the log.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{CommitMeta, Cursor, PartitionState, SegmentId, StateChange, StateRecord};

use super::{frames, ids, meta, sealed_at, start};
use crate::budget::MemoryBudget;
use crate::compute::Pool;
use crate::error::ErrorKind;
use crate::table::testing::view;
use crate::wal::load::{LoadLog, Sealed};
use crate::wal::memory::MemoryWal;

/// Longer than anything a test here waits for: past it, a wait waits for memory nothing frees.
const FOR_EVER: Duration = Duration::from_hours(4);

/// Logs a batch of segment `segment` for table `table`, each table a version of its own and so a
/// schema frame of its own, held by `log`'s budget, the batch's frame by `frames`'.
async fn described(
    load: &LoadLog,
    (frames, log): (&MemoryBudget, &MemoryBudget),
    table: usize,
    segment: u64,
) -> Result<(), crate::Error> {
    let held: rdlt_connector::Permit = Box::new(
        frames
            .try_acquire_working(4_096)
            .expect("the budget has room"),
    );
    let orders = view("orders");
    load.batch(
        &Pool::inline(),
        log,
        held,
        (table, &orders),
        SegmentId(segment),
        &ids(0),
    )
    .await
}

/// The commit of `segments` recording `value` bytes of state.
fn recording(segments: &[u64], value: usize) -> CommitMeta {
    let mut commit = meta(segments);
    commit.state_delta = vec![StateChange::Put(StateRecord {
        key: "k".to_owned(),
        value: vec![7; value].into(),
    })];
    commit
}

/// A seal of `segment` whose cursor takes `cursor` bytes.
fn sealed_with(segment: u64, cursor: usize) -> Sealed {
    let cursor = Cursor::new(1, &vec![b'c'; cursor]).expect("a cursor");
    Sealed {
        state: PartitionState::Cursor(cursor),
        ..sealed_at(segment)
    }
}

#[tokio::test(start_paused = true)]
async fn a_commit_s_frame_takes_memory_that_schema_frames_sent_after_its_seals_would_hold() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let (budget, frames_held) = (
        crate::budget::budget(1 << 20),
        crate::budget::budget(1 << 30),
    );
    let share = budget.shares().log;
    let commit = recording(&[0], 20_000);
    let estimate = super::super::commit::commit_bytes(&[], &commit);
    assert!(
        estimate < share,
        "the commit's frame alone fits the log's share"
    );
    let written = async {
        let held = (&frames_held, &budget);
        described(&log, held, 0, 0).await.expect("logged");
        let seals = log
            .seals(&budget, vec![sealed_at(0)], &commit)
            .await
            .expect("sealed");
        // Other partitions begin tables while the commit's frame is still to come, as many as
        // would leave it no room if their schema frames held their memory.
        for table in 1..=74 {
            described(&log, held, table, 100 + u64::try_from(table).expect("few"))
                .await
                .expect("logged");
        }
        let finished =
            tokio::time::timeout(FOR_EVER, log.finish(&budget, seals, Vec::new(), &commit, 0))
                .await;
        assert!(matches!(finished, Ok(Ok(()))), "{finished:?}");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends well");
}

#[tokio::test(start_paused = true)]
async fn a_commit_s_later_seal_takes_memory_that_schema_frames_sent_after_its_first_would_hold() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let (budget, frames_held) = (
        crate::budget::budget(1 << 20),
        crate::budget::budget(1 << 30),
    );
    let commit = meta(&[0, 1]);
    let written = async {
        let held = (&frames_held, &budget);
        described(&log, held, 0, 0).await.expect("logged");
        described(&log, held, 0, 1).await.expect("logged");
        let _first = log
            .seals(&budget, vec![sealed_with(0, 14_000)], &commit)
            .await
            .expect("sealed");
        // As many tables as would leave the second seal no room if their schema frames held
        // their memory.
        for table in 1..=121 {
            described(&log, held, table, 100 + u64::try_from(table).expect("few"))
                .await
                .expect("logged");
        }
        let sealed = tokio::time::timeout(
            FOR_EVER,
            log.seals(&budget, vec![sealed_with(1, 14_000)], &commit),
        )
        .await;
        assert!(matches!(sealed, Ok(Ok(_))), "{:?}", sealed.map(|_| ()));
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends well");
}

#[tokio::test(start_paused = true)]
async fn a_seal_is_not_held_behind_a_schema_frame_waiting_for_memory_during_its_commit() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let (budget, frames_held) = (
        crate::budget::budget(1 << 20),
        crate::budget::budget(1 << 30),
    );
    let share = budget.shares().log;
    let commit = meta(&[0, 1]);
    let written = async {
        let held = (&frames_held, &budget);
        described(&log, held, 0, 0).await.expect("logged");
        described(&log, held, 0, 1).await.expect("logged");
        let _first = log
            .seals(&budget, vec![sealed_at(0)], &commit)
            .await
            .expect("sealed");
        for table in 1..=20 {
            described(&log, held, table, 100 + u64::try_from(table).expect("few"))
                .await
                .expect("logged");
        }
        // The log's share holds so much else that a seal of no cursor, 4 KiB, just fits beside
        // what the schema frames would hold; one more schema frame asks for memory first.
        let filler = share - budget.reserved() - 4_096;
        let _filler = budget.acquire_log(filler).await.expect("it fits");
        let queued = {
            let (log, budget, frames_held) = (log.clone(), budget.clone(), frames_held.clone());
            tokio::spawn(async move { described(&log, (&frames_held, &budget), 999, 9_999).await })
        };
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        let sealed =
            tokio::time::timeout(FOR_EVER, log.seals(&budget, vec![sealed_at(1)], &commit)).await;
        assert!(matches!(sealed, Ok(Ok(_))), "{:?}", sealed.map(|_| ()));
        queued.abort();
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends well");
}

#[tokio::test(start_paused = true)]
async fn a_close_that_comes_after_seals_without_their_commit_fails_the_log() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = crate::budget::budget(1 << 24);
    let written = async {
        described(&log, (&budget, &budget), 0, 1)
            .await
            .expect("logged");
        let _seals = log
            .seals(&budget, vec![sealed_at(1)], &meta(&[1]))
            .await
            .expect("sealed");
        let closed = tokio::time::timeout(FOR_EVER, log.close()).await;
        let error = closed
            .expect("the close is answered")
            .expect_err("it fails");
        assert_eq!(error.kind(), ErrorKind::Internal, "{error:?}");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    assert!(frames(&store).is_empty(), "the seals are never published");
}
