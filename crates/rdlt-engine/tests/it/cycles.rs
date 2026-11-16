//! Full reads as cycles: what a later run, its stream's modes changed or not, does with a full
//! read left part way, whose positions say where its rows landed only in the place it fills.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rdlt_connector::{ConnectContext, ReadMode, source_factory};
use rdlt_engine::{
    ErrorKind, PipelinePlan, ResetScope, RetryPolicy, RunStatus, StopMode, WriteMode, codes,
};
use serde_json::{Value, json};

use crate::ends::{Ending, STARTS, run};
use crate::support::hooked::{At, Hook, hooked};
use crate::support::{commit_every, engine, memory, pipeline, published_ids, stream, until};

/// `rows` read as `read` and written as `write`, merged by its id where it merges.
fn plan(read: ReadMode, write: WriteMode) -> PipelinePlan {
    let planned = stream("rows").read(read).write(write);
    let planned = if write == WriteMode::Merge {
        planned.key(["id"])
    } else {
        planned
    };
    pipeline("ending", [planned])
}

/// A partition that never ends of `rows` rows, checkpointed in their middle and after the last.
fn shape(rows: u64) -> Value {
    json!({ "rows": rows, "partition": "unbounded", "checkpoints": "middle_and_end" })
}

/// A hook that notes it was called in the flag returned.
fn noting() -> (Hook, Arc<AtomicBool>) {
    let noted = Arc::new(AtomicBool::new(false));
    let noting = Arc::clone(&noted);
    let hook: Hook = Arc::new(move || {
        noting.store(true, Ordering::SeqCst);
        Box::pin(async {})
    });
    (hook, noted)
}

/// Reads `rows` in full into `store`, written as `write` says, and stops the run once its first
/// commit landed: the read checkpoints after the first five of nine rows, pushes the rest and
/// waits to be stopped, so the cycle is left part way with those five committed.
async fn left_part_way(write: WriteMode, store: &str) {
    let shape = json!({
        "rows": 9, "partition": "unbounded", "checkpoints": "middle", "hold": true,
    });
    let source = source_factory::<Ending>()
        .connect(shape, ConnectContext::new())
        .await
        .expect("it connects");
    let (hook, landed) = noting();
    let destination = hooked(memory(store).await, At::Landed, hook);
    let engine = engine(commit_every(1).retry(RetryPolicy::default().max_attempts(1)));
    let stopping = engine.run(plan(ReadMode::Full, write), Arc::from(source), destination);
    let control = stopping.control();
    let stop = async {
        until(|| landed.load(Ordering::SeqCst)).await;
        control.stop(StopMode::AfterCommit);
    };
    let (stopped, ()) = tokio::join!(stopping, stop);
    assert_eq!(stopped.report.status, RunStatus::Stopped, "{stopped:?}");
    assert_eq!(stopped.report.rows, 5, "{write:?}: {stopped:?}");
}

#[tokio::test(start_paused = true)]
async fn a_full_read_resumes_a_cycle_left_part_way_only_where_both_fill_the_same_place() {
    use WriteMode::{Append, Merge, Replace};
    // Both fill the table, or both a generation; otherwise the read starts a new cycle from the
    // beginning, whose rows alone the table then holds.
    for (began, then, starts) in [
        (Append, Append, [0, 5]),
        (Append, Merge, [0, 5]),
        (Replace, Replace, [0, 5]),
        (Append, Replace, [0, 0]),
        (Merge, Replace, [0, 0]),
        (Replace, Append, [0, 0]),
        (Replace, Merge, [0, 0]),
    ] {
        let case = format!("{began:?} then {then:?}");
        let store = format!("cycle_{began:?}_{then:?}");
        STARTS.lock().clear();
        left_part_way(began, &store).await;
        let outcome = run(plan(ReadMode::Full, then), shape(9), &store).await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{case}: {outcome:?}"
        );
        assert_eq!(*STARTS.lock(), starts, "{case}");
        let every: Vec<i64> = (0..9).collect();
        assert_eq!(published_ids(&store, "rows"), every, "{case}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_incremental_read_resumes_a_cycle_left_part_way_in_its_table_and_ends_it() {
    for write in [WriteMode::Append, WriteMode::Merge] {
        let store = format!("cycle_ended_{write:?}");
        STARTS.lock().clear();
        left_part_way(write, &store).await;
        assert_eq!(
            published_ids(&store, "rows"),
            (0..5).collect::<Vec<_>>(),
            "{write:?}"
        );
        let resumed = run(plan(ReadMode::Incremental, write), shape(9), &store).await;
        assert_eq!(resumed.report.status, RunStatus::Succeeded, "{resumed:?}");
        assert_eq!(
            published_ids(&store, "rows"),
            (0..9).collect::<Vec<_>>(),
            "{write:?}"
        );
        // The cycle ended with the incremental read, so the next full read reads every row.
        let full = run(plan(ReadMode::Full, write), shape(9), &store).await;
        assert_eq!(full.report.status, RunStatus::Succeeded, "{full:?}");
        assert_eq!(*STARTS.lock(), [0, 5, 0], "{write:?}");
        let copies = if write == WriteMode::Append { 2 } else { 1 };
        let mut expected: Vec<i64> = (0..copies).flat_map(|_| 0..9).collect();
        expected.sort_unstable();
        assert_eq!(published_ids(&store, "rows"), expected, "{write:?}");
    }
}

/// How a stream refused for a replace cycle left part way is mended.
#[derive(Clone, Copy, Debug)]
enum Mended {
    /// The cycle completes, so its positions are the table's.
    Completed,
    /// The stream is reset with its tables, so incremental reads start from its beginning.
    Reset,
}

/// Mends the stream in `store` as `mended` says.
async fn mend(mended: Mended, store: &str) {
    match mended {
        Mended::Completed => {
            let completed = run(plan(ReadMode::Full, WriteMode::Replace), shape(9), store).await;
            assert_eq!(
                completed.report.status,
                RunStatus::Succeeded,
                "{completed:?}"
            );
        }
        Mended::Reset => {
            let source = source_factory::<Ending>()
                .connect(shape(9), ConnectContext::new())
                .await
                .expect("it connects");
            let reset = engine(commit_every(1_000))
                .reset(
                    "ending",
                    &["rows"],
                    ResetScope::Tables,
                    Arc::from(source),
                    memory(store).await,
                )
                .await;
            assert!(reset.is_ok(), "{reset:?}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_read_not_full_of_a_replace_cycle_left_part_way_is_refused_until_either_remedy() {
    // Completed, the cycle reads on from 5 and the incremental read from its end; reset, the
    // incremental read starts from the beginning.
    let (completed, reset): (&[u64], &[u64]) = (&[0, 5, 9], &[0, 0]);
    for (then, mended, starts) in [
        (WriteMode::Append, Mended::Completed, completed),
        (WriteMode::Merge, Mended::Completed, completed),
        (WriteMode::Append, Mended::Reset, reset),
        (WriteMode::Merge, Mended::Reset, reset),
    ] {
        let case = format!("{then:?} {mended:?}");
        let store = format!("cycle_refused_{then:?}_{mended:?}");
        STARTS.lock().clear();
        left_part_way(WriteMode::Replace, &store).await;
        let source = source_factory::<Ending>()
            .connect(shape(9), ConnectContext::new())
            .await
            .expect("it connects");
        let (hook, changed) = noting();
        let destination = hooked(memory(&store).await, At::Schema, hook);
        let refused = engine(commit_every(1_000).retry(RetryPolicy::default().max_attempts(1)))
            .run(
                plan(ReadMode::Incremental, then),
                Arc::from(source),
                destination,
            )
            .await;
        assert_eq!(refused.report.status, RunStatus::Failed, "{refused:?}");
        let error = refused.error.as_ref().expect("the run failed");
        assert_eq!(
            (error.kind(), error.code(), error.is_retryable()),
            (ErrorKind::Config, Some(codes::CURSOR_MISSING), false),
            "{case}: {error:?}"
        );
        assert_eq!(
            error.stream().map(ToString::to_string).as_deref(),
            Some("rows")
        );
        assert!(!changed.load(Ordering::SeqCst), "{case}: a table changed");
        assert_eq!(*STARTS.lock(), [0], "{case}: nothing is read");
        assert_eq!(published_ids(&store, "rows"), Vec::<i64>::new());
        mend(mended, &store).await;
        let resumed = run(plan(ReadMode::Incremental, then), shape(12), &store).await;
        assert_eq!(resumed.report.status, RunStatus::Succeeded, "{resumed:?}");
        assert_eq!(*STARTS.lock(), starts, "{case}");
        assert_eq!(
            published_ids(&store, "rows"),
            (0..12).collect::<Vec<_>>(),
            "{case}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_reset_ends_a_replace_cycle_and_the_table_keeps_its_rows_until_another_swaps_in() {
    let store = "cycle_reset";
    STARTS.lock().clear();
    let replace = || plan(ReadMode::Full, WriteMode::Replace);
    let first = run(replace(), shape(6), store).await;
    assert_eq!(first.report.status, RunStatus::Succeeded, "{first:?}");
    let published: Vec<i64> = (0..6).collect();
    left_part_way(WriteMode::Replace, store).await;
    let source = source_factory::<Ending>()
        .connect(shape(9), ConnectContext::new())
        .await
        .expect("it connects");
    let reset = engine(commit_every(1_000))
        .reset(
            "ending",
            &["rows"],
            ResetScope::Positions,
            Arc::from(source),
            memory(store).await,
        )
        .await;
    assert!(reset.is_ok(), "{reset:?}");
    assert_eq!(
        published_ids(store, "rows"),
        published,
        "the reset keeps the table"
    );
    // The next cycle starts from the beginning in a generation of its own, and the table keeps
    // its rows while it is part way.
    left_part_way(WriteMode::Replace, store).await;
    assert_eq!(
        published_ids(store, "rows"),
        published,
        "the cycle is hidden"
    );
    let completed = run(replace(), shape(9), store).await;
    assert_eq!(
        completed.report.status,
        RunStatus::Succeeded,
        "{completed:?}"
    );
    assert_eq!(*STARTS.lock(), [0, 0, 0, 5]);
    assert_eq!(published_ids(store, "rows"), (0..9).collect::<Vec<_>>());
}
