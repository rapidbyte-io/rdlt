//! Drive the loader over the load channel and settle the run's outcome.

use rdlt_connector::channel::{ByteReceiver, Permitted};
use rdlt_core::error::Error;
use rdlt_core::report;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::load::{LoadItem, Loader};

/// Drive the loader over the load channel, join the stream tasks, and commit the
/// trailing work. Precedence on error: a concrete stream error > a concrete
/// loader error > `Cancelled` (a loader failure cancels the streams, whose
/// induced `Cancelled` must not mask the original destination error).
pub(super) async fn drive(
    mut loader: Loader,
    mut load_rx: ByteReceiver<LoadItem>,
    mut stream_tasks: JoinSet<Result<(), Error>>,
    cancel: &CancellationToken,
) -> Result<report::Run, Error> {
    let loader_result: Result<(), Error> = loop {
        // `biased` toward the channel: items already in flight (e.g. a checkpoint
        // preceding a stream failure) are drained and committed before a
        // cancellation is observed — keeps failure semantics deterministic.
        let item = tokio::select! {
            biased;
            // The permit is released here, at receipt: the byte budget bounds
            // what is QUEUED, and accounting for anything derived downstream
            // belongs to that stage's own channel.
            item = load_rx.recv() => item.map(Permitted::into_value),
            _ = cancel.cancelled() => break Err(Error::Cancelled),
        };
        match item {
            Some(item) => {
                if let Err(e) = loader.process(item).await {
                    cancel.cancel();
                    break Err(e);
                }
            }
            None => break Ok(()),
        }
    };
    // Release the channel BEFORE joining: a stream task parked in `tx.send` (byte
    // budget held by queued-but-undelivered items) only unblocks when the receiver
    // drops — without this, a loader failure deadlocks the join below.
    drop(load_rx);

    // ---- Join stream tasks; prefer real errors over induced cancellations ----
    let mut first_error: Option<Error> = None;
    let mut saw_cancelled = false;
    while let Some(joined) = stream_tasks.join_next().await {
        let outcome = match joined {
            Ok(res) => res,
            Err(join_err) => Err(Error::internal(format!("stream task panicked: {join_err}"))),
        };
        match outcome {
            Err(Error::Cancelled) => saw_cancelled = true,
            Err(e) => {
                if first_error.is_none() {
                    cancel.cancel();
                    first_error = Some(e);
                }
            }
            Ok(()) => {}
        }
    }

    // ---- Abandonment paths: a stream error, a
    // loader error, or an induced cancellation all mean this session writes
    // no more — best-effort close it (releases whatever `close` releases,
    // e.g. the file destination's lease) rather than leaving that held for
    // a DIFFERENT process's TTL wait. Never lets a close failure mask the
    // run's real error, which is what every one of these returns instead.
    if let Some(error) = first_error {
        loader.close_best_effort().await;
        return Err(error);
    }
    match loader_result {
        Err(e) => {
            loader.close_best_effort().await;
            return Err(e);
        }
        Ok(()) if saw_cancelled => {
            loader.close_best_effort().await;
            return Err(Error::Cancelled);
        }
        Ok(()) => {}
    }

    // ---- Final commit: trailing work; state travels with the data ----
    if let Err(e) = loader.finish().await {
        // The final commit itself failed — still an abandonment path
        // (nothing durable changed as a RESULT of this attempt reaching
        // here, whatever landed in EARLIER commits notwithstanding), so
        // the same best-effort release applies before propagating.
        loader.close_best_effort().await;
        return Err(e);
    }
    // The strict, success-only close: the run's last commit just
    // succeeded, and every path above this point that could still fail
    // has already returned early. The close discipline — non-retryable
    // classification, frozen prefix — lives in `load::session_exit`.
    loader.close().await?;
    Ok(loader.report)
}

#[cfg(test)]
mod drive_tests {
    //! `drive`'s outcome precedence, tested directly.
    //!
    //! The `saw_cancelled` guard survived mutation because the only tests that
    //! reached it drove a real source whose cancellation was decided by a sleep
    //! — so the interesting interleaving was never reliably produced. Calling
    //! `drive` with a hand-built JoinSet removes the timing entirely.
    use rdlt_connector::channel::bytes;
    use rdlt_connector::destination::Capabilities;
    use rdlt_core::commit::CommitPolicy;
    use rdlt_core::id::{LoadId, PipelineId};
    use rdlt_core::state::StateDoc;
    use tokio::sync::broadcast;

    use super::*;
    use crate::load::Sink;
    use crate::run::once::STAGE_MSG_CAPACITY;
    use crate::testing::FakeSession;

    fn loader() -> Loader {
        let (events, _rx) = broadcast::channel(16);
        let pipeline = PipelineId::new("p");
        let load_id = LoadId::new("l");
        Loader::new(
            // The success path ends in `loader.finish()`, which commits once
            // even for a no-op run, so the session must accept a commit.
            Sink {
                session: Box::new(FakeSession::default()),
                capabilities: Capabilities::default(),
            },
            report::Run::new(pipeline.clone(), load_id.clone()),
            StateDoc::new(pipeline, "test"),
            load_id,
            crate::load::Policies {
                commit: CommitPolicy::default(),
                batch: rdlt_core::commit::BatchPolicy::default(),
                max_batch_cells: crate::config::Config::DEFAULT_MAX_BATCH_CELLS,
            },
            None,
            events,
        )
    }

    /// Dropping the sender closes the loader's input, so `recv` returns `None`
    /// immediately and the loader's own result is `Ok`.
    fn closed_input() -> ByteReceiver<LoadItem> {
        let (tx, rx) = bytes::<LoadItem>(4096, STAGE_MSG_CAPACITY);
        drop(tx);
        rx
    }

    /// A cancelled stream task must NOT be reported as a successful run. The
    /// loader completes `Ok` here, so the cancellation is the only thing that can
    /// fail the run — and defeating the guard turns a run whose streams never
    /// finished into a clean report with a committed cursor.
    #[tokio::test]
    async fn a_cancelled_stream_task_defeats_an_otherwise_clean_loader() {
        let mut tasks: JoinSet<Result<(), Error>> = JoinSet::new();
        tasks.spawn(async { Err(Error::Cancelled) });

        let cancel = CancellationToken::new();
        let result = drive(loader(), closed_input(), tasks, &cancel).await;
        assert!(
            matches!(result, Err(Error::Cancelled)),
            "a cancelled stream must surface as Cancelled, not as success: {result:?}"
        );
    }

    /// The companion case: with no cancelled task the same shape succeeds, which
    /// is what makes the assertion above about the guard rather than about
    /// `drive` always failing.
    #[tokio::test]
    async fn a_clean_run_with_no_stream_tasks_reports_success() {
        let cancel = CancellationToken::new();
        let report = drive(loader(), closed_input(), JoinSet::new(), &cancel)
            .await
            .expect("nothing failed, so the run succeeds");
        assert_eq!(
            report.commits, 1,
            "a no-op run still commits once so a fresh pipeline's state exists"
        );
    }

    /// A real stream error outranks an induced cancellation: cancelling is how
    /// the run TELLS the other streams to stop, so reporting `Cancelled` would
    /// hide the cause.
    #[tokio::test]
    async fn a_real_stream_error_outranks_a_cancellation() {
        let mut tasks: JoinSet<Result<(), Error>> = JoinSet::new();
        tasks.spawn(async { Err(Error::Cancelled) });
        tasks.spawn(async { Err(Error::internal("the real cause")) });

        let cancel = CancellationToken::new();
        let error = drive(loader(), closed_input(), tasks, &cancel)
            .await
            .expect_err("a failing stream fails the run");
        assert!(
            matches!(error, Error::Internal { .. }),
            "the real error must be reported, not the cancellation it caused: {error:?}"
        );
    }
}
