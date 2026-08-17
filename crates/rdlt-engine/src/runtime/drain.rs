//! Drain the loader over the load channel and settle the run's outcome.

use rdlt_connector::channel::{ByteReceiver, Permitted};
use rdlt_core::{RdltError, RunReport};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::load::{LoadItem, Loader};

/// Drain the loader over the load channel, join the stream tasks, and commit the
/// trailing work. Precedence on error: a concrete stream error > a concrete
/// loader error > `Cancelled` (a loader failure cancels the streams, whose
/// induced `Cancelled` must not mask the original destination error).
pub(super) async fn drain_loader(
    mut loader: Loader,
    mut load_rx: ByteReceiver<LoadItem>,
    mut stream_tasks: JoinSet<Result<(), RdltError>>,
    cancel: &CancellationToken,
) -> Result<RunReport, RdltError> {
    let loader_result: Result<(), RdltError> = loop {
        // `biased` toward the channel: items already in flight (e.g. a checkpoint
        // preceding a stream failure) are drained and committed before a
        // cancellation is observed — keeps failure semantics deterministic.
        let item = tokio::select! {
            biased;
            // The permit is released here, at receipt: the byte budget bounds
            // what is QUEUED, and accounting for anything derived downstream
            // belongs to that stage's own channel.
            item = load_rx.recv() => item.map(Permitted::into_value),
            _ = cancel.cancelled() => break Err(RdltError::Cancelled),
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
    let mut first_error: Option<RdltError> = None;
    let mut saw_cancelled = false;
    while let Some(joined) = stream_tasks.join_next().await {
        let outcome = match joined {
            Ok(res) => res,
            Err(join_err) => Err(RdltError::internal(format!(
                "stream task panicked: {join_err}"
            ))),
        };
        match outcome {
            Err(RdltError::Cancelled) => saw_cancelled = true,
            Err(e) => {
                if first_error.is_none() {
                    cancel.cancel();
                    first_error = Some(e);
                }
            }
            Ok(()) => {}
        }
    }

    // ---- Abandonment paths (037 US2 fix round 2, I1): a stream error, a
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
            return Err(RdltError::Cancelled);
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
    // has already returned early. `Loader::close` classifies its own
    // error non-retryable and prefixes it — see that method's doc.
    loader.close().await?;
    Ok(loader.report)
}

#[cfg(test)]
mod drain_loader_tests {
    //! `drain_loader`'s outcome precedence, tested directly.
    //!
    //! The `saw_cancelled` guard survived mutation because the only tests that
    //! reached it drove a real source whose cancellation was decided by a sleep
    //! — so the interesting interleaving was never reliably produced. Calling
    //! `drain_loader` with a hand-built JoinSet removes the timing entirely.
    use super::*;
    use crate::load::Sink;
    use crate::runtime::STAGE_MSG_CAPACITY;
    use rdlt_connector::channel::bytes;
    use rdlt_connector::destination::{Capabilities, LoadSession};
    use rdlt_core::{
        CommitMeta, CommitPolicy, CommitReceipt, LoadId, PipelineId, StateDoc, TableName,
        TableSchema, WriteMode,
    };
    use tokio::sync::broadcast;

    /// The success path ends in `loader.finish()`, which commits once even for a
    /// no-op run, so this session must accept a commit.
    struct AcceptingSession;

    #[async_trait::async_trait]
    impl LoadSession for AcceptingSession {
        async fn ensure_table(
            &mut self,
            _: &TableSchema,
            _: &WriteMode,
        ) -> Result<(), rdlt_connector::error::DestinationError> {
            Ok(())
        }
        async fn write(
            &mut self,
            _: &TableName,
            _: rdlt_connector::arrow::RecordBatch,
        ) -> Result<(), rdlt_connector::error::DestinationError> {
            Ok(())
        }
        async fn commit(
            &mut self,
            meta: CommitMeta,
        ) -> Result<CommitReceipt, rdlt_connector::error::DestinationError> {
            Ok(CommitReceipt {
                load_id: meta.load_id,
                commit_seq: meta.commit_seq,
            })
        }
        async fn read_state(
            &mut self,
            _: &PipelineId,
        ) -> Result<Option<StateDoc>, rdlt_connector::error::DestinationError> {
            Ok(None)
        }
    }

    fn loader() -> Loader {
        let (events, _rx) = broadcast::channel(16);
        let pipeline = PipelineId::new("p");
        let load_id = LoadId::new("l");
        Loader::new(
            Sink {
                session: Box::new(AcceptingSession),
                capabilities: Capabilities::default(),
            },
            RunReport::new(pipeline.clone(), load_id.clone()),
            StateDoc::new(pipeline, "test"),
            load_id,
            crate::load::Policies {
                commit: CommitPolicy::default(),
                batch: rdlt_core::BatchPolicy::default(),
                max_batch_cells: crate::DEFAULT_MAX_BATCH_CELLS,
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
        let mut tasks: JoinSet<Result<(), RdltError>> = JoinSet::new();
        tasks.spawn(async { Err(RdltError::Cancelled) });

        let cancel = CancellationToken::new();
        let result = drain_loader(loader(), closed_input(), tasks, &cancel).await;
        assert!(
            matches!(result, Err(RdltError::Cancelled)),
            "a cancelled stream must surface as Cancelled, not as success: {result:?}"
        );
    }

    /// The companion case: with no cancelled task the same shape succeeds, which
    /// is what makes the assertion above about the guard rather than about
    /// `drain_loader` always failing.
    #[tokio::test]
    async fn a_clean_run_with_no_stream_tasks_reports_success() {
        let cancel = CancellationToken::new();
        let report = drain_loader(loader(), closed_input(), JoinSet::new(), &cancel)
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
        let mut tasks: JoinSet<Result<(), RdltError>> = JoinSet::new();
        tasks.spawn(async { Err(RdltError::Cancelled) });
        tasks.spawn(async { Err(RdltError::internal("the real cause")) });

        let cancel = CancellationToken::new();
        let error = drain_loader(loader(), closed_input(), tasks, &cancel)
            .await
            .expect_err("a failing stream fails the run");
        assert!(
            matches!(error, RdltError::Internal { .. }),
            "the real error must be reported, not the cancellation it caused: {error:?}"
        );
    }
}
