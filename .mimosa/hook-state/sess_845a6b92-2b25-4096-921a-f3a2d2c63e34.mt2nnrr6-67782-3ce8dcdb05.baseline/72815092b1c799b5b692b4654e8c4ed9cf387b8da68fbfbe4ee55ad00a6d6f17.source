//! The commit cadence: `CommitPolicy` decides WHEN the loader commits, and its
//! boundaries are exact. Mutation-report closure; the doc names the mutant
//! class it kills.

use rdlt_core::commit::{BatchPolicy, CommitPolicy};
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory;

use super::common::three_batch_source;

/// Kills: `policy_triggers` `>=`→`<` boundaries and `EveryCheckpoints` counting.
/// 3 checkpoints under EveryCheckpoints(2): commit fires at checkpoint 2, the
/// trailing work commits in finish() — exactly 2 commits.
#[tokio::test]
async fn commit_policy_boundaries_are_exact() {
    let dest = memory::Destination::new();
    let mut config = Config::new("policy");
    config = config.with_commit_policy(CommitPolicy::every_checkpoints(2));
    let report = Engine::new(config, three_batch_source(), dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(
        report.commits, 2,
        "checkpoint 2 commits; finish() commits the tail"
    );

    // EveryBytes(1): every checkpoint boundary sees bytes > threshold → commits
    // at each of the 3 checkpoints. `bytes_since_commit` is accumulated from the
    // batch in `Loader::process`, so this pins the POLICY arm, not
    // `LoadItem::byte_size` (see the note on
    // `report_counters_are_exact_and_clean_runs_emit_no_discards`).
    let dest = memory::Destination::new();
    let mut config = Config::new("policy-bytes");
    config = config.with_commit_policy(CommitPolicy::every_bytes(1));
    let report = Engine::new(config, three_batch_source(), dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(
        report.commits, 3,
        "byte policy triggers at every checkpoint"
    );
}

/// `batch_policy` COALESCES source batches into larger destination
/// writes, and the engine does it — so every destination gets it
/// without implementing anything.
///
/// The default is pass-through, which is why this asserts the
/// grouping and not just the rows: row contents are identical either
/// way, and only `write_sizes` can tell one write of 30 from three
/// of 10.
#[tokio::test]
async fn batch_policy_coalesces_writes_and_loses_nothing() {
    // Baseline: no policy, one write per source batch.
    let dest = memory::Destination::new();
    let config = Config::new("batch-none");
    let report = Engine::new(config, three_batch_source(), dest.clone())
        .run()
        .await
        .expect("run");
    let straight_through = dest.write_sizes();
    let rows_written: usize = straight_through.iter().sum();
    assert_eq!(
        straight_through.len(),
        3,
        "the default writes each source batch straight through: {straight_through:?}"
    );

    // Coalescing: a threshold above the source's batch size must
    // produce FEWER, bigger writes carrying the SAME rows.
    //
    // THE COMMIT CADENCE IS AN UPPER BOUND ON BATCHING, because a
    // batch never spans a commit — so the commit policy is widened
    // here too. Leaving it at the default (commit every checkpoint)
    // would flush at every checkpoint and coalesce NOTHING, which is
    // correct behaviour and a real trap: a large `batch_policy` buys
    // nothing against a tight `commit_policy`.
    let dest = memory::Destination::new();
    let mut config = Config::new("batch-coalesced");
    config = config
        .with_batch_policy(BatchPolicy::every_rows(1_000_000))
        .with_commit_policy(CommitPolicy::every_checkpoints(1_000_000));
    let coalesced_report = Engine::new(config, three_batch_source(), dest.clone())
        .run()
        .await
        .expect("run");
    let coalesced = dest.write_sizes();

    assert!(
        coalesced.len() < straight_through.len(),
        "coalescing must reduce the write count: {coalesced:?} vs {straight_through:?}"
    );
    assert_eq!(
        coalesced.iter().sum::<usize>(),
        rows_written,
        "every row must still arrive: {coalesced:?}"
    );
    let total = |r: &rdlt_core::report::Run| r.tables.values().map(|t| t.rows).sum::<u64>();
    assert_eq!(
        total(&coalesced_report),
        total(&report),
        "the report must not change with the grouping"
    );
}

/// A batch NEVER spans a commit: whatever has accumulated is written
/// before the commit unit closes.
///
/// Without this a commit could be declared over rows the destination
/// had not been given yet — the receipt would claim more than landed.
#[tokio::test]
async fn accumulated_rows_are_written_before_each_commit() {
    let dest = memory::Destination::new();
    let mut config = Config::new("batch-vs-commit");
    // A threshold far above the whole run, so nothing would ever
    // flush on its own — only the commit boundary can force it.
    config = config
        .with_batch_policy(BatchPolicy::every_rows(1_000_000))
        .with_commit_policy(CommitPolicy::every_checkpoints(1));
    let report = Engine::new(config, three_batch_source(), dest.clone())
        .run()
        .await
        .expect("run");

    // Three checkpoints, each forcing its accumulation out first.
    assert_eq!(
        dest.write_sizes().len(),
        report.commits as usize,
        "each commit flushed exactly what had accumulated: {:?}",
        dest.write_sizes()
    );
    let total: u64 = report.tables.values().map(|t| t.rows).sum();
    assert_eq!(
        dest.committed_rows("s").len() as u64,
        total,
        "every accumulated row is committed, none stranded in the buffer"
    );
}

/// A schema Delta forces the ACCUMULATED buffer out BEFORE the wider
/// table is ensured: the loader's own
/// invariant — "a schema change forces the buffer out first" — must
/// hold at the Delta, not one batch later. A destination that widens
/// eagerly (DDL, file formats with per-part schemas) would otherwise
/// receive old-schema rows into an already-widened table, or — on a
/// run that ends right after the Delta — have `finish()` write them
/// after the new ensure.
mod delta_flushes_pending_first {
    use super::*;
    use std::sync::{Arc, Mutex};

    use rdlt_connector::arrow::RecordBatch;

    use rdlt_connector::core::commit::{CommitMeta, CommitReceipt};
    use rdlt_connector::core::state::StateDoc;

    use rdlt_connector::destination::{Capabilities, Destination, LoadSession, OpenContext};

    use rdlt_connector::error::DestinationError;
    use rdlt_testkit::memory;
    use serde_json::json;

    /// memory::Destination plus an ordered op log — the ONE observable
    /// this pin needs is whether a `write` or an `ensure` came first.
    #[derive(Clone)]
    struct Recording {
        inner: memory::Destination,
        ops: Arc<Mutex<Vec<String>>>,
    }

    struct RecordingSession {
        inner: Box<dyn LoadSession>,
        ops: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl Destination for Recording {
        async fn check(&self) -> Result<(), DestinationError> {
            Ok(())
        }
        fn spec(&self) -> rdlt_connector::spec::ConnectorSpec {
            self.inner.spec()
        }
        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
        async fn open(
            &self,
            context: OpenContext,
        ) -> Result<Box<dyn LoadSession>, DestinationError> {
            let inner = self.inner.open(context).await?;
            Ok(Box::new(RecordingSession {
                inner,
                ops: Arc::clone(&self.ops),
            }))
        }
    }

    #[async_trait::async_trait]
    impl LoadSession for RecordingSession {
        async fn ensure_table(
            &mut self,
            schema: &rdlt_connector::core::schema::TableSchema,
            mode: &rdlt_connector::core::commit::WriteMode,
        ) -> Result<(), DestinationError> {
            self.ops
                .lock()
                .expect("ops lock")
                .push(format!("ensure:{}", schema.columns.len()));
            self.inner.ensure_table(schema, mode).await
        }
        async fn write(
            &mut self,
            table: &rdlt_connector::core::id::TableName,
            batch: RecordBatch,
        ) -> Result<(), DestinationError> {
            self.ops
                .lock()
                .expect("ops lock")
                .push(format!("write:{}", batch.num_rows()));
            self.inner.write(table, batch).await
        }
        async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
            self.inner.commit(meta).await
        }
        async fn read_state(
            &mut self,
            pipeline: &rdlt_connector::core::id::PipelineId,
        ) -> Result<Option<StateDoc>, DestinationError> {
            self.inner.read_state(pipeline).await
        }
    }

    #[tokio::test]
    async fn pending_batches_flush_before_the_delta_is_ensured() {
        let source = memory::Source::new(vec![memory::Stream::new(
            rdlt_connector::source::StreamSpec::new("s"),
            vec![
                memory::Batch::new(vec![json!({"a": 1}), json!({"a": 2})]).with_checkpoint(1),
                memory::Batch::new(vec![json!({"a": 3, "b": "late"})]).with_checkpoint(2),
            ],
        )]);
        let dest = Recording {
            inner: memory::Destination::new(),
            ops: Arc::new(Mutex::new(Vec::new())),
        };
        let ops_handle = Arc::clone(&dest.ops);
        let mut config = Config::new("delta-flush");
        // Accumulate everything: only a schema change (or the end of
        // the run) may force a write out.
        config = config
            .with_batch_policy(BatchPolicy::every_rows(1_000_000))
            .with_commit_policy(CommitPolicy::every_checkpoints(1_000_000));
        Engine::new(config, source, dest).run().await.expect("run");

        let ops = ops_handle.lock().expect("ops lock").clone();
        let first_write = ops
            .iter()
            .position(|op| op.starts_with("write:"))
            .unwrap_or_else(|| panic!("some write must happen: {ops:?}"));
        let second_ensure = ops
            .iter()
            .enumerate()
            .filter(|(_, op)| op.starts_with("ensure:"))
            .map(|(i, _)| i)
            .nth(1)
            .unwrap_or_else(|| panic!("the evolution must ensure a second time: {ops:?}"));
        assert!(
            first_write < second_ensure,
            "the old-schema buffer must be written BEFORE the widened ensure — got {ops:?}"
        );
    }
}
