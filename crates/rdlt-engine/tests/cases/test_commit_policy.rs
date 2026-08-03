//! The commit cadence: `CommitPolicy` decides WHEN the loader commits, and its
//! boundaries are exact. Mutation-report closure; the doc names the mutant
//! class it kills.

use rdlt_core::CommitPolicy;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::MemoryDestination;

use super::common::three_batch_source;

/// Kills: `policy_triggers` `>=`→`<` boundaries and `EveryCheckpoints` counting.
/// 3 checkpoints under EveryCheckpoints(2): commit fires at checkpoint 2, the
/// trailing work commits in finish() — exactly 2 commits.
#[tokio::test]
async fn commit_policy_boundaries_are_exact() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("policy");
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
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("policy-bytes");
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
    use rdlt_core::BatchPolicy;

    // Baseline: no policy, one write per source batch.
    let dest = MemoryDestination::new();
    let config = EngineConfig::new("batch-none");
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
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("batch-coalesced");
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
    let total = |r: &rdlt_core::RunReport| r.tables.values().map(|t| t.rows).sum::<u64>();
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
    use rdlt_core::BatchPolicy;

    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("batch-vs-commit");
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
