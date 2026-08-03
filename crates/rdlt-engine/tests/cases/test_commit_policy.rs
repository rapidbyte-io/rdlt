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
