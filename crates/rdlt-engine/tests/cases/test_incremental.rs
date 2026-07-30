//! US2 — Incremental sync (spec acceptance scenarios).
//!
//! Second runs move only new data; merge replaces a record's whole subtree.

use rdlt_connector::StreamSpec;
use rdlt_core::{CommitPolicy, Cursor, WriteMode};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::json;

use super::common::{stream_with_batches, three_batch_source};

/// Scenario 1: a second run resumes from the last committed cursor — the source is
/// never asked to re-read completed ranges.
#[tokio::test]
async fn second_run_resumes_from_committed_cursor() {
    let batches = || {
        vec![
            MemoryBatch::new(vec![json!({"seq": 1}), json!({"seq": 2})]).with_checkpoint(2),
            MemoryBatch::new(vec![json!({"seq": 3})]).with_checkpoint(3),
        ]
    };
    let dest = MemoryDestination::new();

    // Run 1: fresh — reads everything, commits per checkpoint.
    let source1 = stream_with_batches(rdlt_connector::StreamSpec::new("events"), batches());
    let log1 = source1.since_log();
    let mut config = EngineConfig::new("incr");
    config = config.with_commit_policy(CommitPolicy::EveryCheckpoints(1));
    let report1 = Engine::new(config.clone(), source1, dest.clone())
        .run()
        .await
        .expect("run 1");
    assert_eq!(report1.total_rows(), 3);
    assert_eq!(
        report1.commits, 2,
        "one commit per checkpoint under EveryCheckpoints(1)"
    );
    assert_eq!(log1.lock().unwrap()[0].1, None, "fresh run gets no cursor");
    assert_eq!(
        dest.state()
            .expect("state persisted")
            .cursors
            .values()
            .next(),
        Some(&Cursor::new(3))
    );

    // Run 2: same destination, fresh source instance — MUST be told to resume from 3
    // and therefore emit nothing new.
    let source2 = stream_with_batches(rdlt_connector::StreamSpec::new("events"), batches());
    let log2 = source2.since_log();
    let report2 = Engine::new(config, source2, dest.clone())
        .run()
        .await
        .expect("run 2");
    assert_eq!(
        log2.lock().unwrap()[0].1,
        Some(Cursor::new(3)),
        "engine must pass the committed cursor as `since`"
    );
    assert_eq!(report2.total_rows(), 0, "no re-read of completed ranges");
    assert_eq!(dest.committed_rows("events").len(), 3, "no duplicates");
    assert!(matches!(
        report2.resumed_from,
        rdlt_core::ResumedFrom::Cursor
    ));
}

/// Scenario 2: merge replaces the updated record AND its entire child subtree.
#[tokio::test]
async fn merge_replaces_whole_subtree() {
    let spec = || rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]);
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("merge");
    config = config.with_write_mode(WriteMode::Merge {
        key: vec!["id".into()],
    });

    // Run 1: user 1 has two emails; user 2 has one.
    let source = MemorySource::single_stream(
        spec(),
        vec![
            json!({"id": 1, "name": "ada", "emails": [{"addr": "old-a"}, {"addr": "old-b"}]}),
            json!({"id": 2, "name": "grace", "emails": [{"addr": "g"}]}),
        ],
    );
    Engine::new(config.clone(), source, dest.clone())
        .run()
        .await
        .expect("run 1");
    assert_eq!(dest.committed_rows("users").len(), 2);
    assert_eq!(dest.committed_rows("users__emails").len(), 3);

    // Run 2: user 1 updated with ONE new email. Old children must vanish; user 2 and
    // its child stay untouched.
    let source = MemorySource::single_stream(
        spec(),
        vec![json!({"id": 1, "name": "ada lovelace", "emails": [{"addr": "new"}]})],
    );
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run 2");

    let users = dest.committed_rows("users");
    assert_eq!(users.len(), 2, "merge upserts, never appends");
    let ada = users
        .iter()
        .find(|u| u["id"] == json!(1))
        .expect("user 1 present");
    assert_eq!(ada["name"], json!("ada lovelace"));

    let emails = dest.committed_rows("users__emails");
    let addrs: Vec<&str> = emails.iter().map(|e| e["addr"].as_str().unwrap()).collect();
    assert_eq!(emails.len(), 2, "old-a and old-b must be deleted");
    assert!(
        addrs.contains(&"new") && addrs.contains(&"g"),
        "got {addrs:?}"
    );
}

/// Keyless merge: byte-identical rows collapse to one (documented dedup semantics).
#[tokio::test]
async fn keyless_merge_dedups_identical_rows() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("dedup");
    config = config.with_write_mode(WriteMode::Merge { key: vec![] }); // keyless: content-hash id
    // NOTE: empty key is engine-legal for content dedup; the facade requires an
    // explicit key and that stays — this exercises the engine layer directly.

    let rows = vec![json!({"x": 1}), json!({"x": 1}), json!({"x": 2})];
    let source = MemorySource::single_stream(rdlt_connector::StreamSpec::new("t"), rows.clone());
    Engine::new(config.clone(), source, dest.clone())
        .run()
        .await
        .expect("run 1");
    assert_eq!(dest.committed_rows("t").len(), 2, "identical rows collapse");

    // Re-delivering the same rows (redelivery window) changes nothing.
    let source = MemorySource::single_stream(rdlt_connector::StreamSpec::new("t"), rows);
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run 2");
    assert_eq!(
        dest.committed_rows("t").len(),
        2,
        "effective exactly-once via dedup"
    );
}

/// Replace mode: this run's rows replace the table; multiple commits within one run
/// still land as ONE replacement, not repeated truncation.
#[tokio::test]
async fn replace_mode_replaces_per_run_not_per_commit() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("replace");
    config = config.with_write_mode(WriteMode::Replace);
    config = config.with_commit_policy(CommitPolicy::EveryCheckpoints(1));

    let batches = vec![
        MemoryBatch::new(vec![json!({"v": "run1-a"})]).with_checkpoint(1),
        MemoryBatch::new(vec![json!({"v": "run1-b"})]).with_checkpoint(2),
    ];
    let source = stream_with_batches(rdlt_connector::StreamSpec::new("t"), batches);
    Engine::new(config.clone(), source, dest.clone())
        .run()
        .await
        .expect("run 1");
    assert_eq!(
        dest.committed_rows("t").len(),
        2,
        "second commit must not re-truncate"
    );

    // A later full run replaces everything again. (No cursor: replace typically pairs
    // with full re-reads.)
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("t"),
        vec![json!({"v": "run2"})],
    );
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run 2");
    let rows = dest.committed_rows("t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["v"], json!("run2"));
}

/// Kills: the fresh-run resume guard (`!state.cursors.is_empty()`→true) — a
/// pipeline with NO committed state must pass `since: None`.
#[tokio::test]
async fn fresh_run_reads_with_no_cursor() {
    let dest = MemoryDestination::new();
    let source = three_batch_source();
    let since_log = source.since_log();
    Engine::new(EngineConfig::new("fresh"), source, dest)
        .run()
        .await
        .expect("run");
    let log = since_log.lock().expect("log");
    assert_eq!(log.len(), 1);
    assert!(
        log[0].1.is_none(),
        "fresh pipeline must not invent a cursor"
    );
}

/// Kills: the resumed-from guard (`!state.cursors.is_empty()`→true) — a
/// recovered state whose cursors are EMPTY (first run never checkpointed) must
/// report `Fresh`, not `Cursor`.
#[tokio::test]
async fn empty_cursor_state_reports_fresh_resume() {
    use rdlt_core::ResumedFrom;
    let dest = MemoryDestination::new();
    // No checkpoints: state commits with an empty cursor map.
    let stream = MemoryStream::new(
        StreamSpec::new("s"),
        vec![MemoryBatch::new(vec![json!({"id": 1})])],
    );
    let report = Engine::new(
        EngineConfig::new("nocursor"),
        MemorySource::new(vec![stream]),
        dest.clone(),
    )
    .run()
    .await
    .expect("run 1");
    assert_eq!(report.resumed_from, ResumedFrom::Fresh);

    // Second run recovers a StateDoc — but with no cursors it is still Fresh.
    let stream = MemoryStream::new(
        StreamSpec::new("s"),
        vec![MemoryBatch::new(vec![json!({"id": 2})])],
    );
    let report = Engine::new(
        EngineConfig::new("nocursor"),
        MemorySource::new(vec![stream]),
        dest,
    )
    .run()
    .await
    .expect("run 2");
    assert_eq!(
        report.resumed_from,
        ResumedFrom::Fresh,
        "empty cursors must never report a cursor resume"
    );
}
