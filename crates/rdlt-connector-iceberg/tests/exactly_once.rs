//! Exactly-once + state cells: replayed commits
//! publish NOTHING, incremental runs resume from the catalog-persisted
//! state doc, and receipts are readable from raw snapshot summaries
//! alone. Live against Polaris + RUSTFS, skip-not-fail.

mod common;

use common::CatalogFixture;
use rdlt_connector::core::{Cursor, LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector::{Destination, OpenCtx};
use rdlt_connector_iceberg::IcebergDest;
use rdlt_testkit::{batch_of, meta_for, schema_for};
use serde_json::json;

/// A crash-recovery replay — a FRESH session re-staging and
/// re-committing an identity that already landed — publishes NOTHING:
/// snapshot count unchanged, no duplicate rows referenced.
#[tokio::test(flavor = "multi_thread")]
async fn replayed_commit_publishes_nothing() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let dest = IcebergDest::from_config(fixture.config("replay")).expect("dest");
    let pipeline = PipelineId::new("replay-pipe");
    let load = LoadId::new("load-a");
    let table = TableName::new("events");

    // Commit #1 lands.
    let mut s1 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open s1");
    s1.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s1.write(&table, batch_of(&[1, 2, 3])).await.expect("write");
    s1.commit(meta_for(&pipeline, &load, 1))
        .await
        .expect("commit 1");
    let after_first = fixture.snapshot_summaries("replay", "events").await;
    assert_eq!(after_first.len(), 1);
    assert_eq!(after_first[0]["added-records"], "3");

    // Crash-recovery replay: fresh session, SAME (load, seq), same rows.
    let mut s2 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open replay session");
    s2.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure again");
    s2.write(&table, batch_of(&[1, 2, 3]))
        .await
        .expect("write replayed rows");
    let receipt = s2
        .commit(meta_for(&pipeline, &load, 1))
        .await
        .expect("replayed commit succeeds (converges, not errors)");
    assert_eq!(receipt.commit_seq, 1);

    let after_replay = fixture.snapshot_summaries("replay", "events").await;
    assert_eq!(
        after_replay.len(),
        1,
        "the replayed identity must publish no new snapshot"
    );

    // A genuinely NEW commit still lands on top.
    let mut s3 = dest
        .open(OpenCtx::new(pipeline.clone(), load.clone()))
        .await
        .expect("open s3");
    s3.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s3.write(&table, batch_of(&[4])).await.expect("write");
    s3.commit(meta_for(&pipeline, &load, 2))
        .await
        .expect("commit 2");
    let after_second = fixture.snapshot_summaries("replay", "events").await;
    assert_eq!(after_second.len(), 2, "the new identity appends normally");
    assert_eq!(after_second[1]["added-records"], "1");
}

/// A second engine run against the same catalog resumes from the
/// state doc persisted on the `_rdlt_state` marker table: the source
/// is handed the committed cursor and re-reads nothing.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_run_resumes_from_catalog_state() {
    use rdlt_connector::StreamSpec;
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let batches = || {
        vec![
            MemoryBatch::new(vec![json!({"seq": 1}), json!({"seq": 2})]).with_checkpoint(2),
            MemoryBatch::new(vec![json!({"seq": 3})]).with_checkpoint(3),
        ]
    };
    let source_for =
        |batches| MemorySource::new(vec![MemoryStream::new(StreamSpec::new("events"), batches)]);

    // Run 1: fresh — loads everything.
    let source1 = source_for(batches());
    let log1 = source1.since_log();
    let dest = IcebergDest::from_config(fixture.config("resume")).expect("dest");
    let report1 = Engine::new(EngineConfig::new("ice-resume"), source1, dest.clone())
        .run()
        .await
        .expect("run 1");
    assert_eq!(report1.total_rows(), 3);
    assert_eq!(log1.lock().unwrap()[0].1, None, "fresh run gets no cursor");
    let after_run1 = fixture.snapshot_summaries("resume", "events").await;
    let snapshots_run1 = after_run1.len();

    // Run 2: fresh source instance, same catalog — the engine must read
    // the state doc back from the marker table and pass cursor 3.
    let source2 = source_for(batches());
    let log2 = source2.since_log();
    let report2 = Engine::new(EngineConfig::new("ice-resume"), source2, dest)
        .run()
        .await
        .expect("run 2");
    assert_eq!(
        log2.lock().unwrap()[0].1,
        Some(Cursor::new(3)),
        "run 2 must resume from the catalog-persisted cursor"
    );
    assert_eq!(report2.total_rows(), 0, "no re-read of completed ranges");
    let after_run2 = fixture.snapshot_summaries("resume", "events").await;
    assert_eq!(
        after_run2.len(),
        snapshots_run1,
        "an all-caught-up run publishes no snapshot"
    );
}

/// Replace is the RECORDED narrowing — a typed "not supported" error at
/// ensure_table, not silent Append degradation, not a non-atomic
/// delete+append emulation. Merge is likewise typed-rejected, through a real
/// catalog session.
#[tokio::test(flavor = "multi_thread")]
async fn replace_rejected_against_live_catalog() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let dest = IcebergDest::from_config(fixture.config("replace_reject")).expect("dest");
    let mut session = dest
        .open(OpenCtx::new(PipelineId::new("p"), LoadId::new("l")))
        .await
        .expect("open");
    let err = session
        .ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect_err("Replace must be typed-unsupported");
    let text = format!("{err}");
    assert!(
        text.contains("Replace is not supported") && text.contains("overwrite"),
        "the narrowing names its reason: {text}"
    );
    let err = session
        .ensure_table(
            &schema_for("events"),
            &WriteMode::Merge {
                key: vec!["id".into()],
            },
        )
        .await
        .expect_err("Merge must be capability-rejected");
    assert!(format!("{err}").contains("does not support Merge"));
}

/// Live: a stream that DROPS a nullable column keeps loading —
/// the dropped column null-fills (the SQL destinations' tolerance)
/// instead of failing every subsequent run with a misleading error.
#[tokio::test(flavor = "multi_thread")]
async fn narrowed_stream_null_fills_dropped_nullable_column() {
    use rdlt_connector::StreamSpec;
    use rdlt_engine::{Engine, EngineConfig};
    use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let dest = IcebergDest::from_config(fixture.config("narrow")).expect("dest");
    // Run 1: the stream carries `email`.
    let run1 = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(vec![json!({"id": 1, "email": "a@x"})]).with_checkpoint(1)],
    )]);
    Engine::new(EngineConfig::new("ice-narrow"), run1, dest.clone())
        .run()
        .await
        .expect("run 1");
    // Run 2: the source dropped the column — must still load.
    let run2 = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1, "email": "a@x"})]).with_checkpoint(1),
            MemoryBatch::new(vec![json!({"id": 2})]).with_checkpoint(2),
        ],
    )]);
    let report = Engine::new(EngineConfig::new("ice-narrow"), run2, dest)
        .run()
        .await
        .expect("narrowed run loads");
    assert_eq!(report.total_rows(), 1, "only the new row");
    let snapshots = fixture.snapshot_summaries("narrow", "events").await;
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[1]["added-records"], "1");
}
