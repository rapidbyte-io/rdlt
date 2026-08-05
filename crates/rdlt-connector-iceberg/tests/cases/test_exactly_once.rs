//! Exactly-once, live: replays publish nothing, resume reads catalog
//! state, a narrowed stream null-fills, and Replace refuses against a
//! real catalog.

use rdlt_connector_iceberg::destination::Shell;
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_sdk::spi::core::WriteMode;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::{CatalogFixture, minimal_doc};

fn one_batch(rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(rows).with_checkpoint(1)],
    )])
}

/// A RESUMED pipeline redelivers nothing: run 2 reads run 1's state
/// from the CATALOG (the marker table) and the source resumes past
/// its checkpoint — totals stay exact.
#[tokio::test]
async fn a_resumed_pipeline_republishes_nothing() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "resume_v2";
    let doc = fixture.doc(namespace);
    let workdir = tempfile::tempdir().expect("workdir");
    let wal = workdir.path().join("wal");

    for _ in 0..2 {
        Engine::new(
            EngineConfig::new("ice-resume-v2").with_workdir(wal.clone()),
            one_batch(vec![json!({"id": 1}), json!({"id": 2})]),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");
    }

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(
        total, 2,
        "run 2 resumed past the checkpoint and republished nothing: {summaries:?}"
    );

    // The raw state protocol behind the resume: the marker table holds
    // this pipeline's scope key.
    use rdlt_connector_iceberg::destination::{Config, testhook};
    use rdlt_connector_sdk::config::Document;
    let config = Config::from_value(doc).expect("valid");
    let raw = testhook::read_raw_state(
        &config,
        &[namespace.to_owned()],
        &testhook::scope_of("ice-resume-v2"),
    )
    .await
    .expect("state readable")
    .expect("state present after a committed load");
    assert!(
        raw.contains("ice-resume-v2"),
        "the doc names its pipeline: {raw}"
    );
}

/// 037 D1, live: state stranded under the pre-037 12-hex scope key
/// REFUSES typed rather than being silently treated as a fresh
/// pipeline. Run 1 writes state at the current 32-hex key the normal
/// way; the state property is then relocated to the legacy key
/// (`testhook::move_state_to_legacy_key`, simulating a warehouse never
/// touched since the widen); run 2 must refuse with the frozen
/// spelling, and — the defect this guards — must NOT duplicate run
/// 1's rows by re-appending them under a fresh (empty) resume state.
///
/// Red-proved against the unfixed code: with the legacy-key probe
/// absent, `read_state` sees nothing at the 32-hex key, agrees this is
/// a first run, and the engine runs fresh — Append republishes run 1's
/// two rows a second time (4 total, not 2), and no refusal ever
/// surfaces.
#[tokio::test]
async fn state_stranded_under_the_legacy_scope_key_refuses_typed() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "legacy_scope_v2";
    let doc = fixture.doc(namespace);
    let pipeline = "ice-legacy-scope";
    let workdir = tempfile::tempdir().expect("workdir");

    Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal1")),
        one_batch(vec![json!({"id": 1}), json!({"id": 2})]),
        Shell::from_value(doc.clone()).expect("valid"),
    )
    .run()
    .await
    .expect("run 1 settles and writes state at the current 32-hex key");

    use rdlt_connector_iceberg::destination::{Config, testhook};
    use rdlt_connector_sdk::config::Document;
    let config = Config::from_value(doc.clone()).expect("valid");
    testhook::move_state_to_legacy_key(&config, &[namespace.to_owned()], pipeline)
        .await
        .expect("state relocated to the legacy 12-hex key");

    let err = Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal2")),
        one_batch(vec![json!({"id": 3}), json!({"id": 4})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("run 2 must refuse rather than silently re-loading from zero");
    let text = format!("{err}");
    assert!(
        text.contains(&format!("state for pipeline `{pipeline}`"))
            && text.contains("pre-037 12-hex scope key")
            && text.contains("this build reads 32-hex scope keys")
            && text.contains("rather than silently re-loading from zero"),
        "{text}"
    );

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(
        total, 2,
        "run 2 refused before writing anything — run 1's rows stay unduplicated: {summaries:?}"
    );
}

/// A REPLAYED commit publishes nothing: the same (load, seq)
/// redelivered through the SPI — a fresh session staging the same
/// batch — converges on the snapshot history instead of appending a
/// second copy.
#[tokio::test]
async fn a_replayed_commit_publishes_nothing() {
    use std::sync::Arc;

    use rdlt_connector_sdk::spi::core::{
        ColumnDef, ColumnType, LoadId, LogicalType, PipelineId, Provenance, StateDoc, TableName,
        TableSchema, WriteMode,
    };
    use rdlt_connector_sdk::spi::{CommitMeta, Destination, OpenContext};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "spi_replay_v2";
    let shell = Shell::from_value(fixture.doc(namespace)).expect("valid");

    let schema = TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            column_type: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    };
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2]))],
    )
    .expect("batch");
    let pipeline = PipelineId::from("ice-spi-replay");
    let load = LoadId::from("load-replay-1");
    let meta = || CommitMeta {
        load_id: load.clone(),
        commit_seq: 1,
        state: StateDoc::new(pipeline.clone(), "test"),
        counters: Default::default(),
    };

    // The same (load, seq) staged and published by TWO sessions — the
    // second is the redelivery.
    for _ in 0..2 {
        let mut session = shell
            .open(OpenContext::new(pipeline.clone(), load.clone()))
            .await
            .expect("open");
        session
            .ensure_table(&schema, &WriteMode::Append)
            .await
            .expect("ensure");
        session
            .write(&TableName::from("events"), batch.clone())
            .await
            .expect("write");
        session.commit(meta()).await.expect("commit");
    }

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(
        summaries.len(),
        1,
        "the replay published NOTHING: {summaries:?}"
    );
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(total, 2, "one copy of the rows: {summaries:?}");
}

/// A stream that stops providing a nullable column keeps loading —
/// the absent column null-fills against the live table.
#[tokio::test]
async fn a_narrowed_stream_null_fills_the_missing_column() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "narrow_v2";
    let doc = fixture.doc(namespace);
    let workdir = tempfile::tempdir().expect("workdir");

    Engine::new(
        EngineConfig::new("ice-narrow-1").with_workdir(workdir.path().join("wal1")),
        one_batch(vec![json!({"id": 1, "note": "present"})]),
        Shell::from_value(doc.clone()).expect("valid"),
    )
    .run()
    .await
    .expect("run 1 settles");

    Engine::new(
        EngineConfig::new("ice-narrow-2").with_workdir(workdir.path().join("wal2")),
        one_batch(vec![json!({"id": 2})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("run 2 settles despite the narrowed stream");

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(summaries.len(), 2, "{summaries:?}");
}

/// Replace refuses LIVE with the frozen spelling — before any table
/// is touched.
#[tokio::test]
async fn replace_is_refused_against_the_live_catalog() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let doc = fixture.doc("replace_v2");
    let workdir = tempfile::tempdir().expect("workdir");
    let err = Engine::new(
        EngineConfig::new("ice-replace-v2")
            .with_workdir(workdir.path().join("wal"))
            .with_write_mode(WriteMode::Replace),
        one_batch(vec![json!({"id": 1})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("Replace must refuse");
    let text = format!("{err}");
    assert!(
        text.contains("Replace is not supported")
            && text.contains("use Append, or a SQL destination for replace semantics"),
        "{text}"
    );
}

/// OFFLINE (no fixture, no catalog reachable): the engine refuses a
/// no-workdir run against this destination before it ever connects.
/// `capabilities()` is a synchronous, pre-connect call on the
/// connector (029 N2 / 037 US3); `validate_streams` reads it and
/// returns before `Destination::open` — so this pin exercises the real
/// `Iceberg` connector's declared `requires_durable_identity` and the
/// engine's refusal with no live catalog at all.
#[tokio::test]
async fn a_no_workdir_run_is_refused_before_any_catalog_connection() {
    let err = Engine::new(
        EngineConfig::new("ice-no-workdir"),
        one_batch(vec![json!({"id": 1})]),
        Shell::from_value(minimal_doc()).expect("valid"),
    )
    .run()
    .await
    .expect_err("N2: no workdir means duplication on mid-publish retry");
    let text = format!("{err}");
    assert!(text.contains("requires a workdir"), "{text}");
}
