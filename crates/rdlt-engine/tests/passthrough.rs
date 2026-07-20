//! US2 — Arrow passthrough (spec acceptance scenarios + contract clauses S7/E7).
//!
//! Structured batches bypass the shredder: schema mapping + policy enforcement +
//! `_rdlt_load_id` stamping only.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use rdlt_connector::{ConnectorSpec, Cursor, ReadRequest, Source, SourceError, StreamSpec};
use rdlt_core::{PolicyAction, RdltError, SchemaPolicy, TableName};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{FaultPoint, FlakyDestination, MemoryDestination};
use serde_json::json;

/// Test source pushing pre-built Arrow batches, checkpointing after each.
struct ArrowSource {
    batches: Vec<RecordBatch>,
    declare_structured: bool,
}

#[async_trait]
impl Source for ArrowSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("arrow-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let spec = StreamSpec::new("metrics");
        Ok(vec![if self.declare_structured {
            spec.structured()
        } else {
            spec
        }])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let start = match &req.since {
            None => 0usize,
            Some(c) => c.as_value().as_u64().unwrap_or(0) as usize,
        };
        for (i, batch) in self.batches.iter().enumerate().skip(start) {
            if req.out.arrow(batch.clone()).await.is_err() {
                return Ok(());
            }
            if req
                .out
                .checkpoint(Cursor::new((i + 1) as u64))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

fn batch_ab(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .expect("batch")
}

fn batch_abc(ids: &[i64], names: &[&str], extra: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(StringArray::from(extra.to_vec())),
        ],
    )
    .expect("batch")
}

/// Scenario 1: contents and types preserved; every row stamped with the run id.
#[tokio::test]
async fn passthrough_preserves_data_and_stamps_load_id() {
    let dest = MemoryDestination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1, 2], &["a", "b"])],
        declare_structured: true,
    };
    let report = Engine::new(EngineConfig::new("pt"), source, dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(report.total_rows(), 2);

    let rows = dest.committed_rows("metrics");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["id"],
        json!(1),
        "Int64 preserved as a number, not re-inferred"
    );
    assert_eq!(rows[1]["name"], json!("b"));
    assert!(
        rows[0]["_rdlt_load_id"].as_str().is_some(),
        "run provenance stamped"
    );
    assert!(
        !rows[0].contains_key("_rdlt_id"),
        "structured streams carry NO per-row identity (clause E7)"
    );
}

/// Scenario 2a: a later batch adds a column → evolve extends the schema.
#[tokio::test]
async fn passthrough_schema_evolves_under_default_policy() {
    let dest = MemoryDestination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"]), batch_abc(&[2], &["b"], &["late"])],
        declare_structured: true,
    };
    Engine::new(EngineConfig::new("pt-evolve"), source, dest.clone())
        .run()
        .await
        .expect("run");
    let schema = dest.schema("metrics").expect("schema");
    assert!(
        schema.column("extra").is_some(),
        "column added by evolution"
    );
    let rows = dest.committed_rows("metrics");
    assert_eq!(rows[1]["extra"], json!("late"));
}

/// Scenario 2b: frozen table → typed failure naming table+column, nothing of the
/// violating batch published.
#[tokio::test]
async fn passthrough_freeze_rejects_before_publication() {
    let dest = MemoryDestination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"]), batch_abc(&[2], &["b"], &["late"])],
        declare_structured: true,
    };
    let mut config = EngineConfig::new("pt-freeze");
    config.schema_policy = SchemaPolicy::evolve().table("metrics", PolicyAction::Freeze);
    config.commit_policy = rdlt_core::CommitPolicy::EveryCheckpoints(1);

    let err = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect_err("freeze must fire");
    match &err {
        RdltError::Schema(violation) => {
            assert_eq!(violation.table, TableName::new("metrics"));
            assert_eq!(violation.column.as_deref(), Some("extra"));
        }
        other => panic!("expected Schema violation, got {other:?}"),
    }
    assert_eq!(
        dest.committed_rows("metrics").len(),
        1,
        "batch 2 unpublished"
    );
}

/// Clause S7: pushing Arrow on a stream NOT declared structured is rejected.
#[tokio::test]
async fn undeclared_arrow_push_is_rejected() {
    let dest = MemoryDestination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"])],
        declare_structured: false,
    };
    let err = Engine::new(EngineConfig::new("pt-undeclared"), source, dest)
        .run()
        .await
        .expect_err("must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("structured"),
        "error explains the S7 violation: {msg}"
    );
}

/// Structured segments participate in WAL crash recovery like any data (FR-009).
#[tokio::test]
async fn structured_segments_replay_from_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = MemoryDestination::new();
    let flaky = FlakyDestination::new(inner.clone(), FaultPoint::BeforeCommit(2));
    let mut config = EngineConfig::new("pt-crash");
    config.workdir = Some(dir.path().to_path_buf());
    config.commit_policy = rdlt_core::CommitPolicy::EveryCheckpoints(1);

    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"]), batch_ab(&[2], &["b"])],
        declare_structured: true,
    };
    Engine::new(config.clone(), source, flaky.clone())
        .run()
        .await
        .expect_err("injected crash");
    assert_eq!(inner.committed_rows("metrics").len(), 1, "span 1 only");

    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"]), batch_ab(&[2], &["b"])],
        declare_structured: true,
    };
    let report = Engine::new(config, source, flaky)
        .run()
        .await
        .expect("recovery");
    assert!(
        matches!(report.resumed_from, rdlt_core::ResumedFrom::Wal { .. }),
        "WAL replay, not re-extraction: {:?}",
        report.resumed_from
    );
    assert_eq!(
        inner.committed_rows("metrics").len(),
        2,
        "converged, no dupes"
    );
}

/// Clause B4: Merge on a structured stream is rejected at plan time — BEFORE the
/// destination is even opened.
#[tokio::test]
async fn merge_on_structured_stream_rejected_before_any_io() {
    let dest = MemoryDestination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"])],
        declare_structured: true,
    };
    let mut config = EngineConfig::new("pt-merge");
    config.write_mode = rdlt_core::WriteMode::Merge {
        key: vec!["id".into()],
    };
    let err = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect_err("B4 must reject");
    let msg = err.to_string();
    assert!(msg.contains("metrics"), "error names the stream: {msg}");
    assert!(msg.contains("Merge"), "error names the mode: {msg}");
    assert_eq!(dest.opens(), 0, "rejected before the destination opened");

    // Append is fine on the same stream.
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"])],
        declare_structured: true,
    };
    Engine::new(EngineConfig::new("pt-append"), source, dest)
        .run()
        .await
        .expect("append works");
}

/// A source column literally named `_rdlt_load_id` must be SUFFIXED, never aliased
/// with the system provenance column (regression: UniqueNamer::reserve).
#[tokio::test]
async fn input_column_named_like_system_column_is_suffixed() {
    use arrow::array::StringArray as SA;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_rdlt_load_id", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(SA::from(vec!["upstream-value"])),
        ],
    )
    .expect("batch");
    let dest = MemoryDestination::new();
    let source = ArrowSource {
        batches: vec![batch],
        declare_structured: true,
    };
    Engine::new(EngineConfig::new("pt-sysname"), source, dest.clone())
        .run()
        .await
        .expect("run");
    let rows = dest.committed_rows("metrics");
    let row = &rows[0];
    // The system column holds OUR load id; the upstream value lives in a suffixed column.
    assert_ne!(row["_rdlt_load_id"], json!("upstream-value"));
    let suffixed: Vec<&String> = row
        .keys()
        .filter(|k| k.starts_with("_rdlt_load_id_"))
        .collect();
    assert_eq!(
        suffixed.len(),
        1,
        "upstream column suffixed, got keys {:?}",
        row.keys()
    );
    assert_eq!(row[suffixed[0].as_str()], json!("upstream-value"));
}

/// Mutation-report closure: cross-batch NARROWING (Utf8 batch then Int64 batch)
/// must not narrow the registry schema — the column stays Utf8 and later
/// batches cast losslessly upward (clause E7). Kills the registry
/// widening-guard mutants at the observable level.
#[tokio::test]
async fn cross_batch_narrowing_keeps_the_wide_type() {
    use arrow::array::{Int64Array, StringArray};
    let batch1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["ten"]))],
    )
    .expect("batch1");
    let batch2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![11]))],
    )
    .expect("batch2");
    let dest = MemoryDestination::new();
    let source = ArrowSource {
        batches: vec![batch1, batch2],
        declare_structured: true,
    };
    Engine::new(EngineConfig::new("pt-narrow"), source, dest.clone())
        .run()
        .await
        .expect("run");
    let schema = dest.schema("metrics").expect("schema");
    let v = schema
        .columns
        .iter()
        .find(|c| c.name == "v")
        .expect("v column");
    assert_eq!(
        v.ty,
        rdlt_core::ColumnType::scalar(rdlt_core::LogicalType::Utf8),
        "narrowing must not shrink the registry type"
    );
    let rows = dest.committed_rows("metrics");
    assert_eq!(
        rows[1]["v"],
        serde_json::json!("11"),
        "int cast losslessly to text"
    );
}
