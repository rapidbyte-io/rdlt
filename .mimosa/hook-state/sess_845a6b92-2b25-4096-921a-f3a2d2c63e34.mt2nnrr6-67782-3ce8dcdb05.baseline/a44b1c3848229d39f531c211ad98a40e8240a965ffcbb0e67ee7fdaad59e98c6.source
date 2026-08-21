//! Arrow passthrough.
//!
//! Structured batches bypass the shredder: schema mapping + policy enforcement +
//! `_rdlt_load_id` stamping only.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use rdlt_connector::core::cursor::Cursor;
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;
use rdlt_core::error::Error;
use rdlt_core::id::TableName;
use rdlt_core::schema;
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;
use rdlt_engine::policy::{PolicyAction, SchemaPolicy};
use rdlt_testkit::memory;
use serde_json::json;

use super::support::crash::{CrashDestination, FaultPoint};

/// Test source pushing pre-built Arrow batches, checkpointing after each.
struct ArrowSource {
    batches: Vec<RecordBatch>,
    declare_structured: bool,
}

#[async_trait]
impl Source for ArrowSource {
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("arrow-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let spec = StreamSpec::new("metrics");
        Ok(vec![if self.declare_structured {
            spec.with_structured()
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
    let dest = memory::Destination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1, 2], &["a", "b"])],
        declare_structured: true,
    };
    let report = Engine::new(Config::new("pt"), source, dest.clone())
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
        rows[0][schema::system::LOAD_ID].as_str().is_some(),
        "run provenance stamped"
    );
    assert!(
        !rows[0].contains_key(schema::system::ID),
        "structured streams carry NO per-row identity"
    );
}

/// Scenario 2a: a later batch adds a column → evolve extends the schema.
#[tokio::test]
async fn passthrough_schema_evolves_under_default_policy() {
    let dest = memory::Destination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"]), batch_abc(&[2], &["b"], &["late"])],
        declare_structured: true,
    };
    Engine::new(Config::new("pt-evolve"), source, dest.clone())
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
    let dest = memory::Destination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"]), batch_abc(&[2], &["b"], &["late"])],
        declare_structured: true,
    };
    let mut config = Config::new("pt-freeze");
    config =
        config.with_schema_policy(SchemaPolicy::evolve().table("metrics", PolicyAction::Freeze));
    config = config.with_commit_policy(rdlt_core::commit::CommitPolicy::every_checkpoints(1));

    let err = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect_err("freeze must fire");
    match &err {
        Error::Schema(violation) => {
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

/// Pushing Arrow on a stream NOT declared structured is rejected.
#[tokio::test]
async fn undeclared_arrow_push_is_rejected() {
    let dest = memory::Destination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"])],
        declare_structured: false,
    };
    let err = Engine::new(Config::new("pt-undeclared"), source, dest)
        .run()
        .await
        .expect_err("must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("structured"),
        "error explains the violation: {msg}"
    );
}

/// Structured segments participate in WAL crash recovery like any data.
#[tokio::test]
async fn structured_segments_replay_from_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let inner = memory::Destination::new();
    let flaky = CrashDestination::new(inner.clone(), FaultPoint::BeforeCommit(2));
    let mut config = Config::new("pt-crash");
    config = config.with_workdir(dir.path().to_path_buf());
    config = config.with_commit_policy(rdlt_core::commit::CommitPolicy::every_checkpoints(1));

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
        matches!(
            report.resumed_from,
            rdlt_core::report::ResumedFrom::Wal { .. }
        ),
        "WAL replay, not re-extraction: {:?}",
        report.resumed_from
    );
    assert_eq!(
        inner.committed_rows("metrics").len(),
        2,
        "converged, no dupes"
    );
}

/// Keyless Merge on a structured stream is rejected at plan time — BEFORE the
/// destination is even opened.
#[tokio::test]
async fn merge_on_structured_stream_rejected_before_any_io() {
    let dest = memory::Destination::new();
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"])],
        declare_structured: true,
    };
    let mut config = Config::new("pt-merge");
    config = config.with_write_mode(rdlt_core::commit::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let err = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect_err("keyless structured merge must reject");
    let msg = err.to_string();
    assert!(msg.contains("metrics"), "error names the stream: {msg}");
    assert!(msg.contains("Merge"), "error names the mode: {msg}");
    assert_eq!(dest.opens(), 0, "rejected before the destination opened");

    // Append is fine on the same stream.
    let source = ArrowSource {
        batches: vec![batch_ab(&[1], &["a"])],
        declare_structured: true,
    };
    Engine::new(Config::new("pt-append"), source, dest)
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
            Field::new(schema::system::LOAD_ID, DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(SA::from(vec!["upstream-value"])),
        ],
    )
    .expect("batch");
    let dest = memory::Destination::new();
    let source = ArrowSource {
        batches: vec![batch],
        declare_structured: true,
    };
    Engine::new(Config::new("pt-sysname"), source, dest.clone())
        .run()
        .await
        .expect("run");
    let rows = dest.committed_rows("metrics");
    let row = &rows[0];
    // The system column holds OUR load id; the upstream value lives in a suffixed column.
    assert_ne!(row[schema::system::LOAD_ID], json!("upstream-value"));
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
/// batches cast losslessly upward. Kills the registry
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
    let dest = memory::Destination::new();
    let source = ArrowSource {
        batches: vec![batch1, batch2],
        declare_structured: true,
    };
    Engine::new(Config::new("pt-narrow"), source, dest.clone())
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
        v.column_type,
        rdlt_core::schema::ColumnType::scalar(rdlt_core::types::LogicalType::Utf8),
        "narrowing must not shrink the registry type"
    );
    let rows = dest.committed_rows("metrics");
    assert_eq!(
        rows[1]["v"],
        serde_json::json!("11"),
        "int cast losslessly to text"
    );
}

// ---- Keyed structured merge ----

/// Structured source that DECLARES a primary key, making it merge-eligible
/// under the keyed structured-merge rule.
struct KeyedArrowSource {
    batches: Vec<RecordBatch>,
    key: Vec<String>,
}

#[async_trait]
impl Source for KeyedArrowSource {
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("keyed-arrow-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new("metrics")
                .with_structured()
                .with_primary_key(self.key.clone()),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        for batch in &self.batches {
            if req.out.arrow(batch.clone()).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

fn merge_config(pipeline: &str, key: &[&str]) -> Config {
    let mut config = Config::new(pipeline);
    config = config.with_write_mode(rdlt_core::commit::WriteMode::Merge {
        key: key.iter().map(|k| (*k).to_string()).collect(),
    });
    config
}

/// Structured + declared key + Merge{same key} is ACCEPTED, and
/// re-delivered keys converge to one row per key (last wins).
#[tokio::test]
async fn keyed_structured_merge_accepted_and_converges() {
    let dest = memory::Destination::new();
    let source = KeyedArrowSource {
        batches: vec![batch_ab(&[1, 2], &["a", "b"])],
        key: vec!["id".into()],
    };
    Engine::new(merge_config("pt-kmerge", &["id"]), source, dest.clone())
        .run()
        .await
        .expect("keyed merge accepted");
    assert_eq!(dest.committed_rows("metrics").len(), 2);

    // Second load updates key 2 and adds key 3: no duplicates, updated value.
    let source = KeyedArrowSource {
        batches: vec![batch_ab(&[2, 3], &["b2", "c"])],
        key: vec!["id".into()],
    };
    Engine::new(merge_config("pt-kmerge", &["id"]), source, dest.clone())
        .run()
        .await
        .expect("merge run 2");
    let rows = dest.committed_rows("metrics");
    assert_eq!(rows.len(), 3, "one row per key");
    let row2 = rows
        .iter()
        .find(|r| r["id"] == json!(2))
        .expect("key 2 present");
    assert_eq!(row2["name"], json!("b2"), "merge took the updated value");
}

/// The key is a SET — a reordered composite key is the same key
/// (reflection returns attnum order, users write DDL order).
#[tokio::test]
async fn reordered_composite_merge_key_is_accepted() {
    let dest = memory::Destination::new();
    let source = KeyedArrowSource {
        batches: vec![batch_ab(&[1], &["a"])],
        key: vec!["id".into(), "name".into()],
    };
    Engine::new(
        merge_config("pt-kreorder", &["name", "id"]),
        source,
        dest.clone(),
    )
    .run()
    .await
    .expect("reordered composite key accepted");
    assert_eq!(dest.committed_rows("metrics").len(), 1);
}

/// The Merge key must EQUAL the declared primary_key.
#[tokio::test]
async fn merge_key_mismatch_rejected_at_plan_time() {
    let dest = memory::Destination::new();
    let source = KeyedArrowSource {
        batches: vec![batch_ab(&[1], &["a"])],
        key: vec!["id".into()],
    };
    let err = Engine::new(merge_config("pt-kmm", &["name"]), source, dest.clone())
        .run()
        .await
        .expect_err("key mismatch must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("primary_key"), "{msg}");
    assert_eq!(dest.opens(), 0, "rejected before the destination opened");
}

/// Write-time guard: a NULL in a merge-key column is a typed error naming the
/// column — keys are identities.
#[tokio::test]
async fn null_in_merge_key_is_a_typed_write_time_error() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .expect("batch");
    let dest = memory::Destination::new();
    let source = KeyedArrowSource {
        batches: vec![batch],
        key: vec!["id".into()],
    };
    let err = Engine::new(merge_config("pt-knull", &["id"]), source, dest)
        .run()
        .await
        .expect_err("NULL key must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("id") && msg.to_lowercase().contains("null"),
        "error names the key column: {msg}"
    );
}

/// A refused column is projected AWAY, and its loss is counted per value.
///
/// On a structured stream a Discard policy cannot filter rows — the batch is
/// already columnar — so it projects the refused COLUMN out instead. Two things
/// about that were unpinned, and both are quiet failures:
///
/// The projection predicate decides which columns SURVIVE. Invert it and the
/// engine keeps exactly the refused column and drops every accepted one — total
/// data loss, reported as a successful run.
///
/// The value count is `rows × refused columns`. It is the only number saying
/// how much was lost, and any other arithmetic on those two operands still
/// produces a plausible-looking figure.
#[tokio::test]
async fn a_refused_column_is_projected_away_and_counted_per_value() {
    let dest = memory::Destination::new();
    let mut config = Config::new("passthrough-discard");
    config = config.with_schema_policy(SchemaPolicy::with_default(PolicyAction::DiscardValue));

    // Batch 1 establishes {id, name}. Batch 2 adds TWO refused columns across
    // four rows: 4 × 2 = 8 lost values. Two columns and a row count that is not
    // a multiple of it are both deliberate — with one column, `rows * cols` and
    // `rows / cols` produce the SAME number and the arithmetic is untestable.
    let wide = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
            Field::new("spare", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3i64, 4, 5, 6])),
            Arc::new(StringArray::from(vec!["c", "d", "e", "f"])),
            Arc::new(StringArray::from(vec!["x", "y", "z", "w"])),
            Arc::new(StringArray::from(vec!["p", "q", "r", "s"])),
        ],
    )
    .expect("wide batch");
    let source = ArrowSource {
        batches: vec![batch_ab(&[1, 2], &["a", "b"]), wide],
        declare_structured: true,
    };
    let engine = Engine::new(config, source, dest.clone());
    let mut events = engine.events();
    let report = engine.run().await.expect("run");

    // The ACCEPTED columns survived and the refused one did not.
    let rows = dest.committed_rows("metrics");
    assert_eq!(rows.len(), 6, "every row still loads: {}", rows.len());
    let schema = dest.schema("metrics").expect("schema");
    let columns: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(columns.contains(&"id"), "accepted column kept: {columns:?}");
    assert!(
        columns.contains(&"name"),
        "accepted column kept: {columns:?}"
    );
    for refused in ["extra", "spare"] {
        assert!(
            !columns.contains(&refused),
            "the refused column `{refused}` must be projected AWAY: {columns:?}"
        );
    }

    // …and the loss is counted as rows × refused columns, from the batch that
    // carried it.
    let mut counted = 0u64;
    while let Some(event) = events.recv().await {
        if let rdlt_core::event::PipelineEvent::Discarded { values, .. } = event {
            counted += values;
        }
    }
    assert_eq!(
        counted, 8,
        "four rows × two refused columns — neither the sum (6) nor the quotient (2)"
    );
    let reported: u64 = report.tables.values().map(|t| t.discarded_values).sum();
    assert_eq!(reported, 8, "the report agrees with the events");
}
