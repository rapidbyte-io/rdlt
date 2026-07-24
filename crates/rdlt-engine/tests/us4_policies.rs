//! US4 — Schema evolution under policy (spec acceptance scenarios 1–3).
//!
//! Evolve applies deltas and continues; Freeze fails typed and early (before any row
//! of the violating batch is written); Discard* loads conforming data and counts
//! every discard — never silent.

use rdlt_core::{PolicyAction, RdltError, SchemaPolicy, TableName};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::json;

fn two_batch_source(batch2_rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("t"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1, "v": 10})]).with_checkpoint(1),
            MemoryBatch::new(batch2_rows).with_checkpoint(2),
        ],
    )])
}

/// Scenario 1: default evolve — a new mid-run column lands; earlier rows read null.
#[tokio::test]
async fn evolve_applies_new_columns() {
    let dest = MemoryDestination::new();
    let source = two_batch_source(vec![json!({"id": 2, "v": 20, "extra": "late"})]);
    let report = Engine::new(EngineConfig::new("evolve"), source, dest.clone())
        .run()
        .await
        .expect("evolve run succeeds");
    assert_eq!(report.total_rows(), 2);
    let schema = dest.schema("t").expect("schema");
    assert!(schema.column("extra").is_some());
    let rows = dest.committed_rows("t");
    assert_eq!(rows[1]["extra"], json!("late"));
}

/// Scenario 2: frozen table — an incompatible change fails with a typed error naming
/// table and column, and NO row of the violating batch is published.
#[tokio::test]
async fn freeze_fails_fast_and_publishes_nothing_from_violating_batch() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("freeze");
    config.schema_policy = SchemaPolicy::evolve().table("t", PolicyAction::Freeze);
    config.commit_policy = rdlt_core::CommitPolicy::EveryCheckpoints(1);

    // Batch 2 both adds a column AND widens `v` — either alone must trip the freeze.
    let source = two_batch_source(vec![json!({"id": 2, "v": "not a number"})]);
    let err = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect_err("freeze must abort the run");
    match &err {
        RdltError::Schema(violation) => {
            assert_eq!(violation.table, TableName::new("t"));
            assert_eq!(violation.column.as_deref(), Some("v"));
        }
        other => panic!("expected Schema(ContractViolation), got {other:?}"),
    }
    // Batch 1 was committed (its checkpoint passed); batch 2 must be invisible.
    let rows = dest.committed_rows("t");
    assert_eq!(rows.len(), 1, "no row of the violating batch is published");
    assert_eq!(rows[0]["v"], json!(10));
}

/// Freeze does NOT fire when data conforms.
#[tokio::test]
async fn freeze_allows_conforming_data() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("freeze-ok");
    config.schema_policy = SchemaPolicy::evolve().table("t", PolicyAction::Freeze);
    let source = two_batch_source(vec![json!({"id": 2, "v": 20})]);
    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("conforming");
    assert_eq!(report.total_rows(), 2);
}

/// Scenario 3a: DiscardRow — non-conforming rows are dropped; conforming ones load;
/// the report carries exact counts.
#[tokio::test]
async fn discard_row_drops_and_counts() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("discard-row");
    config.schema_policy = SchemaPolicy::evolve().table("t", PolicyAction::DiscardRow);

    let source = two_batch_source(vec![
        json!({"id": 2, "v": "bad type"}),     // widens v → discarded
        json!({"id": 3, "v": 30}),             // conforming → loads
        json!({"id": 4, "v": 40, "extra": 1}), // new column → discarded
    ]);
    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");

    let rows = dest.committed_rows("t");
    assert_eq!(rows.len(), 2, "id 1 and id 3 only");
    let schema = dest.schema("t").expect("schema");
    assert!(
        schema.column("extra").is_none(),
        "schema must not evolve under discard"
    );
    assert_eq!(
        schema.column("v").expect("v").column_type,
        rdlt_core::ColumnType::scalar(rdlt_core::LogicalType::Int64)
    );

    let table = &report.tables[&TableName::new("t")];
    assert_eq!(table.discarded_rows, 2, "exact count, never silent");
    assert_eq!(table.rows, 2);
}

/// Scenario 3b: DiscardValue — the offending values null out; rows load; counts land.
#[tokio::test]
async fn discard_value_nulls_and_counts() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("discard-value");
    config.schema_policy = SchemaPolicy::evolve().table("t", PolicyAction::DiscardValue);

    let source = two_batch_source(vec![
        json!({"id": 2, "v": "bad type", "extra": "x"}),
        json!({"id": 3, "v": 30}),
    ]);
    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");

    let rows = dest.committed_rows("t");
    assert_eq!(rows.len(), 3, "all rows load");
    let row2 = rows.iter().find(|r| r["id"] == json!(2)).expect("row 2");
    assert_eq!(row2["v"], json!(null), "offending value nulled");
    let row3 = rows.iter().find(|r| r["id"] == json!(3)).expect("row 3");
    assert_eq!(row3["v"], json!(30));

    let table = &report.tables[&TableName::new("t")];
    assert_eq!(table.discarded_values, 2, "bad `v` + refused `extra`");
    assert_eq!(table.discarded_rows, 0);
}

/// Per-column override beats the table policy.
#[tokio::test]
async fn per_column_policy_overrides_table_policy() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("column-override");
    config.schema_policy = SchemaPolicy::evolve()
        .table("t", PolicyAction::Freeze)
        .column("t", "extra", PolicyAction::Evolve);

    // Adding `extra` is allowed by the column override even though `t` is frozen.
    let source = two_batch_source(vec![json!({"id": 2, "v": 20, "extra": "ok"})]);
    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(report.total_rows(), 2);
    assert!(dest.schema("t").expect("schema").column("extra").is_some());
}

/// Review finding #6 regression: DiscardRow on a MIDDLE table must cascade to
/// grandchildren (and deeper) — no orphaned rows with dangling lineage, and the
/// cascade is counted.
#[tokio::test]
async fn discard_row_on_middle_table_cascades_to_grandchildren() {
    let dest = MemoryDestination::new();
    let mut config = EngineConfig::new("cascade");
    config.schema_policy = SchemaPolicy::evolve().table("orders__items", PolicyAction::DiscardRow);

    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("orders"),
        vec![
            // Batch 1 establishes the items schema (no `bad` column).
            MemoryBatch::new(vec![json!({
                "id": 1,
                "items": [{"sku": "a", "tags": [{"t": "x"}]}]
            })]),
            // Batch 2's item carries a new column → DiscardRow fires on the item;
            // its tag (a grandchild of the root) must cascade away with it.
            MemoryBatch::new(vec![json!({
                "id": 2,
                "items": [{"sku": "b", "bad": 1, "tags": [{"t": "y"}]}]
            })]),
        ],
    )]);
    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("run");

    assert_eq!(dest.committed_rows("orders").len(), 2, "roots unaffected");
    let items = dest.committed_rows("orders__items");
    assert_eq!(items.len(), 1, "offending item dropped");
    assert_eq!(items[0]["sku"], json!("a"));
    let tags = dest.committed_rows("orders__items__tags");
    assert_eq!(
        tags.len(),
        1,
        "grandchild of the dropped item must NOT survive as an orphan"
    );
    assert_eq!(tags[0]["t"], json!("x"));

    let items_report = &report.tables[&TableName::new("orders__items")];
    assert_eq!(items_report.discarded_rows, 1);
    let tags_report = &report.tables[&TableName::new("orders__items__tags")];
    assert_eq!(
        tags_report.discarded_rows, 1,
        "cascade drops are counted, never silent"
    );
}
