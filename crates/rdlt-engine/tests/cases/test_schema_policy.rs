//! US4 — Schema evolution under policy (spec acceptance scenarios 1–3).
//!
//! Evolve applies deltas and continues; Freeze fails typed and early (before any row
//! of the violating batch is written); Discard* loads conforming data and counts
//! every discard — never silent.

use rdlt_core::error::Error;
use rdlt_core::id::TableName;
use rdlt_engine::policy::{PolicyAction, SchemaPolicy};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory;
use serde_json::json;

use super::common::stream_with_batches;
use super::support::scripted;

fn two_batch_source(batch2_rows: Vec<serde_json::Value>) -> scripted::Source {
    stream_with_batches(
        rdlt_connector::source::StreamSpec::new("t"),
        vec![
            memory::Batch::new(vec![json!({"id": 1, "v": 10})]).with_checkpoint(1),
            memory::Batch::new(batch2_rows).with_checkpoint(2),
        ],
    )
}

/// Scenario 1: default evolve — a new mid-run column lands; earlier rows read null.
#[tokio::test]
async fn evolve_applies_new_columns() {
    let dest = memory::Destination::new();
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
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("freeze");
    config = config.with_schema_policy(SchemaPolicy::evolve().table("t", PolicyAction::Freeze));
    config = config.with_commit_policy(rdlt_core::commit::CommitPolicy::every_checkpoints(1));

    // Batch 2 both adds a column AND widens `v` — either alone must trip the freeze.
    let source = two_batch_source(vec![json!({"id": 2, "v": "not a number"})]);
    let err = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect_err("freeze must abort the run");
    match &err {
        Error::Schema(violation) => {
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
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("freeze-ok");
    config = config.with_schema_policy(SchemaPolicy::evolve().table("t", PolicyAction::Freeze));
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
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("discard-row");
    config = config.with_schema_policy(SchemaPolicy::evolve().table("t", PolicyAction::DiscardRow));

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
        rdlt_core::schema::ColumnType::scalar(rdlt_core::types::LogicalType::Int64)
    );

    let table = &report.tables[&TableName::new("t")];
    assert_eq!(table.discarded_rows, 2, "exact count, never silent");
    assert_eq!(table.rows, 2);
}

/// Scenario 3b: DiscardValue — the offending values null out; rows load; counts land.
#[tokio::test]
async fn discard_value_nulls_and_counts() {
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("discard-value");
    config =
        config.with_schema_policy(SchemaPolicy::evolve().table("t", PolicyAction::DiscardValue));

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
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("column-override");
    config = config.with_schema_policy(
        SchemaPolicy::evolve()
            .table("t", PolicyAction::Freeze)
            .column("t", "extra", PolicyAction::Evolve),
    );

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
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("cascade");
    config = config.with_schema_policy(
        SchemaPolicy::evolve().table("orders__items", PolicyAction::DiscardRow),
    );

    let source = stream_with_batches(
        rdlt_connector::source::StreamSpec::new("orders"),
        vec![
            // Batch 1 establishes the items schema (no `bad` column).
            memory::Batch::new(vec![json!({
                "id": 1,
                "items": [{"sku": "a", "tags": [{"t": "x"}]}]
            })]),
            // Batch 2's item carries a new column → DiscardRow fires on the item;
            // its tag (a grandchild of the root) must cascade away with it.
            memory::Batch::new(vec![json!({
                "id": 2,
                "items": [{"sku": "b", "bad": 1, "tags": [{"t": "y"}]}]
            })]),
        ],
    );
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

/// A frozen table must be frozen against EVERY shape of change, not only against
/// changes to columns it already has.
///
/// A new scalar field aborts the run; a new list-of-objects field creates and
/// loads a whole new child table. Both are drift on a frozen stream, and treating
/// only the first as drift makes the contract depend on which shape the new data
/// happens to have.
#[tokio::test]
async fn freeze_refuses_a_child_table_created_mid_run() {
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("freeze-child");
    config = config.with_schema_policy(SchemaPolicy::evolve().table("t", PolicyAction::Freeze));
    config = config.with_commit_policy(rdlt_core::commit::CommitPolicy::every_checkpoints(1));

    // Batch 2 introduces a nested collection, which materialises as a child table.
    let source = two_batch_source(vec![json!({"id": 2, "v": 20, "items": [{"sku": "a"}]})]);
    let err = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect_err("a new child table on a frozen stream is drift");
    match &err {
        Error::Schema(violation) => {
            assert!(
                violation.table.as_str().starts_with("t"),
                "the violation names the table that would have been created: {violation:?}"
            );
            assert_eq!(
                violation.column, None,
                "a table creation has no column to name"
            );
        }
        other => panic!("expected Schema(ContractViolation), got {other:?}"),
    }
    assert!(
        !dest.committed_tables().iter().any(|t| t.as_str() != "t"),
        "no child table is created"
    );
}

/// The freeze on a PARENT reaches the child tables its own data creates —
/// otherwise freezing `t` says nothing about `t`'s nested collections, and the
/// contract is only as strong as the shape of the first batch.
#[tokio::test]
async fn a_frozen_parent_freezes_the_child_tables_it_creates() {
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("freeze-inherit");
    config = config.with_schema_policy(SchemaPolicy::evolve().table("t", PolicyAction::Freeze));
    config = config.with_commit_policy(rdlt_core::commit::CommitPolicy::every_checkpoints(1));

    // Batch 1 establishes BOTH t and its child; batch 2 adds a column to the CHILD.
    let source = stream_with_batches(
        rdlt_connector::source::StreamSpec::new("t"),
        vec![
            memory::Batch::new(vec![json!({"id": 1, "items": [{"sku": "a"}]})]).with_checkpoint(1),
            memory::Batch::new(vec![json!({"id": 2, "items": [{"sku": "b", "qty": 3}]})])
                .with_checkpoint(2),
        ],
    );
    let err = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect_err("the parent's freeze governs its child tables");
    match &err {
        Error::Schema(violation) => {
            assert_eq!(violation.column.as_deref(), Some("qty"));
        }
        other => panic!("expected Schema(ContractViolation), got {other:?}"),
    }
}

/// The bootstrap is not drift: everything the FIRST drain creates is the
/// pipeline's initial shape, however many tables that is.
#[tokio::test]
async fn freeze_allows_the_tables_the_first_drain_establishes() {
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("freeze-bootstrap");
    config = config.with_schema_policy(SchemaPolicy::evolve().table("t", PolicyAction::Freeze));
    let source = stream_with_batches(
        rdlt_connector::source::StreamSpec::new("t"),
        vec![
            memory::Batch::new(vec![json!({"id": 1, "items": [{"sku": "a"}]})]).with_checkpoint(1),
            memory::Batch::new(vec![json!({"id": 2, "items": [{"sku": "b"}]})]).with_checkpoint(2),
        ],
    );
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("a frozen stream still establishes its own initial shape");
    assert_eq!(dest.committed_rows("t").len(), 2);
}

/// Discard means "load the conforming data and COUNT every discard". A table
/// created mid-run has no column to null and no prior shape to roll back to, so
/// discarding its creation means discarding its rows — counted, never silently
/// creating the table anyway.
#[tokio::test]
async fn discard_refuses_a_mid_run_child_table_and_counts_its_rows() {
    let dest = memory::Destination::new();
    let mut config = EngineConfig::new("discard-child");
    config = config.with_schema_policy(SchemaPolicy::evolve().table("t", PolicyAction::DiscardRow));
    config = config.with_commit_policy(rdlt_core::commit::CommitPolicy::every_checkpoints(1));

    let source = two_batch_source(vec![
        json!({"id": 2, "v": 20, "items": [{"sku": "a"}, {"sku": "b"}]}),
    ]);
    let report = Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("discard loads the conforming data");

    assert!(
        !dest
            .committed_tables()
            .iter()
            .any(|t| t.as_str().contains("items")),
        "the discarded table is not created: {:?}",
        dest.committed_tables()
    );
    let discarded: u64 = report.tables.values().map(|t| t.discarded_rows).sum();
    assert_eq!(discarded, 2, "both child rows are counted, never silent");
    // The parent's own rows are untouched — only the refused table is discarded.
    assert_eq!(dest.committed_rows("t").len(), 2);
}
