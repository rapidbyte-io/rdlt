//! US1 — First full sync (spec acceptance scenarios 1–3).
//!
//! Nested, heterogeneous records flow memory→memory into typed tables with inferred
//! schemas, child-table splitting, and `_rdlt_*` lineage. No schema declared, Append.

use rdlt_core::{ColumnType, LogicalType, schema::system_columns};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::json;

fn scalar(ty: LogicalType) -> ColumnType {
    ColumnType::scalar(ty)
}

/// Scenario 1 + 2 + 3 in one run, plus mid-run evolution across two source batches.
#[tokio::test]
async fn full_sync_infers_types_splits_children_and_stamps_lineage() {
    let batch1 = MemoryBatch::new(vec![
        json!({
            "id": 1,
            "score": 10,                     // Int64 …
            "ratio": 1,                      // Int64 …
            "created_at": "2026-07-19T10:00:00Z",
            "profile": {"city": "NYC", "zip": 10001},
            "tags": [{"label": "red"}, {"label": "blue"}],
            "blob": {"a": 1}                 // object here …
        }),
        json!({
            "id": 2,
            "score": 10.5,                   // … then Float64 (fine so far)
            "ratio": 2.5,                    // … then Float64 → widens losslessly
            "created_at": "2026-07-19T11:30:00+02:00",
            "profile": {"city": "LA", "zip": "90210"},  // zip Int64 → Utf8 widening
            "tags": [{"label": "green"}],
            "blob": 7                        // … scalar here → irreconcilable → Json
        }),
    ]);
    // Second push: a brand-new column appears mid-run (evolution, not variant columns),
    // and score sees an Int64 beyond ±2^53 — under an existing Float64 the
    // value-checked lattice must escalate the column to Utf8, never silently round.
    let batch2 = MemoryBatch::new(vec![json!({
        "id": 3,
        "score": 9_007_199_254_740_993i64,   // 2^53 + 1
        "ratio": 3,
        "created_at": "2026-07-19T12:00:00Z",
        "profile": {"city": "SF", "zip": 94103},
        "tags": [],
        "blob": "x",
        "note": "late column"
    })]);

    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("items"),
        vec![batch1, batch2],
    )]);
    let dest = MemoryDestination::new();

    let report = Engine::new(EngineConfig::new("us1"), source, dest.clone())
        .run()
        .await
        .expect("run succeeds");

    // ---- Scenario 1: typed root + child tables, lineage columns ----
    let root_rows = dest.committed_rows("items");
    assert_eq!(root_rows.len(), 3);
    let child_rows = dest.committed_rows("items__tags");
    assert_eq!(child_rows.len(), 3, "red, blue, green");

    let schema = dest.schema("items").expect("root schema");
    let ty = |name: &str| {
        schema
            .column(name)
            .unwrap_or_else(|| panic!("column {name}"))
            .ty
            .clone()
    };
    assert_eq!(ty("id"), scalar(LogicalType::Int64));
    assert_eq!(ty("ratio"), scalar(LogicalType::Float64));
    assert_eq!(ty("created_at"), scalar(LogicalType::TimestampTz));
    assert_eq!(ty("note"), scalar(LogicalType::Utf8));
    // Nested object preserved as a struct column with its own evolved fields.
    match ty("profile") {
        ColumnType::Struct { fields } => {
            let zip = fields.iter().find(|f| f.name == "zip").expect("zip field");
            assert_eq!(zip.ty, scalar(LogicalType::Utf8), "Int64 ⊔ Utf8 → Utf8");
        }
        other => panic!("profile should be a struct column, got {other:?}"),
    }
    // Lists of objects are NOT columns.
    assert!(
        schema.column("tags").is_none(),
        "tags must be a child table, not a column"
    );

    // ---- Scenario 2: value-checked widening, one column, no variants ----
    assert_eq!(
        ty("score"),
        scalar(LogicalType::Utf8),
        "big int under Float64 → Utf8"
    );
    assert!(
        !schema.columns.iter().any(|c| c.name.starts_with("score__")),
        "widening must never multiply columns"
    );
    let scores: Vec<&str> = root_rows
        .iter()
        .map(|r| r["score"].as_str().expect("canonical text"))
        .collect();
    assert_eq!(
        scores,
        ["10", "10.5", "9007199254740993"],
        "canonical, lossless renderings"
    );

    // ---- Scenario 3: irreconcilable conflict → Json, values preserved verbatim ----
    assert_eq!(ty("blob"), scalar(LogicalType::Json));
    let blobs: Vec<&str> = root_rows
        .iter()
        .map(|r| r["blob"].as_str().expect("json text"))
        .collect();
    assert_eq!(blobs, [r#"{"a":1}"#, "7", r#""x""#]);

    // ---- Lineage ----
    let root_ids: Vec<&str> = root_rows
        .iter()
        .map(|r| r[system_columns::ID].as_str().expect("_rdlt_id"))
        .collect();
    assert_eq!(root_ids.len(), 3);
    for row in &root_rows {
        assert!(row[system_columns::LOAD_ID].as_str().is_some());
    }
    let child_schema = dest.schema("items__tags").expect("child schema");
    assert_eq!(
        child_schema.parent.as_ref().map(|p| p.parent.as_str()),
        Some("items"),
        "child links to parent"
    );
    // (red,0) (blue,1) belong to row 1; (green,0) to row 2.
    let by_label = |label: &str| {
        child_rows
            .iter()
            .find(|r| r["label"].as_str() == Some(label))
            .unwrap_or_else(|| panic!("child row {label}"))
    };
    assert_eq!(
        by_label("red")[system_columns::PARENT_ID],
        json!(root_ids[0])
    );
    assert_eq!(by_label("red")[system_columns::POS], json!(0));
    assert_eq!(
        by_label("blue")[system_columns::PARENT_ID],
        json!(root_ids[0])
    );
    assert_eq!(by_label("blue")[system_columns::POS], json!(1));
    assert_eq!(
        by_label("green")[system_columns::PARENT_ID],
        json!(root_ids[1])
    );
    // Depth-1 children: root id == parent id.
    for row in &child_rows {
        assert_eq!(row[system_columns::ROOT_ID], row[system_columns::PARENT_ID]);
    }

    // ---- Report accounting matches destination reality ----
    let items = &report.tables[&rdlt_core::TableName::new("items")];
    assert_eq!(items.rows, 3);
    let tags = &report.tables[&rdlt_core::TableName::new("items__tags")];
    assert_eq!(tags.rows, 3);
    assert!(report.commits >= 1);
    // Mid-run evolution surfaced as schema migrations (create tables + added column).
    assert!(
        report.schema_migrations.iter().any(|delta| {
            delta.changes.iter().any(|c| {
                matches!(c, rdlt_core::SchemaChange::AddColumn { column } if column.name == "note")
            })
        }),
        "the late `note` column must appear as an evolution delta"
    );
}

/// A destination without native scalar-list support gets a child table; with support,
/// a list column. (Capability-driven shred planning.)
#[tokio::test]
async fn scalar_lists_follow_destination_capabilities() {
    let rows = vec![json!({"id": 1, "nums": [1, 2, 3]})];

    // Full-featured destination: native list column.
    let dest = MemoryDestination::new();
    let source = MemorySource::single_stream(rdlt_connector::StreamSpec::new("s"), rows.clone());
    Engine::new(EngineConfig::new("lists-native"), source, dest.clone())
        .run()
        .await
        .expect("run");
    let schema = dest.schema("s").expect("schema");
    assert_eq!(
        schema.column("nums").expect("nums column").ty,
        ColumnType::ScalarList {
            item: LogicalType::Int64
        }
    );

    // Degraded destination: child table instead.
    let dest = MemoryDestination::new().with_capabilities(rdlt_connector::DestCapabilities {
        merge: false,
        structs: true,
        scalar_lists: false,
        json_type: true,
        decimal: true,
        ident_rules: Default::default(),
    });
    let source = MemorySource::single_stream(rdlt_connector::StreamSpec::new("s"), rows);
    Engine::new(EngineConfig::new("lists-child"), source, dest.clone())
        .run()
        .await
        .expect("run");
    let schema = dest.schema("s").expect("schema");
    assert!(schema.column("nums").is_none());
    let child = dest.committed_rows("s__nums");
    assert_eq!(child.len(), 3);
    assert_eq!(child[0]["value"], json!(1));
}

/// A destination without native structs receives collision-safe FLATTENED columns
/// (lowering at the destination seam, design doc §5.3).
#[tokio::test]
async fn structless_destination_gets_flattened_columns() {
    let rows = vec![json!({"id": 1, "profile": {"city": "NYC", "geo": {"lat": 1.5}}})];
    let dest = MemoryDestination::new().with_capabilities(rdlt_connector::DestCapabilities {
        merge: false,
        structs: false,
        scalar_lists: true,
        json_type: true,
        decimal: true,
        ident_rules: Default::default(),
    });
    let source = MemorySource::single_stream(rdlt_connector::StreamSpec::new("s"), rows);
    Engine::new(EngineConfig::new("flatten"), source, dest.clone())
        .run()
        .await
        .expect("run");
    let out = dest.committed_rows("s");
    assert_eq!(out[0]["profile__city"], json!("NYC"));
    assert_eq!(out[0]["profile__geo__lat"], json!(1.5));
    assert!(
        !out[0].contains_key("profile"),
        "struct column must be flattened away"
    );
}

/// Regression (shredder twin of the passthrough case): a JSON field literally named
/// `_rdlt_id` is suffixed, never aliased with the lineage column.
#[tokio::test]
async fn json_field_named_like_system_column_is_suffixed() {
    let dest = MemoryDestination::new();
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("s"),
        vec![json!({"id": 1, "_rdlt_id": "upstream"})],
    );
    Engine::new(EngineConfig::new("sysname"), source, dest.clone())
        .run()
        .await
        .expect("run");
    let rows = dest.committed_rows("s");
    let row = &rows[0];
    assert_ne!(
        row["_rdlt_id"],
        json!("upstream"),
        "lineage column not aliased"
    );
    assert!(
        row.keys().any(|k| k.starts_with("_rdlt_id_")),
        "upstream field suffixed, got {:?}",
        row.keys()
    );
}
