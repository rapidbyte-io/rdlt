//! The ensure DDL, pinned as data.
//!
//! Ensuring a table used to build each statement and execute it immediately,
//! so the only way to see what it emits was to run it against a database —
//! and nothing pinned the text. These cells compare the rendered sequence
//! itself, in order, with no connection anywhere.
//!
//! Several of them pin where this destination deliberately DIFFERS from the
//! postgres one. Those differences are not accidents of implementation: each
//! exists because DuckDB rejects, or has no use for, what postgres emits, and
//! a refactor that "harmonized" them would break this database.

use rdlt_connector::core::{
    ColumnDef, ColumnType, LogicalType, Provenance, TableName, TableSchema, WriteMode,
};
use rdlt_connector_duckdb::dest::sqlgen::{ensure_merge_sql, ensure_table_sql};
use rdlt_connector_duckdb::dest::{DestinationOptions, MergeStrategy};

fn col(name: &str, scalar: LogicalType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_owned(),
        column_type: ColumnType::Scalar { scalar },
        nullable,
        provenance: Provenance::Inferred,
    }
}

fn schema(table: &str, columns: Vec<ColumnDef>) -> TableSchema {
    TableSchema {
        table: TableName::from(table),
        parent: None,
        columns,
    }
}

fn events() -> TableSchema {
    schema(
        "events",
        vec![
            col("id", LogicalType::Int64, false),
            col("note", LogicalType::Utf8, true),
        ],
    )
}

/// Options carrying one explicit strategy — set at construction so the value
/// is visible where the options are made.
fn with_strategy(strategy: MergeStrategy) -> DestinationOptions {
    DestinationOptions {
        merge_strategy: Some(strategy),
        ..DestinationOptions::default()
    }
}

fn merge_by_id() -> WriteMode {
    WriteMode::Merge {
        key: vec!["id".to_owned()],
    }
}

#[test]
fn both_legs_are_always_created_even_without_merge() {
    // DIFFERS from postgres deliberately: every write mode here publishes
    // through a stage, so the stage leg is not conditional on Merge.
    let sql = ensure_table_sql(&events(), None);
    let creates: Vec<&String> = sql.iter().filter(|s| s.starts_with("CREATE")).collect();
    assert_eq!(creates.len(), 2, "target and stage legs: {sql:?}");
    assert!(creates[0].contains("\"events\""));
    assert!(
        creates[1].contains("TEMP") || creates[1].contains("TEMPORARY"),
        "the stage leg is temporary: {}",
        creates[1]
    );
}

#[test]
fn columns_are_ensured_on_the_target_then_the_stage() {
    let sql = ensure_table_sql(&events(), None);
    let adds: Vec<&String> = sql
        .iter()
        .filter(|s| s.contains("ADD COLUMN IF NOT EXISTS"))
        .collect();
    // Two columns × two legs, target leg first.
    assert_eq!(adds.len(), 4, "{sql:?}");
    assert!(adds[0].contains("\"events\"") && adds[0].contains("\"id\""));
    assert!(adds[1].contains("\"events\"") && adds[1].contains("\"note\""));
    assert!(!adds[2].contains("ALTER TABLE \"events\" "), "{}", adds[2]);
}

#[test]
fn a_widened_column_uses_set_data_type_and_never_a_using_cast() {
    // DIFFERS from postgres deliberately: DuckDB migrates existing rows on
    // SET DATA TYPE and has no USING clause here.
    let before = schema("events", vec![col("id", LogicalType::Int64, false)]);
    let after = schema("events", vec![col("id", LogicalType::Utf8, false)]);
    let sql = ensure_table_sql(&after, Some(&before));
    let widens: Vec<&String> = sql.iter().filter(|s| s.contains("ALTER COLUMN")).collect();
    assert_eq!(widens.len(), 2, "one widen per leg: {sql:?}");
    for w in &widens {
        assert!(w.contains("SET DATA TYPE"), "{w}");
        assert!(!w.contains("USING"), "no USING cast on this dialect: {w}");
    }
}

#[test]
fn an_unchanged_column_is_added_but_never_widened() {
    let same = events();
    let sql = ensure_table_sql(&same, Some(&same));
    assert!(
        !sql.iter().any(|s| s.contains("ALTER COLUMN")),
        "nothing changed type, so nothing may be widened: {sql:?}"
    );
}

#[test]
fn scd2_validity_columns_carry_no_not_null_constraint() {
    // DIFFERS from postgres deliberately: DuckDB REJECTS ADD COLUMN with a
    // NOT NULL constraint. The insert arm always supplies the boundary value,
    // so the constraint was belt only.
    let stmts = ensure_merge_sql(
        &with_strategy(MergeStrategy::Scd2),
        &events(),
        &merge_by_id(),
    )
    .expect("valid options");
    let validity: Vec<&str> = stmts
        .iter()
        .map(|s| s.0.as_str())
        // ADD COLUMN only: the scd2 index also NAMES a validity column.
        .filter(|s| s.contains("ADD COLUMN") && s.contains("_rdlt_valid"))
        .collect();
    assert_eq!(validity.len(), 2, "{stmts:?}");
    assert!(validity[0].contains("_rdlt_valid_from"));
    assert!(validity[0].ends_with("TIMESTAMPTZ DEFAULT now()"));
    assert!(validity[1].contains("_rdlt_valid_to"));
    assert!(validity[1].ends_with("TIMESTAMPTZ"));
    for v in &validity {
        assert!(!v.contains("NOT NULL"), "DuckDB rejects this: {v}");
    }
}

#[test]
fn a_unique_index_is_preceded_by_dropping_its_legacy_name() {
    // A unique index created under an older naming scheme would otherwise
    // survive beside the current one; the drop must come FIRST.
    let stmts = ensure_merge_sql(
        &with_strategy(MergeStrategy::Upsert),
        &events(),
        &merge_by_id(),
    )
    .expect("valid options");
    let texts: Vec<&str> = stmts.iter().map(|s| s.0.as_str()).collect();
    let drop = texts
        .iter()
        .position(|s| s.starts_with("DROP INDEX IF EXISTS"))
        .expect("legacy drop is emitted");
    let create = texts
        .iter()
        .position(|s| s.contains("CREATE UNIQUE INDEX"))
        .expect("the unique index is created");
    assert!(drop < create, "drop precedes create: {texts:?}");
    // Only the unique index carries the key columns for the duplicate
    // diagnosis; the drop does not.
    assert!(stmts[drop].1.is_none());
    assert_eq!(stmts[create].1.as_deref(), Some(&["id".to_owned()][..]));
}

#[test]
fn the_default_strategy_ensures_a_plain_supporting_index_and_no_drop() {
    // delete-insert needs the key indexed to find rows, but must NOT declare
    // it unique — so there is no legacy unique name to retire either.
    let stmts = ensure_merge_sql(&DestinationOptions::default(), &events(), &merge_by_id())
        .expect("valid options");
    assert_eq!(stmts.len(), 1, "one supporting index: {stmts:?}");
    assert!(stmts[0].1.is_none(), "not unique: {stmts:?}");
    assert!(stmts[0].0.starts_with("CREATE INDEX IF NOT EXISTS"));
}

#[test]
fn a_non_merge_mode_emits_no_merge_statements_and_still_validates() {
    let stmts = ensure_merge_sql(
        &DestinationOptions::default(),
        &events(),
        &WriteMode::Append,
    )
    .expect("default options are valid");
    assert!(
        stmts.is_empty(),
        "nothing to ensure beyond the tables: {stmts:?}"
    );
}

#[test]
fn merge_only_options_are_refused_under_append_with_the_shared_error() {
    let err = ensure_merge_sql(
        &with_strategy(MergeStrategy::Upsert),
        &events(),
        &WriteMode::Append,
    )
    .expect_err("a merge strategy under Append is a config error");
    assert!(
        format!("{err}").contains("events"),
        "the error names the table: {err}"
    );
}
