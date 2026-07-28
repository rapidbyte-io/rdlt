//! The option vocabulary behaves identically to the other SQL destinations.
//!
//! Parity here is not "similar" — it is the SAME validation, reached through
//! the same shared core, producing the same sentence. A user moving a pipeline
//! from Postgres to Snowflake must not discover that a configuration their
//! document has always carried is now silently accepted, or refused with
//! different words.
//!
//! Needs no account: validation is a decision about a document.

use rdlt_connector::core::{
    ColumnDef, ColumnType, LogicalType, Provenance, TableName, TableSchema, WriteMode,
};
use rdlt_connector_snowflake::dest::testhook::{Catalog, ensure_merge_sql};
use rdlt_connector_sqlcore::options::{DestOptions, MergeStrategy, TableOptions};

/// A keyed structured table — no `_rdlt_id`, so the strategies that converge
/// on a declared key are available.
fn keyed(columns: &[(&str, LogicalType)]) -> TableSchema {
    TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: columns
            .iter()
            .map(|(name, ty)| ColumnDef {
                name: (*name).to_owned(),
                column_type: ColumnType::scalar(*ty),
                nullable: true,
                provenance: Provenance::Inferred,
            })
            .collect(),
    }
}

/// The same table as the engine shreds it: identity is a content hash, and
/// there is no declared key for a strategy to converge on.
fn shredded(columns: &[(&str, LogicalType)]) -> TableSchema {
    let mut schema = keyed(columns);
    schema.columns.insert(
        0,
        ColumnDef {
            name: rdlt_connector::core::schema::system_columns::ID.to_owned(),
            column_type: ColumnType::scalar(LogicalType::Utf8),
            nullable: false,
            provenance: Provenance::System,
        },
    );
    schema
}

fn merge_mode() -> WriteMode {
    WriteMode::Merge {
        key: vec!["id".into()],
    }
}

fn options(strategy: Option<MergeStrategy>, table: Option<TableOptions>) -> DestOptions {
    DestOptions {
        merge_strategy: strategy,
        tables: table
            .map(|opts| [("events".to_string(), opts)].into_iter().collect())
            .unwrap_or_default(),
        ..DestOptions::default()
    }
}

fn refusal(schema: &TableSchema, options: &DestOptions) -> String {
    ensure_merge_sql(options, schema, &merge_mode(), &Catalog::default())
        .expect_err("this configuration must be refused")
        .to_string()
}

/// Every combination refused elsewhere is refused here, in the same words.
///
/// The text is the shared core's, asserted rather than paraphrased: a
/// destination that re-worded a refusal would be telling operators something
/// different about the same mistake.
#[test]
fn refusals_carry_the_shared_core_wording() {
    let columns = [
        ("id", LogicalType::Int64),
        ("note", LogicalType::Utf8),
        ("deleted", LogicalType::Bool),
    ];

    for (label, schema, options, expected) in [
        (
            "upsert on a shredded stream",
            shredded(&columns),
            options(Some(MergeStrategy::Upsert), None),
            "the upsert strategy requires a KEYED structured stream",
        ),
        (
            "scd2 on a shredded stream",
            shredded(&columns),
            options(Some(MergeStrategy::Scd2), None),
            "scd2 requires a KEYED structured stream",
        ),
        (
            "dedup_sort on a shredded stream",
            shredded(&columns),
            options(
                Some(MergeStrategy::DeleteInsert),
                Some(TableOptions {
                    dedup_sort: Some(rdlt_connector_sqlcore::options::DedupSort {
                        column: "note".into(),
                        order: rdlt_connector_sqlcore::options::SortOrder::Desc,
                    }),
                    ..TableOptions::default()
                }),
            ),
            "dedup_sort requires a KEYED structured",
        ),
        (
            "merge_scope on a shredded stream",
            shredded(&columns),
            options(
                Some(MergeStrategy::DeleteInsert),
                Some(TableOptions {
                    merge_scope: Some(vec!["note".into()]),
                    ..TableOptions::default()
                }),
            ),
            "merge_scope requires a KEYED structured",
        ),
        (
            "merge_scope naming a column the table lacks",
            keyed(&columns),
            options(
                Some(MergeStrategy::Upsert),
                Some(TableOptions {
                    merge_scope: Some(vec!["region".into()]),
                    ..TableOptions::default()
                }),
            ),
            "merge_scope column `region` is not a column of table",
        ),
        (
            "merge_scope naming the deletion flag",
            keyed(&columns),
            options(
                Some(MergeStrategy::Upsert),
                Some(TableOptions {
                    merge_scope: Some(vec!["deleted".into()]),
                    hard_delete: Some("deleted".into()),
                    ..TableOptions::default()
                }),
            ),
            "is the hard_delete flag — a deletion flag is not a scope",
        ),
    ] {
        let rendered = refusal(&schema, &options);
        assert!(
            rendered.contains(expected),
            "{label}: expected the shared wording `{expected}`, got `{rendered}`"
        );
    }
}

/// And every combination the other destinations ACCEPT is accepted here.
///
/// The half worth stating explicitly: a destination that refused more than its
/// siblings would also break parity, and only refusals tend to get tested.
#[test]
fn every_supported_combination_is_accepted() {
    let columns = [
        ("id", LogicalType::Int64),
        ("region", LogicalType::Utf8),
        ("note", LogicalType::Utf8),
        ("deleted", LogicalType::Bool),
    ];
    let schema = keyed(&columns);

    for (label, options) in [
        (
            "plain delete_insert",
            options(Some(MergeStrategy::DeleteInsert), None),
        ),
        ("plain upsert", options(Some(MergeStrategy::Upsert), None)),
        ("plain scd2", options(Some(MergeStrategy::Scd2), None)),
        (
            "upsert + hard_delete + dedup_sort + merge_scope",
            options(
                Some(MergeStrategy::Upsert),
                Some(TableOptions {
                    hard_delete: Some("deleted".into()),
                    dedup_sort: Some(rdlt_connector_sqlcore::options::DedupSort {
                        column: "note".into(),
                        order: rdlt_connector_sqlcore::options::SortOrder::Desc,
                    }),
                    merge_scope: Some(vec!["region".into()]),
                    ..TableOptions::default()
                }),
            ),
        ),
    ] {
        ensure_merge_sql(&options, &schema, &merge_mode(), &Catalog::default())
            .unwrap_or_else(|e| panic!("{label} must be accepted: {e}"));
    }
}

/// A shredded stream's supported strategy stays supported.
#[test]
fn delete_insert_is_the_shredded_streams_strategy_and_still_works() {
    let columns = [("id", LogicalType::Int64), ("note", LogicalType::Utf8)];
    ensure_merge_sql(
        &options(Some(MergeStrategy::DeleteInsert), None),
        &shredded(&columns),
        &merge_mode(),
        &Catalog::default(),
    )
    .expect("delete_insert is what a shredded stream uses");
}
