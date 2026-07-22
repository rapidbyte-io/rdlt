//! Golden-SQL pins (feature 013 T001, contract SM4).
//!
//! These tests bind the EXACT statement text the postgres destination
//! generates for a representative plan matrix. They were captured against the
//! pre-extraction code and MUST NOT change across the rdlt-connector-sqlcore
//! extraction — if a pin has to change, that is a behavioral edit, not an
//! extraction, and the extraction stops (tasks.md implementation strategy).
//!
//! No database: the builders are pure. Behavior (what the SQL does) is pinned
//! by the 006/008/010 conformance + sweep suites; THIS suite pins the text.

use rdlt_connector::core::schema::ColumnDef;
use rdlt_connector::core::{ColumnType, LogicalType, Provenance, TableName, TableSchema};
use rdlt_connector_postgres::dest::sqlgen::{
    HardDelete, MergePlan, PgDialect, identity_delete_insert_sql, keyed_delete_insert_sql,
    keyed_upsert_sql, scd2_merge_sql, scope_replace_sql,
};
use rdlt_connector_postgres::dest::{DedupSort, Scd2Options, SortOrder};

fn col(name: &str, scalar: LogicalType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty: ColumnType::Scalar { scalar },
        nullable: true,
        provenance: Provenance::Inferred,
    }
}

/// A keyed structured table: id/day keys + data columns + a bool flag.
fn keyed_schema() -> TableSchema {
    TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: vec![
            col("id", LogicalType::Int64),
            col("day", LogicalType::Int64),
            col("name", LogicalType::Utf8),
            col("seq", LogicalType::Int64),
            col("deleted", LogicalType::Bool),
            col("_rdlt_load_id", LogicalType::Utf8),
        ],
    }
}

fn plan<'a>(
    schema: &'a TableSchema,
    key: &'a [String],
    hard_delete: Option<&str>,
    dedup: Option<&'a DedupSort>,
) -> MergePlan<'a> {
    MergePlan {
        dialect: &PgDialect,
        target: "\"events\"",
        stage: "\"_rdlt_stage_feedcafe\"",
        cols: "\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\"",
        schema,
        key,
        root_stage: "\"_rdlt_stage_feedcafe\"".into(),
        is_child: false,
        hard_delete: hard_delete.map(|c| HardDelete::new(c, schema, &PgDialect)),
        dedup_sort: dedup,
    }
}

#[test]
fn pin_scope_replace() {
    let sql = scope_replace_sql(
        &PgDialect,
        "\"events\"",
        "\"_rdlt_stage_feedcafe\"",
        &["day".into(), "tenant".into()],
    );
    assert_eq!(
        sql,
        "DELETE FROM \"events\" WHERE (\"day\", \"tenant\") IN (\n             SELECT \"day\", \"tenant\" FROM \"_rdlt_stage_feedcafe\" WHERE \"day\" IS NOT NULL AND \"tenant\" IS NOT NULL)"
    );
}

#[test]
fn pin_keyed_delete_insert_plain() {
    let schema = keyed_schema();
    let key = vec!["id".to_string()];
    let stmts = keyed_delete_insert_sql(&plan(&schema, &key, None, None));
    assert_eq!(stmts, vec!["DELETE FROM \"events\" WHERE (\"id\") IN (SELECT \"id\" FROM \"_rdlt_stage_feedcafe\")".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"__rdlt_arrival\" DESC) deduped".to_string()]);
}

#[test]
fn pin_keyed_delete_insert_with_dedup_and_hard_delete() {
    let schema = keyed_schema();
    let key = vec!["id".to_string()];
    let dedup = DedupSort {
        column: "seq".into(),
        order: SortOrder::Desc,
    };
    let stmts = keyed_delete_insert_sql(&plan(&schema, &key, Some("deleted"), Some(&dedup)));
    assert_eq!(stmts, vec!["DELETE FROM \"events\" WHERE (\"id\") IN (SELECT \"id\" FROM \"_rdlt_stage_feedcafe\")".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"seq\" DESC NULLS LAST, \"__rdlt_arrival\" DESC) deduped WHERE \"deleted\" IS NOT TRUE".to_string()]);
}

#[test]
fn pin_keyed_upsert_plain() {
    let schema = keyed_schema();
    let key = vec!["id".to_string(), "day".to_string()];
    let stmts = keyed_upsert_sql(&plan(&schema, &key, None, None));
    assert_eq!(stmts, vec!["INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"id\", \"day\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"day\", \"__rdlt_arrival\" DESC) deduped ON CONFLICT (\"id\", \"day\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"seq\" = EXCLUDED.\"seq\", \"deleted\" = EXCLUDED.\"deleted\", \"_rdlt_load_id\" = EXCLUDED.\"_rdlt_load_id\"".to_string()]);
}

#[test]
fn pin_keyed_upsert_with_hard_delete_asc_dedup() {
    let schema = keyed_schema();
    let key = vec!["id".to_string()];
    let dedup = DedupSort {
        column: "seq".into(),
        order: SortOrder::Asc,
    };
    let stmts = keyed_upsert_sql(&plan(&schema, &key, Some("deleted"), Some(&dedup)));
    assert_eq!(
        stmts,
        vec!["DELETE FROM \"events\" WHERE (\"id\") IN (SELECT \"id\" FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"seq\" ASC NULLS LAST, \"__rdlt_arrival\" DESC) d WHERE \"deleted\" IS TRUE)".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"seq\" ASC NULLS LAST, \"__rdlt_arrival\" DESC) deduped WHERE \"deleted\" IS NOT TRUE ON CONFLICT (\"id\") DO UPDATE SET \"day\" = EXCLUDED.\"day\", \"name\" = EXCLUDED.\"name\", \"seq\" = EXCLUDED.\"seq\", \"deleted\" = EXCLUDED.\"deleted\", \"_rdlt_load_id\" = EXCLUDED.\"_rdlt_load_id\"".to_string()]
    );
}

#[test]
fn pin_identity_delete_insert_root_with_hard_delete() {
    let mut schema = keyed_schema();
    schema.columns.insert(0, col("_rdlt_id", LogicalType::Utf8));
    let key: Vec<String> = vec![];
    let stmts = identity_delete_insert_sql(&plan(&schema, &key, Some("deleted"), None));
    assert_eq!(stmts, vec!["DELETE FROM \"events\" WHERE \"_rdlt_id\" IN (SELECT \"_rdlt_id\" FROM \"_rdlt_stage_feedcafe\")".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"_rdlt_id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"_rdlt_id\", \"__rdlt_arrival\" DESC) deduped WHERE \"deleted\" IS NOT TRUE".to_string()]);
}

#[test]
fn pin_scd2_keep_and_retire() {
    let schema = keyed_schema();
    let key = vec!["id".to_string()];
    let scd2 = Scd2Options::default();
    let stmts = scd2_merge_sql(&plan(&schema, &key, None, None), &scd2);
    assert_eq!(stmts, vec!["UPDATE \"events\" t SET \"_rdlt_valid_to\" = now() FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"__rdlt_arrival\" DESC) d WHERE t.\"_rdlt_valid_to\" IS NULL AND t.\"id\" = d.\"id\" AND (t.\"day\" IS DISTINCT FROM d.\"day\" OR t.\"name\" IS DISTINCT FROM d.\"name\" OR t.\"seq\" IS DISTINCT FROM d.\"seq\" OR t.\"deleted\" IS DISTINCT FROM d.\"deleted\")".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\", \"_rdlt_valid_from\", \"_rdlt_valid_to\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\", now(), NULL FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"__rdlt_arrival\" DESC) d WHERE NOT EXISTS ( SELECT 1 FROM \"events\" t WHERE t.\"_rdlt_valid_to\" IS NULL AND t.\"id\" = d.\"id\")".to_string()]);

    let retire = Scd2Options {
        absent: rdlt_connector_postgres::dest::AbsentPolicy::Retire,
        ..Scd2Options::default()
    };
    let stmts = scd2_merge_sql(&plan(&schema, &key, None, None), &retire);
    assert_eq!(stmts.len(), 3);
    assert_eq!(
        stmts[2],
        "UPDATE \"events\" t SET \"_rdlt_valid_to\" = now() WHERE t.\"_rdlt_valid_to\" IS NULL AND (\"id\") NOT IN (SELECT \"id\" FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"__rdlt_arrival\" DESC) d)"
    );
}
