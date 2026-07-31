//! Identifiers, types, and the statements that make a table match a schema.
//!
//! Two rules govern everything here, and both come from how Snowflake
//! actually behaves rather than from taste:
//!
//! **Identifiers are emitted quoted and upper case.** Unquoted names fold to
//! upper case, and a quoted lower-case name is a *different object* — probing
//! the service produced `EVENTS` and `events` coexisting as two tables. Quoted
//! upper case is also exactly what an unquoted user query resolves to, so
//! `select * from events` in a worksheet finds what rdlt wrote.
//!
//! **Ensure reads before it writes.** The other SQL destinations re-assert
//! every column on every load because a local round trip is free. Against a
//! service across a network it is not: this reads the catalog once per table
//! and then emits only what is genuinely missing, so a steady-state load
//! issues no schema statements at all.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::core::{ColumnType, LogicalType, TableName, TableSchema, WriteMode};
use rdlt_connector_sqlcore::DestinationOptions;
use rdlt_connector_sqlcore::FullLoadPublish;
use rdlt_connector_sqlcore::ensure::{self, EnsureStep, Leg, Validity};
use rdlt_connector_sqlcore::plan::ValidateError;

use super::config::TableType;

/// Quote an identifier the way this destination always spells them.
///
/// Uppercased and quoted: quoting alone would preserve the engine's lower-case
/// names as objects no unquoted query can reach, and uppercasing alone would
/// break on reserved words and on names carrying specials.
pub(super) fn quote(name: &str) -> String {
    // A quote inside an identifier is escaped by doubling; the engine's
    // normalisation makes this vanishingly rare, and silently producing broken
    // SQL for it would be worse than the two characters it costs to handle.
    format!("\"{}\"", name.to_uppercase().replace('"', "\"\""))
}

/// The catalog image of a name — what `INFORMATION_SCHEMA` holds for it.
pub(super) fn catalog_name(name: &str) -> String {
    name.to_uppercase()
}

/// Snowflake type for a logical column type.
///
/// Closed by construction: every logical type maps to something documented,
/// and the nested cases cannot arrive because a destination that does not
/// advertise structs receives them already lowered.
pub(super) fn sql_type(ty: &ColumnType) -> String {
    match ty {
        ColumnType::Scalar { scalar } => match scalar {
            LogicalType::Bool => "BOOLEAN".into(),
            // i64's full range, exactly: NUMBER's default precision is 38, so
            // being explicit is what keeps a value that fits in the engine
            // from being accepted into a wider column than the schema says.
            LogicalType::Int64 => "NUMBER(19,0)".into(),
            LogicalType::Float64 => "FLOAT".into(),
            LogicalType::Utf8 => "VARCHAR".into(),
            // No native UUID type here; the canonical text form round-trips
            // and is what every other tool in this service's ecosystem uses.
            LogicalType::Uuid => "VARCHAR(36)".into(),
            LogicalType::Json => "VARIANT".into(),
            LogicalType::Binary => "BINARY".into(),
            // The zone-carrying and naive forms are DIFFERENT types here, and
            // collapsing them would silently relocate every timestamp.
            LogicalType::TimestampTz => "TIMESTAMP_TZ".into(),
            LogicalType::TimestampNaive => "TIMESTAMP_NTZ".into(),
            LogicalType::Date => "DATE".into(),
            LogicalType::Time => "TIME".into(),
            LogicalType::Decimal { precision, scale } => format!("NUMBER({precision},{scale})"),
        },
        // Structs and lists never reach a destination that does not advertise
        // them — the engine lowers them before they arrive.
        ColumnType::Struct { .. } | ColumnType::ScalarList { .. } => "VARCHAR".into(),
    }
}

/// What the catalog already holds, read once per session.
///
/// Absent from the map means "not read yet"; present with an empty column set
/// means "read, and the table does not exist". The distinction matters: the
/// first is a reason to look, the second is a reason to create.
#[derive(Debug, Default, Clone)]
pub struct Catalog {
    tables: BTreeMap<String, BTreeSet<String>>,
}

impl Catalog {
    /// Record what a read found: the table's columns, empty if it is absent.
    pub fn observe(&mut self, table: &str, columns: BTreeSet<String>) {
        self.tables.insert(catalog_name(table), columns);
    }

    /// Has this table been read this session?
    pub fn is_known(&self, table: &str) -> bool {
        self.tables.contains_key(&catalog_name(table))
    }

    fn exists(&self, table: &str) -> bool {
        self.tables
            .get(&catalog_name(table))
            .is_some_and(|columns| !columns.is_empty())
    }

    fn has_column(&self, table: &str, column: &str) -> bool {
        self.tables
            .get(&catalog_name(table))
            .is_some_and(|columns| columns.contains(&catalog_name(column)))
    }

    /// Fold an applied statement into the image, so a second ensure in the
    /// same session does not re-emit what the first already created.
    pub fn record_created(&mut self, table: &str, columns: &[String]) {
        self.tables.insert(
            catalog_name(table),
            columns.iter().map(|c| catalog_name(c)).collect(),
        );
    }

    /// Note a column added after the table was read.
    pub fn record_column(&mut self, table: &str, column: &str) {
        self.tables
            .entry(catalog_name(table))
            .or_default()
            .insert(catalog_name(column));
    }
}

/// The query that reads a table's columns.
///
/// One statement per table per session — the read that makes emitting nothing
/// possible. Parameterised so no name is interpolated into SQL.
///
/// `INFORMATION_SCHEMA` belongs to a DATABASE, not to a schema: qualifying it
/// with the schema names a database that does not exist, and the service says
/// so in exactly those words. The schema is a filter, not a qualifier.
pub(super) fn describe_sql(database: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM {}.INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        quote(database)
    )
}

/// The stage table's name for a target — the shared rule, so two pipelines
/// writing the same table in one schema cannot truncate each other's rows.
pub(super) fn stage_name(pipeline: &str, table: &TableName) -> String {
    rdlt_connector_sqlcore::names::stage_table(pipeline, table.as_str())
}

/// Read one table's columns from the catalog.
///
/// The single read that makes emitting nothing possible. Names are bound
/// rather than interpolated, and the catalog's own upper-case image is what
/// comes back — which is why every comparison here uppercases first.
pub(super) async fn observe_table(
    executor: &dyn super::client::Executor,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<BTreeSet<String>, rdlt_connector::DestinationError> {
    executor
        .column_names(
            &describe_sql(database),
            &[&catalog_name(schema), &catalog_name(table)],
        )
        .await
}

/// Phase 1 — bring both legs into line with the schema, emitting only what
/// the catalog does not already have.
pub(super) fn table_ddl_stmts(
    pipeline: &str,
    schema: &TableSchema,
    mode: &WriteMode,
    table_type: TableType,
    previous: Option<&TableSchema>,
    catalog: &Catalog,
) -> Vec<String> {
    // Merge stages; Append and Replace publish straight into the target, so
    // no staging twin is built for them.
    let plan = ensure::table_plan(schema, mode, FullLoadPublish::DirectToTarget, previous);
    let leg_name = |leg: Leg| match leg {
        Leg::Target => schema.table.as_str().to_owned(),
        Leg::Stage => stage_name(pipeline, &schema.table),
    };
    let transient = match table_type {
        TableType::Transient => "TRANSIENT ",
        TableType::Permanent => "",
    };
    let mut out = Vec::new();
    for step in plan {
        match step {
            EnsureStep::Table { leg } => {
                let name = leg_name(leg);
                if catalog.exists(&name) {
                    continue;
                }
                let mut columns: Vec<String> = schema
                    .columns
                    .iter()
                    .map(|c| {
                        let null = if leg == Leg::Target && !c.nullable {
                            " NOT NULL"
                        } else {
                            // The stage takes whatever arrives; it is the
                            // merge that decides what survives into the
                            // target, and a NOT NULL here would reject rows
                            // the dedup was going to discard anyway.
                            ""
                        };
                        format!("{} {}{null}", quote(&c.name), sql_type(&c.column_type))
                    })
                    .collect();
                if leg == Leg::Stage {
                    // Arrival order for deterministic merge dedup. An identity
                    // column rather than a sequence object: it is created and
                    // dropped with the table, so a stage table cannot outlive
                    // its counter or inherit a stale one.
                    columns.push(format!(
                        "{} NUMBER AUTOINCREMENT",
                        quote(rdlt_connector_sqlcore::names::ARRIVAL_COL)
                    ));
                }
                out.push(format!(
                    "CREATE {transient}TABLE IF NOT EXISTS {} ({})",
                    quote(&name),
                    columns.join(", ")
                ));
            }
            EnsureStep::Column { leg, column } => {
                let name = leg_name(leg);
                let def = &schema.columns[column];
                // The whole point of reading first: a column the catalog
                // already has costs no statement at all.
                if catalog.has_column(&name, &def.name) || !catalog.exists(&name) {
                    continue;
                }
                out.push(format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                    quote(&name),
                    quote(&def.name),
                    sql_type(&def.column_type)
                ));
            }
            EnsureStep::Widen { leg, column } => {
                let def = &schema.columns[column];
                out.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DATA TYPE {}",
                    quote(&leg_name(leg)),
                    quote(&def.name),
                    sql_type(&def.column_type)
                ));
            }
            _ => unreachable!("phase 1 plans relations and columns only"),
        }
    }
    out
}

/// Phase 2 — option validation and the scd2 validity columns.
///
/// No indexes: this service enforces no unique constraints and needs no
/// supporting index to find rows, so the shared plan's index steps are
/// deliberately dropped here rather than emitted as something that would do
/// nothing.
pub(super) fn merge_ensure_stmts(
    options: &DestinationOptions,
    schema: &TableSchema,
    mode: &WriteMode,
    catalog: &Catalog,
) -> Result<Vec<String>, ValidateError> {
    let table = schema.table.as_str();
    let scd2 = options.scd2_for(table);
    let mut out = Vec::new();
    for step in ensure::merge_plan(options, schema, mode)? {
        match step {
            EnsureStep::Validity(which) => {
                let (col, ty) = match which {
                    Validity::From => (&scd2.valid_from, "TIMESTAMP_TZ"),
                    Validity::To => (&scd2.valid_to, "TIMESTAMP_TZ"),
                };
                if catalog.has_column(table, col) {
                    continue;
                }
                // No NOT NULL even on valid_from: the insert arm always
                // supplies the boundary, and ADDING a NOT NULL column to a
                // table with rows is refused.
                out.push(format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {ty}",
                    quote(table),
                    quote(col)
                ));
            }
            // Indexes are not a thing here — see the doc above.
            EnsureStep::Index(_) => {}
            _ => unreachable!("phase 2 plans validity columns and indexes only"),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_connector::core::{ColumnDef, Provenance};

    fn col(name: &str, scalar: LogicalType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_owned(),
            column_type: ColumnType::Scalar { scalar },
            nullable,
            provenance: Provenance::Inferred,
        }
    }

    fn events() -> TableSchema {
        TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: vec![
                col("id", LogicalType::Int64, false),
                col("note", LogicalType::Utf8, true),
            ],
        }
    }

    #[test]
    fn identifiers_are_quoted_upper_case() {
        // Both halves matter: quoting alone leaves an object no unquoted query
        // can reach, uppercasing alone breaks on reserved words.
        assert_eq!(quote("events"), "\"EVENTS\"");
        assert_eq!(quote("Mixed_Case"), "\"MIXED_CASE\"");
        assert_eq!(quote("select"), "\"SELECT\"");
        assert_eq!(quote("with space"), "\"WITH SPACE\"");
        assert_eq!(quote("has\"quote"), "\"HAS\"\"QUOTE\"");
    }

    #[test]
    fn every_logical_type_maps_to_a_documented_snowflake_type() {
        let cases = [
            (LogicalType::Bool, "BOOLEAN"),
            (LogicalType::Int64, "NUMBER(19,0)"),
            (LogicalType::Float64, "FLOAT"),
            (LogicalType::Utf8, "VARCHAR"),
            (LogicalType::Uuid, "VARCHAR(36)"),
            (LogicalType::Json, "VARIANT"),
            (LogicalType::Binary, "BINARY"),
            (LogicalType::TimestampTz, "TIMESTAMP_TZ"),
            (LogicalType::TimestampNaive, "TIMESTAMP_NTZ"),
            (LogicalType::Date, "DATE"),
            (LogicalType::Time, "TIME"),
        ];
        for (scalar, expected) in cases {
            assert_eq!(sql_type(&ColumnType::Scalar { scalar }), expected);
        }
        assert_eq!(
            sql_type(&ColumnType::Scalar {
                scalar: LogicalType::Decimal {
                    precision: 18,
                    scale: 4
                }
            }),
            "NUMBER(18,4)"
        );
    }

    #[test]
    fn zone_carrying_and_naive_timestamps_stay_distinct() {
        // Collapsing them would silently relocate every timestamp in a load.
        assert_ne!(
            sql_type(&ColumnType::scalar(LogicalType::TimestampTz)),
            sql_type(&ColumnType::scalar(LogicalType::TimestampNaive))
        );
    }

    #[test]
    fn an_unknown_table_is_created_with_every_column() {
        let catalog = Catalog::default();
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert_eq!(sql.len(), 1, "one CREATE, no ALTERs: {sql:?}");
        assert_eq!(
            sql[0],
            "CREATE TABLE IF NOT EXISTS \"EVENTS\" \
             (\"ID\" NUMBER(19,0) NOT NULL, \"NOTE\" VARCHAR)"
        );
    }

    #[test]
    fn a_table_that_already_matches_costs_no_statements_at_all() {
        // The requirement this module exists for: a steady-state load issues
        // no schema statements.
        let mut catalog = Catalog::default();
        catalog.observe(
            "events",
            ["ID".to_owned(), "NOTE".to_owned()].into_iter().collect(),
        );
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert!(sql.is_empty(), "nothing to do: {sql:?}");
    }

    #[test]
    fn only_the_genuinely_missing_column_is_added() {
        let mut catalog = Catalog::default();
        catalog.observe("events", ["ID".to_owned()].into_iter().collect());
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert_eq!(
            sql,
            vec!["ALTER TABLE \"EVENTS\" ADD COLUMN IF NOT EXISTS \"NOTE\" VARCHAR"]
        );
    }

    #[test]
    fn merge_creates_a_stage_leg_with_an_autoincrement_arrival_column() {
        let catalog = Catalog::default();
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Merge {
                key: vec!["id".to_owned()],
            },
            TableType::Permanent,
            None,
            &catalog,
        );
        assert_eq!(sql.len(), 2, "target and stage: {sql:?}");
        let stage = &sql[1];
        assert!(
            stage.contains("__RDLT_ARRIVAL\" NUMBER AUTOINCREMENT"),
            "{stage}"
        );
        // The stage takes whatever arrives — the merge decides what survives.
        assert!(!stage.contains("NOT NULL"), "{stage}");
    }

    #[test]
    fn append_builds_no_stage_leg() {
        let catalog = Catalog::default();
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert!(
            !sql.iter().any(|s| s.contains("__RDLT_ARRIVAL")),
            "no stage for a direct publish: {sql:?}"
        );
    }

    #[test]
    fn transient_applies_to_every_table_this_pipeline_creates() {
        let catalog = Catalog::default();
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Merge {
                key: vec!["id".to_owned()],
            },
            TableType::Transient,
            None,
            &catalog,
        );
        assert!(
            sql.iter().all(|s| s.contains("CREATE TRANSIENT TABLE")),
            "both legs are transient: {sql:?}"
        );
    }

    #[test]
    fn a_widened_column_sets_its_data_type() {
        let mut catalog = Catalog::default();
        catalog.observe("events", ["ID".to_owned()].into_iter().collect());
        let before = TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: vec![col("id", LogicalType::Int64, false)],
        };
        let after = TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: vec![col("id", LogicalType::Utf8, false)],
        };
        let sql = table_ddl_stmts(
            "p",
            &after,
            &WriteMode::Append,
            TableType::Permanent,
            Some(&before),
            &catalog,
        );
        assert_eq!(
            sql,
            vec!["ALTER TABLE \"EVENTS\" ALTER COLUMN \"ID\" SET DATA TYPE VARCHAR"]
        );
    }

    #[test]
    fn the_describe_query_binds_its_names_rather_than_interpolating_them() {
        let sql = describe_sql("raw");
        assert!(sql.contains("\"RAW\".INFORMATION_SCHEMA.COLUMNS"), "{sql}");
        assert_eq!(
            sql.matches('?').count(),
            2,
            "schema and table are bound: {sql}"
        );
    }

    #[test]
    fn scd2_adds_validity_columns_without_a_not_null_constraint() {
        let options = DestinationOptions {
            merge_strategy: Some(rdlt_connector_sqlcore::MergeStrategy::Scd2),
            ..DestinationOptions::default()
        };
        let mut catalog = Catalog::default();
        catalog.observe("events", ["ID".to_owned()].into_iter().collect());
        let sql = merge_ensure_stmts(
            &options,
            &events(),
            &WriteMode::Merge {
                key: vec!["id".to_owned()],
            },
            &catalog,
        )
        .expect("valid options");
        assert_eq!(sql.len(), 2, "two validity columns, no index: {sql:?}");
        for stmt in &sql {
            assert!(stmt.contains("TIMESTAMP_TZ"), "{stmt}");
            // Adding a NOT NULL column to a table with rows is refused.
            assert!(!stmt.contains("NOT NULL"), "{stmt}");
        }
    }

    #[test]
    fn no_index_is_ever_emitted() {
        // This service enforces no unique constraints and needs no supporting
        // index to find rows; emitting one would cost time and do nothing.
        let options = DestinationOptions {
            merge_strategy: Some(rdlt_connector_sqlcore::MergeStrategy::Upsert),
            ..DestinationOptions::default()
        };
        let sql = merge_ensure_stmts(
            &options,
            &events(),
            &WriteMode::Merge {
                key: vec!["id".to_owned()],
            },
            &Catalog::default(),
        )
        .expect("valid options");
        assert!(
            !sql.iter().any(|s| s.contains("INDEX")),
            "no indexes here: {sql:?}"
        );
    }

    #[test]
    fn merge_only_options_are_refused_under_append_with_the_shared_error() {
        let options = DestinationOptions {
            merge_strategy: Some(rdlt_connector_sqlcore::MergeStrategy::Upsert),
            ..DestinationOptions::default()
        };
        let err = merge_ensure_stmts(&options, &events(), &WriteMode::Append, &Catalog::default())
            .expect_err("a merge strategy under Append is a config error");
        assert!(format!("{err}").contains("events"), "{err}");
    }
}
