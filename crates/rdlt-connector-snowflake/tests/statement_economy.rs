//! What a load COSTS in round trips, counted rather than assumed.
//!
//! Every statement here crosses the internet to a SaaS warehouse, so the count
//! is a user-visible property, not an implementation detail: a load that
//! re-asserts each column separately is not slightly slower than one that
//! reads the catalog once, it is slower in proportion to the schema.
//!
//! Counted at the seam the crate itself uses, so these measure the real
//! ensure path rather than a model of it.

use rdlt_connector::core::{
    ColumnDef, ColumnType, LogicalType, Provenance, TableName, TableSchema, WriteMode,
};
use rdlt_connector_snowflake::dest::TableType;
use rdlt_connector_snowflake::dest::testhook::{Catalog, ensure_table_sql};

/// How a statement is charged.
#[derive(Debug, Default, PartialEq, Eq)]
struct Counts {
    /// Statements that ASK the catalog what exists.
    reads: usize,
    /// Statements that CHANGE the schema.
    mutations: usize,
}

/// Classify by leading verb — the same reading the unit guard uses, and for
/// the same reason: the verb is what the service dispatches on.
fn charge(statements: &[String]) -> Counts {
    let mut counts = Counts::default();
    for sql in statements {
        let verb = sql
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        match verb.as_str() {
            "SELECT" | "SHOW" | "DESCRIBE" | "DESC" => counts.reads += 1,
            "CREATE" | "ALTER" | "DROP" => counts.mutations += 1,
            _ => {}
        }
    }
    counts
}

fn schema_of(table: &str, columns: &[&str]) -> TableSchema {
    TableSchema {
        table: TableName::from(table),
        parent: None,
        columns: columns
            .iter()
            .map(|name| ColumnDef {
                name: (*name).to_owned(),
                column_type: ColumnType::scalar(LogicalType::Utf8),
                nullable: true,
                provenance: Provenance::Inferred,
            })
            .collect(),
    }
}

/// The catalog as it stands once a table exists with these columns.
fn catalog_holding(table: &str, columns: &[&str]) -> Catalog {
    let mut catalog = Catalog::default();
    catalog.observe(table, columns.iter().map(|c| c.to_uppercase()).collect());
    catalog
}

fn ensure(schema: &TableSchema, previous: Option<&TableSchema>, catalog: &Catalog) -> Vec<String> {
    ensure_table_sql(
        "pipeline",
        schema,
        &WriteMode::Append,
        TableType::Permanent,
        previous,
        catalog,
    )
}

/// A schema the catalog already matches costs NOTHING to ensure.
///
/// The property that makes a steady-state load cheap. Re-asserting each column
/// "just in case" is the shape this rules out: it is invisible on a loopback
/// database and pays a full round trip per column against a service.
#[test]
fn an_unchanged_schema_emits_no_statements_at_all() {
    let columns = ["id", "note", "region", "amount", "created_at"];
    let schema = schema_of("events", &columns);
    let catalog = catalog_holding("EVENTS", &columns);

    let statements = ensure(&schema, Some(&schema), &catalog);
    assert_eq!(
        charge(&statements),
        Counts {
            reads: 0,
            mutations: 0
        },
        "an unchanged schema must cost nothing: {statements:?}"
    );
}

/// One added column costs exactly one statement — not a re-assertion of all.
#[test]
fn one_added_column_costs_exactly_one_alter() {
    let before = schema_of("events", &["id", "note"]);
    let after = schema_of("events", &["id", "note", "region"]);
    let catalog = catalog_holding("EVENTS", &["id", "note"]);

    let statements = ensure(&after, Some(&before), &catalog);
    assert_eq!(
        charge(&statements),
        Counts {
            reads: 0,
            mutations: 1
        },
        "exactly one ADD COLUMN: {statements:?}"
    );
    assert!(
        statements[0].contains("ADD COLUMN IF NOT EXISTS \"REGION\""),
        "{statements:?}"
    );
}

/// The cost of ensuring a table does not grow with its width.
///
/// The measurement that distinguishes "read the catalog once, then emit what
/// is missing" from "assert every column". Both look identical on a table with
/// three columns.
#[test]
fn the_statement_count_is_independent_of_column_count() {
    let narrow: Vec<String> = (0..3).map(|i| format!("c{i}")).collect();
    let wide: Vec<String> = (0..200).map(|i| format!("c{i}")).collect();

    let counts: Vec<Counts> = [&narrow, &wide]
        .into_iter()
        .map(|columns| {
            let names: Vec<&str> = columns.iter().map(String::as_str).collect();
            let schema = schema_of("events", &names);
            let catalog = catalog_holding("EVENTS", &names);
            charge(&ensure(&schema, Some(&schema), &catalog))
        })
        .collect();

    assert_eq!(
        counts[0], counts[1],
        "a 200-column table must cost what a 3-column table costs"
    );

    // And creating from nothing is ONE statement whatever the width — the
    // columns ride the CREATE rather than arriving as separate ALTERs.
    let names: Vec<&str> = wide.iter().map(String::as_str).collect();
    let created = ensure(&schema_of("events", &names), None, &Catalog::default());
    assert_eq!(
        charge(&created),
        Counts {
            reads: 0,
            mutations: 1
        },
        "a fresh wide table is one CREATE: {} statements",
        created.len()
    );
}

/// A table the catalog has never seen is created once, and only once.
#[test]
fn a_new_table_costs_one_create_and_the_image_then_settles() {
    let schema = schema_of("events", &["id", "note"]);
    let first = ensure(&schema, None, &Catalog::default());
    assert_eq!(
        charge(&first),
        Counts {
            reads: 0,
            mutations: 1
        },
        "{first:?}"
    );

    // Once the catalog holds it, the same ensure emits nothing — the session's
    // second call to ensure_table for an unchanged stream is free.
    let settled = ensure(
        &schema,
        Some(&schema),
        &catalog_holding("EVENTS", &["id", "note"]),
    );
    assert!(settled.is_empty(), "{settled:?}");
}
