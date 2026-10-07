use bytes::Bytes;
use rusqlite::Connection;
use rusqlite::types::Value;

use super::super::tests::{pipeline, query, run_all, table};
use super::super::{SqlDialect, SqlPlanner, Sqlite, Statement, Upserts};
use crate::destination::TableRef;
use crate::id::GenerationId;
use crate::id::TablePath;
use crate::state::{StateChange, StateRecord};
use crate::types::LogicalType;

/// SQLite, writing rows whose key a row may hold in standard SQL, as a dialect without
/// `ON CONFLICT` does.
#[derive(Debug)]
struct Standard;

impl SqlDialect for Standard {
    fn placeholder(&self, index: usize) -> String {
        Sqlite.placeholder(index)
    }

    fn column_type(&self, logical: &LogicalType) -> Option<String> {
        Sqlite.column_type(logical)
    }

    fn columns(&self, table: &str) -> Statement {
        Sqlite.columns(table)
    }

    fn resolves(&self, name: &str) -> Statement {
        Sqlite.resolves(name)
    }

    fn transactional_ddl(&self) -> bool {
        true
    }

    fn upserts(&self) -> Upserts {
        Upserts::Guarded
    }
}

/// [`Standard`], selecting bound values from the table `dual`, as Oracle does.
#[derive(Debug)]
struct Dual;

impl SqlDialect for Dual {
    fn placeholder(&self, index: usize) -> String {
        Standard.placeholder(index)
    }

    fn column_type(&self, logical: &LogicalType) -> Option<String> {
        Standard.column_type(logical)
    }

    fn columns(&self, table: &str) -> Statement {
        Standard.columns(table)
    }

    fn resolves(&self, name: &str) -> Statement {
        Standard.resolves(name)
    }

    fn transactional_ddl(&self) -> bool {
        Standard.transactional_ddl()
    }

    fn upserts(&self) -> Upserts {
        Standard.upserts()
    }

    fn values_table(&self) -> Option<&str> {
        Some("dual")
    }
}

/// What the catalog holds after two opens, two pipelines claiming one table, a state record put
/// twice, and a table registered under a path it then takes another name for: the epoch, the
/// owner, the state and the registered names.
fn catalog<D: SqlDialect>(dialect: D) -> Vec<Vec<Vec<Value>>> {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE dual (x INTEGER); INSERT INTO dual VALUES (1)")
        .unwrap();
    let planner = SqlPlanner::try_new(dialect).unwrap();
    run_all(&connection, &planner.bootstrap());
    let orders = pipeline("orders");
    run_all(&connection, &planner.open(&orders));
    run_all(&connection, &planner.open(&orders));
    run_all(&connection, &planner.claim(&orders, "events"));
    let other = planner.claim(&pipeline("other"), "events");
    run_all(&connection, &other);
    let put = |value: &'static [u8]| {
        StateChange::Put(StateRecord {
            key: "cursor".to_owned(),
            value: Bytes::from_static(value),
        })
    };
    run_all(
        &connection,
        &planner.state_changes(&orders, &[put(b"1"), put(b"2")]),
    );
    run_all(&connection, &planner.register_of(&table("events")));
    let generation = TableRef {
        generation: Some(GenerationId(3)),
        ..table("events")
    };
    run_all(&connection, &planner.register_of(&generation));
    run_all(&connection, &planner.register_of(&generation));
    let renamed = TableRef {
        path: TablePath::new(["events"]).unwrap(),
        name: "events_v2".into(),
        ..table("events")
    };
    run_all(&connection, &planner.register_of(&renamed));
    vec![
        query(&connection, &planner.epoch(&orders)),
        query(&connection, &planner.owner("events")),
        query(&connection, &planner.state(&orders)),
        query(
            &connection,
            &planner.table_name(&pipeline("mine"), &renamed.path),
        ),
        query(&connection, &planner.owned_by(&orders)),
        query(&connection, &planner.owned_tables()),
        query(&connection, &planner.generations("events")),
    ]
}

#[test]
fn standard_sql_writes_the_catalog_as_on_conflict_does() {
    let expected = vec![
        vec![vec![Value::Integer(2)]],
        vec![vec![Value::Text("orders".to_owned())]],
        vec![vec![
            Value::Text("cursor".to_owned()),
            Value::Blob(b"2".to_vec()),
        ]],
        vec![vec![Value::Text("events_v2".to_owned())]],
        vec![vec![Value::Text("events".to_owned())]],
        vec![vec![Value::Text("events".to_owned())]],
        vec![vec![
            Value::Text("_rdlt_generation_3__events".to_owned()),
            Value::Integer(3),
        ]],
    ];
    assert_eq!(catalog(Sqlite), expected);
    assert_eq!(catalog(Standard), expected);
    assert_eq!(catalog(Dual), expected);
}

#[test]
fn only_sqlite_writes_on_conflict() {
    assert_eq!(Sqlite.upserts(), Upserts::OnConflict);
    assert_eq!(Standard.upserts(), Upserts::Guarded);
    assert_eq!(super::super::tests::Widening.upserts(), Upserts::Guarded);
}

#[test]
fn a_byte_of_bytes_is_taken_in_standard_sql_unless_the_dialect_says_otherwise() {
    assert_eq!(
        Standard.byte_at("flags", 3),
        "SUBSTRING(flags FROM 3 FOR 1)"
    );
    assert_eq!(Sqlite.byte_at("flags", 3), "SUBSTR(flags, 3, 1)");
}
