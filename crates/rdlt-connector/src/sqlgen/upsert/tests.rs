use bytes::Bytes;
use rusqlite::Connection;
use rusqlite::types::Value;

use super::super::tests::{pipeline, query, run_all, table};
use super::super::{SqlDialect, SqlPlanner, Sqlite, Statement, Upserts};
use crate::destination::TableRef;
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

    fn transactional_ddl(&self) -> bool {
        true
    }

    fn upserts(&self) -> Upserts {
        Upserts::Guarded
    }
}

/// What the catalog holds after two opens, two pipelines claiming one table, a state record put
/// twice, and a table registered under a path it then takes another name for: the epoch, the
/// owner, the state and the registered names.
fn catalog<D: SqlDialect>(dialect: D) -> Vec<Vec<Vec<Value>>> {
    let connection = Connection::open_in_memory().unwrap();
    let planner = SqlPlanner::try_new(dialect).unwrap();
    run_all(&connection, &planner.bootstrap());
    let orders = pipeline("orders");
    run_all(&connection, &planner.open(&orders));
    run_all(&connection, &planner.open(&orders));
    run_all(&connection, &planner.claim(&orders, "events"));
    run_all(&connection, &planner.claim(&pipeline("other"), "events"));
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
    run_all(&connection, &planner.register(&table("events")));
    let renamed = TableRef {
        path: TablePath::new(["events"]).unwrap(),
        name: "events_v2".into(),
        ..table("events")
    };
    run_all(&connection, &planner.register(&renamed));
    vec![
        query(&connection, &planner.epoch(&orders)),
        query(&connection, &planner.owner("events")),
        query(&connection, &planner.state(&orders)),
        query(&connection, &planner.tables()),
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
    ];
    assert_eq!(catalog(Sqlite), expected);
    assert_eq!(catalog(Standard), expected);
}

#[test]
fn only_sqlite_writes_on_conflict() {
    assert_eq!(Sqlite.upserts(), Upserts::OnConflict);
    assert_eq!(Standard.upserts(), Upserts::Guarded);
    assert_eq!(super::super::tests::Widening.upserts(), Upserts::Guarded);
}
