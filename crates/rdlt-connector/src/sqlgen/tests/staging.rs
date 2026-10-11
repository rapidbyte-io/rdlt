//! What a commit reads of staging, by SQLite's plans: only what it publishes.

use rusqlite::Connection;
use rusqlite::types::Value;

use super::super::{SqlPlanner, Sqlite, Statement};
use super::{
    apply, columns, create, database, keyed, pipeline, query, run_all, segments, staged, table,
};
use crate::commit::SegmentSet;
use crate::destination::TableRef;
use crate::id::Epoch;
use crate::types::LogicalType;

/// SQLite's plan for `statement`, a step a line.
fn explained(connection: &Connection, statement: &Statement) -> Vec<String> {
    let explain = Statement {
        sql: format!("EXPLAIN QUERY PLAN {}", statement.sql),
        params: statement.params.clone(),
    };
    query(connection, &explain)
        .into_iter()
        .map(|row| match &row[3] {
            Value::Text(step) => step.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// Checks that no statement of `plan` scans a staging table.
fn reads_staging_by_index(connection: &Connection, plan: &[Statement]) {
    for statement in plan {
        let steps = explained(connection, statement);
        assert!(
            !steps
                .iter()
                .any(|step| step.starts_with("SCAN _rdlt_staging__")),
            "{}\n{steps:#?}",
            statement.sql
        );
    }
}

/// A commit of segments 1, 2, 4 and 5, with 3 another commit's.
fn gapped() -> SegmentSet {
    segments(&[1, 2, 4, 5])
}

/// The indexes of the table `name`.
fn indexes_of(connection: &Connection, name: &str) -> Vec<Vec<Value>> {
    let statement = Statement {
        sql: format!("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '{name}'"),
        params: Vec::new(),
    };
    query(connection, &statement)
}

/// Readies `table` for a writer as a session does: indexes its staging by who staged its rows
/// or drops that index, and indexes what a commit finds by key.
fn ready(connection: &Connection, planner: &SqlPlanner<Sqlite>, table: &TableRef) {
    run_all(connection, &[planner.staged_index_of(table)]);
    run_all(connection, &planner.key_indexes_of(table));
    run_all(connection, planner.root_index_of(table).unwrap().as_slice());
}

/// The table `orders` of an `id` and a sequence, created as `created` and readied for a writer
/// of `written`.
fn orders(created: &TableRef, written: &TableRef) -> (Connection, SqlPlanner<Sqlite>) {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(created, &fields)).unwrap();
    let staging = planner.staging_table("orders");
    assert_eq!(indexes_of(&connection, &staging), Vec::<Vec<Value>>::new());
    ready(&connection, &planner, written);
    (connection, planner)
}

#[test]
fn an_append_publishes_and_deletes_its_staging_through_the_staging_index() {
    let (connection, planner) = orders(&table("orders"), &table("orders"));
    let columns = columns(&connection, &planner, "orders");
    let (mine, orders) = (pipeline("mine"), staged("orders", None, None));
    let plan = planner
        .publish_as(&orders, &columns, &mine, Epoch(2), &gapped())
        .unwrap();
    reads_staging_by_index(&connection, &plan);
    let abandon = planner.abandon_as(&orders, &mine, Epoch(2), &gapped());
    reads_staging_by_index(&connection, &[abandon]);
}

#[test]
fn a_staging_index_stands_while_its_table_s_writer_appends() {
    let staging_index = |planner: &SqlPlanner<Sqlite>| {
        Value::Text(planner.staged_index_name(&planner.staging_table("orders")))
    };
    // Created keyed, then written by an append writer.
    let (connection, planner) = orders(&keyed("orders"), &table("orders"));
    let staging = planner.staging_table("orders");
    assert_eq!(
        indexes_of(&connection, &staging),
        [[staging_index(&planner)]]
    );
    // Created to append, then written by a keyed writer, whose merge reads no such index.
    let (connection, planner) = orders(&table("orders"), &table("orders"));
    ready(&connection, &planner, &keyed("orders"));
    let key = Value::Text(planner.key_index_name(&staging));
    assert_eq!(indexes_of(&connection, &staging), [[key]]);
    let columns = columns(&connection, &planner, "orders");
    let merge = staged("orders", None, keyed("orders").merge);
    let plan = planner
        .publish_as(&merge, &columns, &pipeline("mine"), Epoch(2), &gapped())
        .unwrap();
    for statement in &plan {
        let steps = explained(&connection, statement);
        assert!(
            !steps.iter().any(|step| step.contains("_rdlt_staged__")),
            "{}\n{steps:#?}",
            statement.sql
        );
    }
}
