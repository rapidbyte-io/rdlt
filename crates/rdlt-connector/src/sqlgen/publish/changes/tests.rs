use super::super::super::tests::{
    apply, columns, counting, create, database, digits, pipeline, planned, run_all, segments, table,
};
use rusqlite::Connection;

use super::super::super::{SqlPlanner, Sqlite, Statement};
use super::super::Staged;
use crate::destination::{ChangeColumns, Deletion, MergeKey};
use crate::error::ConnectorErrorKind;
use crate::id::{Epoch, GenerationId};
use crate::types::LogicalType;

fn key(deletion: Deletion) -> MergeKey {
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: Some(ChangeColumns {
            op: "op".into(),
            unchanged: None,
            deletion,
        }),
        history: None,
    }
}

#[test]
fn a_change_stream_merges_into_its_table_never_a_generation() {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&table("orders"), &fields)).unwrap();
    let columns = columns(&connection, &planner, "orders");
    let staged = Staged {
        name: "orders".to_owned(),
        generation: Some(GenerationId(3)),
        merge: Some(key(Deletion::Hard)),
    };
    let error = planner
        .publish_as(
            &staged,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
}

#[test]
fn soft_deletes_into_a_table_without_their_column_are_refused() {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&table("orders"), &fields)).unwrap();
    let columns = columns(&connection, &planner, "orders");
    let staged = Staged {
        name: "orders".to_owned(),
        generation: None,
        merge: Some(key(Deletion::Soft {
            at: "deleted_at".into(),
        })),
    };
    let error = planner
        .publish_as(
            &staged,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

/// A database holding `orders`, keyed by `id` and deleting as `deletion` says, ready for changes.
fn orders(deletion: Deletion) -> (Connection, SqlPlanner<Sqlite>, Staged) {
    let (connection, planner) = database();
    let orders = crate::destination::TableRef {
        merge: Some(key(deletion)),
        ..table("orders")
    };
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
        ("deleted_at", LogicalType::Int64, true),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let tables = [
        "orders".to_owned(),
        planner.staging_table("orders"),
        planner.tombstone_table("orders"),
    ]
    .map(|name| columns(&connection, &planner, &name));
    let ready = planner
        .change_tables_of(&orders, [&tables[0], &tables[1], &tables[2]])
        .unwrap();
    run_all(&connection, &ready);
    run_all(&connection, &planner.key_indexes_of(&orders));
    let staged = Staged {
        name: "orders".to_owned(),
        generation: None,
        merge: orders.merge,
    };
    (connection, planner, staged)
}

/// The plan publishing one staged segment of `orders`.
fn committing(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    staged: &Staged,
    segment: u64,
) -> Vec<Statement> {
    let columns = columns(connection, planner, "orders");
    planner
        .publish_as(
            staged,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[segment]),
        )
        .unwrap()
}

#[test]
fn soft_truncates_cost_their_count_and_the_rows_not_their_product() {
    const ROWS: u32 = 4_000;
    const TRUNCATES: u32 = 400;
    let (connection, planner, staged) = orders(Deletion::Soft {
        at: "deleted_at".into(),
    });
    let staging = planner.staging_table("orders");
    let filled = format!(
        "{} INSERT INTO orders SELECT i, {}, NULL FROM _n",
        counting(ROWS),
        digits("1000000 + i")
    );
    connection.execute_batch(&filled).unwrap();
    // One truncate in segments 1 and 3, many in segments 2 and 4; the first two before every row,
    // which they leave, the last two past every row, each of which they mark.
    let segments_of = [
        (1, 1, 0),
        (2, TRUNCATES, 0),
        (3, 1, 2_000_000),
        (4, TRUNCATES, 2_000_000),
    ];
    for (segment, count, first) in segments_of {
        let staged = format!(
            "{} INSERT INTO \"{staging}\" (_rdlt_pipeline, _rdlt_epoch, _rdlt_segment, seq, \
             deleted_at, op) SELECT 'mine', 1, {segment}, {}, i, 3 FROM _n",
            counting(count),
            digits(&format!("{first} + i"))
        );
        connection.execute_batch(&staged).unwrap();
    }
    let timed = |segment| {
        planned(
            &connection,
            &committing(&connection, &planner, &staged, segment),
        )
    };
    for (one, many) in [(1, 2), (3, 4)] {
        let (one, many) = (timed(one), timed(many));
        assert!(
            many < one * 6,
            "one truncate took {one} steps, {TRUNCATES} took {many}"
        );
    }
    // The many past every row marked each, at the last one's sequence and the first one's time.
    run_all(&connection, &committing(&connection, &planner, &staged, 4));
    let marked: (i64, i64, i64) = connection
        .query_row(
            &format!(
                "SELECT count(*), min(deleted_at), max(deleted_at) FROM orders WHERE seq = {}",
                digits(&format!("2000000 + {TRUNCATES}"))
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(marked, (i64::from(ROWS), 1, 1));
}

#[test]
fn a_commit_without_a_truncate_reads_only_the_rows_of_its_keys() {
    let soft = Deletion::Soft {
        at: "deleted_at".into(),
    };
    for deletion in [Deletion::Hard, soft] {
        let hard = deletion == Deletion::Hard;
        let (connection, planner, staged) = orders(deletion);
        let staging = planner.staging_table("orders");
        let tombstones = planner.tombstone_table("orders");
        // Five hundred rows, and as many tombstones of other keys under an old bound.
        let filled = format!(
            "{count} INSERT INTO orders SELECT i, {seq}, NULL FROM _n; \
             {count} INSERT INTO \"{tombstones}\" SELECT 10000 + i, {seq} FROM _n; \
             INSERT INTO \"{tombstones}\" VALUES (NULL, {bound})",
            count = counting(500),
            seq = digits("i"),
            bound = digits("0"),
        );
        connection.execute_batch(&filled).unwrap();
        // An update, a delete of a row and a delete of none.
        let changes = format!(
            "INSERT INTO \"{staging}\" (_rdlt_pipeline, _rdlt_epoch, _rdlt_segment, id, seq, \
             deleted_at, op) VALUES ('mine', 1, 1, 7, {seq}, NULL, 1), ('mine', 1, 1, 8, {seq}, \
             5, 2), ('mine', 1, 1, 900, {seq}, 5, 2)",
            seq = digits("1000")
        );
        connection.execute_batch(&changes).unwrap();
        for statement in committing(&connection, &planner, &staged, 1) {
            let mut prepared = connection.prepare(&statement.sql).unwrap();
            let params = statement
                .params
                .iter()
                .map(super::super::super::tests::value);
            prepared
                .execute(rusqlite::params_from_iter(params))
                .unwrap();
            let steps = prepared.get_status(rusqlite::StatementStatus::FullscanStep);
            assert!(steps < 50, "hard: {hard}: {steps} steps: {}", statement.sql);
        }
        let count = |sql: &str| -> i64 { connection.query_row(sql, [], |row| row.get(0)).unwrap() };
        let rows = count("SELECT count(*) FROM orders");
        let buried = count(&format!("SELECT count(*) FROM \"{tombstones}\""));
        // The hard delete removed its row and buried both its keys; the soft one marked its row.
        let expected = if hard { (499, 503) } else { (500, 501) };
        assert_eq!((rows, buried), expected, "hard: {hard}");
    }
}
