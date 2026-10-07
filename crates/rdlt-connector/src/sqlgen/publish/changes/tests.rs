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

/// A database holding `orders` with `width` columns beside its key, sequence and deletion
/// time, whose changes may flag columns unchanged and whose deletes are `hard` or soft.
fn wide_orders(width: usize, hard: bool) -> (Connection, SqlPlanner<Sqlite>, Staged) {
    let (connection, planner) = database();
    let mut merge = key(if hard {
        Deletion::Hard
    } else {
        Deletion::Soft {
            at: "deleted_at".into(),
        }
    });
    merge.changes.as_mut().unwrap().unchanged = Some("unchanged".into());
    let orders = crate::destination::TableRef {
        merge: Some(merge),
        ..table("orders")
    };
    let names: Vec<String> = (0..width).map(|column| format!("c{column}")).collect();
    let mut fields = vec![
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
        ("deleted_at", LogicalType::Int64, true),
    ];
    fields.extend(
        names
            .iter()
            .map(|name| (name.as_str(), LogicalType::Int64, true)),
    );
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let tables = [
        "orders".to_owned(),
        planner.staging_table("orders"),
        planner.tombstone_table("orders"),
    ]
    .map(|name| columns(&connection, &planner, &name));
    let ready = planner.change_tables_of(&orders, [&tables[0], &tables[1], &tables[2]]);
    run_all(&connection, &ready.unwrap());
    run_all(&connection, &planner.key_indexes_of(&orders));
    let staged = Staged {
        name: "orders".to_owned(),
        generation: None,
        merge: orders.merge,
    };
    (connection, planner, staged)
}

/// The flags of a staged update that flags every column of `orders` past its key and its
/// sequence, as a literal.
fn every_column_flagged(width: usize) -> String {
    format!("X'0000{}'", "01".repeat(width + 1))
}

/// The steps a commit of `rows` updates takes, each flagging every column of a table of `width`
/// columns beside its key and sequence, in a table holding their rows.
fn flagged_commit(rows: u32, width: usize, hard: bool) -> u64 {
    let (connection, planner, staged) = wide_orders(width, hard);
    let staging = planner.staging_table("orders");
    let names: Vec<String> = (0..width).map(|column| format!("c{column}")).collect();
    let sevens = vec!["7"; width].join(", ");
    let listed = names.join(", ");
    let flags = every_column_flagged(width);
    let filled = format!(
        "{count} INSERT INTO orders (id, seq, {listed}) SELECT i, {old}, {sevens} FROM _n; \
         {count} INSERT INTO \"{staging}\" (_rdlt_pipeline, _rdlt_epoch, _rdlt_segment, id, seq, \
         op, unchanged) SELECT 'mine', 1, 1, i, {new}, 1, {flags} FROM _n",
        count = counting(rows),
        old = digits("i"),
        new = digits("1000000 + i"),
    );
    connection.execute_batch(&filled).unwrap();
    let plan = committing(&connection, &planner, &staged, 1);
    let steps = planned(&connection, &plan);
    // The commit kept every flagged value and moved each row to its change's sequence.
    run_all(&connection, &plan);
    let kept: (i64, i64) = connection
        .query_row(
            &format!(
                "SELECT count(*), sum(c{}) FROM orders WHERE seq > {}",
                width - 1,
                digits("1000000")
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        kept,
        (i64::from(rows), 7 * i64::from(rows)),
        "width {width}"
    );
    steps
}

#[test]
fn flagged_updates_cost_their_rows_and_columns_not_the_columns_squared() {
    for hard in [true, false] {
        let steps = |rows, width| flagged_commit(rows, width, hard);
        // Four times the columns: fewer than four times the steps, since a row costs some apart
        // from its columns. A plan whose rows each cost the square of the columns took eight
        // times as many.
        let (narrow, wide) = (steps(40, 30), steps(40, 120));
        assert!(
            wide < narrow * 4,
            "hard: {hard}: 30 columns took {narrow} steps, 120 took {wide}"
        );
        // Four times the rows: about four times the steps.
        let (few, many) = (steps(40, 60), steps(160, 60));
        assert!(
            many < few * 5,
            "hard: {hard}: 40 rows took {few} steps, 160 took {many}"
        );
    }
}

#[test]
fn only_a_stream_that_flags_columns_merges_through_passes_over_its_flags() {
    let joined = |plan: &[Statement]| -> String {
        let sql: Vec<&str> = plan
            .iter()
            .map(|statement| statement.sql.as_str())
            .collect();
        sql.join("\n")
    };
    for hard in [true, false] {
        let deletion = if hard {
            Deletion::Hard
        } else {
            Deletion::Soft {
                at: "deleted_at".into(),
            }
        };
        let (connection, planner, staged) = orders(deletion);
        let plain = joined(&committing(&connection, &planner, &staged, 1));
        assert!(!plain.contains("_rdlt_set"), "hard: {hard}: {plain}");
        // Where rows flag columns, each column but the key and the sequence is passed over.
        let (connection, planner, staged) = wide_orders(2, hard);
        let flagged = joined(&committing(&connection, &planner, &staged, 1));
        for column in ["c0", "c1", "deleted_at"] {
            assert!(
                flagged.contains(&format!("END) AS \"{column}\"")),
                "{column}: {flagged}"
            );
        }
        for column in ["id", "seq"] {
            assert!(
                !flagged.contains(&format!("END) AS \"{column}\"")),
                "{column}: {flagged}"
            );
        }
    }
}
