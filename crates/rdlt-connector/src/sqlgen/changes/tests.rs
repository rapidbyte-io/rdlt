use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch};

use super::super::tests::{apply, columns, create, database, query, run_all, table};
use super::super::tests::{pipeline, segments};
use super::super::{Staged, Statement};
use super::staged_changes;
use crate::destination::{ChangeColumns, Deletion, MergeKey, TableRef};
use crate::error::ConnectorErrorKind;
use crate::id::Epoch;
use crate::types::LogicalType;
use rusqlite::types::Value;

fn changed(name: &str) -> TableRef {
    TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: Some(ChangeColumns {
                op: "op".into(),
                unchanged: Some("unchanged".into()),
                deletion: Deletion::Hard,
            }),
        }),
        ..table(name)
    }
}

#[test]
fn a_change_stream_stages_its_directions_and_keeps_tombstones_of_its_key() {
    let (connection, planner) = database();
    let orders = changed("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let names = [
        "orders".to_owned(),
        planner.staging_table("orders"),
        planner.tombstone_table("orders"),
    ];
    let tables = names
        .clone()
        .map(|name| columns(&connection, &planner, &name));
    let plan = planner
        .change_tables(&orders, [&tables[0], &tables[1], &tables[2]])
        .unwrap();
    run_all(&connection, &plan);
    let [_, staging, tombstones] = names.map(|name| columns(&connection, &planner, &name));
    let staged: Vec<(&str, &str)> = staging
        .iter()
        .map(|column| (column.name.as_str(), column.declared.as_str()))
        .skip(4 + fields.len())
        .collect();
    assert_eq!(staged, [("op", "INTEGER"), ("unchanged", "TEXT")]);
    let kept: Vec<(&str, &str)> = tombstones
        .iter()
        .map(|column| (column.name.as_str(), column.declared.as_str()))
        .collect();
    assert_eq!(kept, [("id", "INTEGER"), ("seq", "BLOB")]);
    // A table merging no changes never needed any of it.
    let unmerged = table("orders");
    assert_eq!(
        planner
            .change_tables(&unmerged, [&tables[0], &tables[1], &[]])
            .unwrap(),
        []
    );
}

#[test]
fn a_change_stream_merging_by_a_column_its_table_lacks_is_refused() {
    let (connection, planner) = database();
    let orders = changed("orders");
    let fields = [("id", LogicalType::Int64, false)];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let target = columns(&connection, &planner, "orders");
    let staging = columns(&connection, &planner, &planner.staging_table("orders"));
    let error = planner
        .change_tables(&orders, [&target, &staging, &[]])
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

/// A written batch of `orders`: key, name, sequence, op, and the unchanged flags `flags`.
fn written(flags: &[Option<Vec<u8>>]) -> RecordBatch {
    let rows = flags.len();
    let ids: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.clone())) as ArrayRef),
        ("name", Arc::new(Int64Array::from(ids.clone())) as ArrayRef),
        ("seq", Arc::new(Int64Array::from(ids)) as ArrayRef),
        ("op", Arc::new(Int8Array::from(vec![1; rows])) as ArrayRef),
        (
            "unchanged",
            Arc::new(BinaryArray::from(
                flags.iter().map(Option::as_deref).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn unchanged_flags_stage_as_the_ordinals_of_the_table_columns_they_name() {
    let (connection, planner) = database();
    let orders = changed("orders");
    let fields = [
        ("seq", LogicalType::Int64, false),
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Int64, true),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let target = columns(&connection, &planner, "orders");
    // Field 1 of the batch, `name`, is column 2 of the table; no flags, or none set, stage none.
    let batch = written(&[Some(vec![0b10]), None, Some(vec![0]), Some(Vec::new())]);
    let staged = staged_changes(&batch, &orders, &target).unwrap();
    let flags = staged
        .column_by_name("unchanged")
        .unwrap()
        .as_string::<i32>();
    let flags: Vec<Option<&str>> = (0..flags.len())
        .map(|row| (!flags.is_null(row)).then(|| flags.value(row)))
        .collect();
    assert_eq!(flags, [Some(",2,"), None, None, None]);
    assert_eq!(staged.num_columns(), batch.num_columns());
    // A table merging no changes stages its batches as they are.
    let plain = staged_changes(&batch, &table("orders"), &target).unwrap();
    assert_eq!(plain, batch);
}

#[test]
fn flags_on_a_key_a_sequence_or_a_column_the_table_lacks_are_refused() {
    let (connection, planner) = database();
    let orders = changed("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Int64, false),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let target = columns(&connection, &planner, "orders");
    // Fields 0, 2 and 1: the key, the sequence, and `name`, which the table lacks.
    for bitmap in [0b1, 0b100, 0b10] {
        let batch = written(&[Some(vec![bitmap])]);
        let error = staged_changes(&batch, &orders, &target).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{bitmap:b}");
    }
    // Flags that are not bytes are refused too.
    let batch = written(&[None]);
    let mut columns = batch.columns().to_vec();
    columns[4] = Arc::new(Int64Array::from(vec![1]));
    let mut fields: Vec<arrow_schema::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields[4] = arrow_schema::Field::new("unchanged", arrow_schema::DataType::Int64, true);
    let schema = arrow_schema::Schema::new(fields);
    let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
    let error = staged_changes(&batch, &orders, &target).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

#[test]
fn a_change_whose_op_is_no_change_op_is_refused() {
    let (connection, planner) = database();
    let orders = changed("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Int64, true),
        ("seq", LogicalType::Int64, false),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let target = columns(&connection, &planner, "orders");
    // Codes past a truncate's are those a commit computes its rows under.
    for op in [3, 4, 7, -1] {
        let batch = written(&[None]);
        let mut columns = batch.columns().to_vec();
        columns[3] = Arc::new(Int8Array::from(vec![op]));
        let batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let staged = staged_changes(&batch, &orders, &target);
        assert_eq!(staged.is_ok(), op == 3, "{op}");
        if let Err(error) = staged {
            assert_eq!(error.kind(), ConnectorErrorKind::Data, "{op}");
        }
    } // A batch without its op column is refused too.
    let batch = written(&[None]).project(&[0, 1, 2, 4]).unwrap();
    let error = staged_changes(&batch, &orders, &target).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

#[test]
fn a_commit_finds_the_rows_its_changes_touch_by_their_key() {
    let (connection, planner) = database();
    let orders = changed("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let names = [
        "orders".to_owned(),
        planner.staging_table("orders"),
        planner.tombstone_table("orders"),
    ];
    let tables = names.map(|name| columns(&connection, &planner, &name));
    let ready = planner
        .change_tables(&orders, [&tables[0], &tables[1], &tables[2]])
        .unwrap();
    run_all(&connection, &ready);
    let columns = columns(&connection, &planner, "orders");
    let staged = Staged {
        name: "orders".into(),
        generation: None,
        merge: orders.merge.clone(),
    };
    let plan = planner
        .publish(
            &staged,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap();
    // The statement computing the commit's rows, and those deleting the rows and tombstones of
    // its keys, never read the table or its tombstones whole: a commit costs what it changes.
    let whole = [
        "orders",
        "_rdlt_p",
        "_rdlt_k",
        "_rdlt_tombstones__orders",
        "_rdlt_t",
    ];
    for index in [0, 1, 4] {
        let explain = Statement {
            sql: format!("EXPLAIN QUERY PLAN {}", plan[index].sql),
            params: plan[index].params.clone(),
        };
        for row in query(&connection, &explain) {
            let Value::Text(step) = &row[3] else {
                panic!("{row:?}")
            };
            let scanned = step
                .strip_prefix("SCAN ")
                .and_then(|rest| rest.split(' ').next());
            assert!(
                scanned.is_none_or(|table| !whole.contains(&table)),
                "statement {index}: {step}"
            );
        }
    }
}

#[test]
fn a_change_table_its_staging_and_its_tombstones_are_indexed_by_its_key() {
    let (connection, planner) = database();
    let orders = changed("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let names = [
        "orders".to_owned(),
        planner.staging_table("orders"),
        planner.tombstone_table("orders"),
    ];
    let ready = |connection: &rusqlite::Connection| {
        let tables = names
            .clone()
            .map(|name| columns(connection, &planner, &name));
        planner
            .change_tables(&orders, [&tables[0], &tables[1], &tables[2]])
            .unwrap()
    };
    run_all(&connection, &ready(&connection));
    // Ready, the tables need no more columns, only their key indexes, which exist.
    let again = ready(&connection);
    assert!(
        again
            .iter()
            .all(|statement| statement.sql.starts_with("CREATE INDEX IF NOT EXISTS")),
        "{again:?}"
    );
    run_all(&connection, &again);
    let listing = Statement {
        sql: "SELECT tbl_name FROM sqlite_master WHERE type = 'index' AND name LIKE '_rdlt_key%' \
              ORDER BY tbl_name"
            .to_owned(),
        params: Vec::new(),
    };
    let indexed: Vec<Value> = query(&connection, &listing)
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    let keyed = [
        "_rdlt_staging__orders",
        "_rdlt_tombstones__orders",
        "orders",
    ]
    .map(|table| Value::Text(table.to_owned()));
    assert_eq!(indexed, keyed);
}
