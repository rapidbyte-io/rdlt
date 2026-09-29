use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch};

use super::super::tests::{apply, columns, create, database, run_all, table};
use super::staged_changes;
use crate::destination::{ChangeColumns, Deletion, MergeKey, TableRef};
use crate::error::ConnectorErrorKind;
use crate::types::LogicalType;

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
    // Ready, the tables need nothing more; a table merging no changes never did.
    assert_eq!(
        planner
            .change_tables(&orders, [&tables[0], &staging, &tombstones])
            .unwrap(),
        []
    );
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
