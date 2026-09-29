use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, ChangeOp, Deletion, MergeKey};

use super::changes::stored;
use super::{merge, written_schema};

/// One change: its key, value, sequence, op, and the fields it flags unchanged.
struct Row {
    id: Option<i64>,
    value: Option<&'static str>,
    seq: u8,
    op: ChangeOp,
    unchanged: Option<Vec<u8>>,
    at: Option<i64>,
}

fn row(id: i64, value: &'static str, seq: u8) -> Row {
    Row {
        id: Some(id),
        value: Some(value),
        seq,
        op: ChangeOp::Update,
        unchanged: None,
        at: None,
    }
}

fn delete(id: i64, seq: u8, at: i64) -> Row {
    Row {
        id: Some(id),
        value: None,
        seq,
        op: ChangeOp::Delete,
        unchanged: None,
        at: Some(at),
    }
}

fn truncate(seq: u8, at: i64) -> Row {
    Row {
        id: None,
        value: None,
        seq,
        op: ChangeOp::Truncate,
        unchanged: None,
        at: Some(at),
    }
}

fn sequence(seq: u8) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[15] = seq;
    bytes
}

fn stored_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("seq", DataType::Binary, false),
        Field::new("at", DataType::Int64, true),
    ]))
}

/// The changes as a written batch: the stored columns, then the op and unchanged columns.
fn written(rows: &[Row]) -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "id",
            Arc::new(rows.iter().map(|row| row.id).collect::<Int64Array>()),
        ),
        (
            "value",
            Arc::new(rows.iter().map(|row| row.value).collect::<StringArray>()),
        ),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| sequence(row.seq)),
            )),
        ),
        (
            "at",
            Arc::new(rows.iter().map(|row| row.at).collect::<Int64Array>()),
        ),
        (
            "op",
            Arc::new(
                rows.iter()
                    .map(|row| Some(row.op.code()))
                    .collect::<Int8Array>(),
            ),
        ),
        (
            "unchanged",
            Arc::new(
                rows.iter()
                    .map(|row| row.unchanged.clone())
                    .collect::<BinaryArray>(),
            ),
        ),
    ];
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

fn key(deletion: Deletion) -> MergeKey {
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: Some(ChangeColumns {
            op: "op".into(),
            unchanged: Some("unchanged".into()),
            deletion,
        }),
    }
}

fn soft() -> Deletion {
    Deletion::Soft { at: "at".into() }
}

/// Each published row as its key, value, sequence's last byte and deletion time, by key.
fn rows(batches: &[RecordBatch]) -> Vec<(i64, Option<String>, u8, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let values = batch.column_by_name("value").unwrap().as_string::<i32>();
        let seqs = batch.column_by_name("seq").unwrap().as_binary::<i32>();
        let at = batch
            .column_by_name("at")
            .unwrap()
            .as_primitive::<Int64Type>();
        for index in 0..batch.num_rows() {
            let value = (!values.is_null(index)).then(|| values.value(index).to_owned());
            let deleted = (!at.is_null(index)).then(|| at.value(index));
            rows.push((ids.value(index), value, seqs.value(index)[15], deleted));
        }
    }
    rows.sort();
    rows
}

fn apply(published: &[RecordBatch], batches: &[&[Row]], deletion: Deletion) -> Vec<RecordBatch> {
    let incoming: Vec<RecordBatch> = batches.iter().map(|rows| written(rows)).collect();
    merge(&stored_schema(), published, &incoming, &key(deletion)).expect("the rows merge")
}

#[test]
fn a_change_applies_only_past_the_published_rows_sequence() {
    let published = apply(&[], &[&[row(1, "a", 5)]], Deletion::Hard);
    let merged = apply(
        &published,
        &[&[row(1, "stale", 4), row(1, "replayed", 5)]],
        Deletion::Hard,
    );
    assert_eq!(rows(&merged), [(1, Some("a".into()), 5, None)]);
    let merged = apply(&published, &[&[row(1, "b", 6)]], Deletion::Hard);
    assert_eq!(rows(&merged), [(1, Some("b".into()), 6, None)]);
}

#[test]
fn rows_apply_in_sequence_order_whichever_batch_carries_them() {
    let merged = apply(
        &[],
        &[&[row(1, "late", 9)], &[row(1, "early", 3)]],
        Deletion::Hard,
    );
    assert_eq!(rows(&merged), [(1, Some("late".into()), 9, None)]);
}

#[test]
fn a_hard_delete_removes_the_row_and_a_later_insert_brings_it_back() {
    let published = apply(&[], &[&[row(1, "a", 1), row(2, "b", 2)]], Deletion::Hard);
    let merged = apply(&published, &[&[delete(1, 3, 100)]], Deletion::Hard);
    assert_eq!(rows(&merged), [(2, Some("b".into()), 2, None)]);
    let merged = apply(&merged, &[&[row(1, "again", 4)]], Deletion::Hard);
    assert_eq!(
        rows(&merged),
        [
            (1, Some("again".into()), 4, None),
            (2, Some("b".into()), 2, None)
        ]
    );
}

#[test]
fn a_soft_delete_keeps_the_values_and_records_when_and_its_sequence() {
    let published = apply(&[], &[&[row(1, "a", 1)]], soft());
    let merged = apply(
        &published,
        &[&[delete(1, 3, 100), delete(7, 4, 100)]],
        soft(),
    );
    assert_eq!(rows(&merged), [(1, Some("a".into()), 3, Some(100))]);
    let merged = apply(&merged, &[&[row(1, "back", 5)]], soft());
    assert_eq!(rows(&merged), [(1, Some("back".into()), 5, None)]);
}

#[test]
fn a_truncate_removes_every_row_before_it_and_none_after() {
    let published = apply(&[], &[&[row(1, "a", 1), row(2, "b", 2)]], Deletion::Hard);
    let merged = apply(
        &published,
        &[&[row(3, "c", 3), truncate(4, 100), row(4, "d", 5)]],
        Deletion::Hard,
    );
    assert_eq!(rows(&merged), [(4, Some("d".into()), 5, None)]);
    let merged = apply(&published, &[&[truncate(2, 100)]], soft());
    assert_eq!(
        rows(&merged),
        [
            (1, Some("a".into()), 2, Some(100)),
            (2, Some("b".into()), 2, None)
        ]
    );
}

#[test]
fn a_row_already_deleted_keeps_when_it_was_deleted() {
    let published = apply(&[], &[&[row(1, "a", 1), row(2, "b", 2)]], soft());
    let deleted = apply(&published, &[&[delete(1, 3, 100)]], soft());
    // A later delete of the row, and a truncate, move its sequence but not its deletion.
    let merged = apply(&deleted, &[&[delete(1, 4, 200), truncate(5, 300)]], soft());
    assert_eq!(
        rows(&merged),
        [
            (1, Some("a".into()), 5, Some(100)),
            (2, Some("b".into()), 5, Some(300))
        ]
    );
}

#[test]
fn an_update_keeps_the_published_value_of_each_column_it_flags_unchanged() {
    let published = apply(&[], &[&[row(1, "kept", 1)]], Deletion::Hard);
    let partial = |id, seq| Row {
        id: Some(id),
        value: None,
        seq,
        op: ChangeOp::Update,
        // Bit 1 of the written batch: the value column.
        unchanged: Some(vec![0b10]),
        at: None,
    };
    let merged = apply(
        &published,
        &[&[partial(1, 2), partial(2, 3)]],
        Deletion::Hard,
    );
    assert_eq!(
        rows(&merged),
        [(1, Some("kept".into()), 2, None), (2, None, 3, None)]
    );
}

#[test]
fn a_table_without_changes_upserts_as_before() {
    let key = MergeKey {
        changes: None,
        ..key(Deletion::Hard)
    };
    let published = merge(&stored_schema(), &[], &[written(&[row(1, "a", 5)])], &key).unwrap();
    let merged = merge(
        &stored_schema(),
        &published,
        &[written(&[row(1, "older", 1)])],
        &key,
    )
    .unwrap();
    assert_eq!(rows(&merged), [(1, Some("older".into()), 1, None)]);
}

#[test]
fn a_change_stream_stores_neither_its_ops_nor_its_unchanged_flags() {
    for unchanged in [Some("unchanged"), None] {
        let changes = ChangeColumns {
            op: "op".into(),
            unchanged: unchanged.map(Into::into),
            deletion: Deletion::Hard,
        };
        let written = written_schema(&stored_schema(), &changes);
        let kept: Vec<String> = stored(&written, &changes)
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(kept, ["id", "value", "seq", "at"], "{unchanged:?}");
    }
}
