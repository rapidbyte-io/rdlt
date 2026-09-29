use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, ChangeOp, Deletion, MergeKey};

use super::changes::stored;
use super::{Merged, merge, tombstone_schema, written_schema};

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
fn rows(merged: &Merged) -> Vec<(i64, Option<String>, u8, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in &merged.rows {
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

/// Each tombstone as its key, none for a truncate's bound, and its sequence's last byte, by key.
fn tombstones(merged: &Merged) -> Vec<(Option<i64>, u8)> {
    let mut tombstones = Vec::new();
    for batch in &merged.tombstones {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let seqs = batch.column_by_name("seq").unwrap().as_binary::<i32>();
        for index in 0..batch.num_rows() {
            let id = (!ids.is_null(index)).then(|| ids.value(index));
            tombstones.push((id, seqs.value(index)[15]));
        }
    }
    tombstones.sort();
    tombstones
}

fn apply(published: &Merged, batches: &[&[Row]], deletion: Deletion) -> Merged {
    let incoming: Vec<RecordBatch> = batches.iter().map(|rows| written(rows)).collect();
    let (schema, key) = (stored_schema(), key(deletion));
    merge(
        &schema,
        &published.rows,
        &published.tombstones,
        &incoming,
        &key,
    )
    .expect("the rows merge")
}

fn empty() -> Merged {
    Merged {
        rows: Vec::new(),
        tombstones: Vec::new(),
    }
}

#[test]
fn a_change_applies_only_past_the_published_rows_sequence() {
    let published = apply(&empty(), &[&[row(1, "a", 5)]], Deletion::Hard);
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
        &empty(),
        &[&[row(1, "late", 9)], &[row(1, "early", 3)]],
        Deletion::Hard,
    );
    assert_eq!(rows(&merged), [(1, Some("late".into()), 9, None)]);
}

#[test]
fn a_hard_delete_removes_the_row_and_a_later_insert_brings_it_back() {
    let published = apply(
        &empty(),
        &[&[row(1, "a", 1), row(2, "b", 2)]],
        Deletion::Hard,
    );
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
    let published = apply(&empty(), &[&[row(1, "a", 1)]], soft());
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
    let published = apply(
        &empty(),
        &[&[row(1, "a", 1), row(2, "b", 2)]],
        Deletion::Hard,
    );
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
    let published = apply(&empty(), &[&[row(1, "a", 1), row(2, "b", 2)]], soft());
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
    let published = apply(&empty(), &[&[row(1, "kept", 1)]], Deletion::Hard);
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
    let incoming = [written(&[row(1, "a", 5)])];
    let published = merge(&stored_schema(), &[], &[], &incoming, &key).unwrap();
    let incoming = [written(&[row(1, "older", 1)])];
    let merged = merge(&stored_schema(), &published.rows, &[], &incoming, &key).unwrap();
    assert_eq!(rows(&merged), [(1, Some("older".into()), 1, None)]);
    assert_eq!(tombstones(&merged), []);
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

#[test]
fn a_change_replayed_after_a_hard_delete_never_brings_its_row_back() {
    let published = apply(
        &empty(),
        &[&[row(1, "a", 1), row(2, "b", 2)]],
        Deletion::Hard,
    );
    // Deleting a key no row holds buries it too: its earlier insert may arrive again.
    let deleted = apply(
        &published,
        &[&[delete(1, 3, 100), delete(7, 4, 100)]],
        Deletion::Hard,
    );
    assert_eq!(tombstones(&deleted), [(Some(1), 3), (Some(7), 4)]);
    let replayed = apply(
        &deleted,
        &[&[row(1, "a", 1), row(1, "stale", 3), row(7, "g", 2)]],
        Deletion::Hard,
    );
    assert_eq!(rows(&replayed), [(2, Some("b".into()), 2, None)]);
    assert_eq!(tombstones(&replayed), [(Some(1), 3), (Some(7), 4)]);
    // A change sequenced after the delete brings the row back, and its tombstone goes.
    let back = apply(&replayed, &[&[row(1, "again", 5)]], Deletion::Hard);
    assert_eq!(
        rows(&back),
        [
            (1, Some("again".into()), 5, None),
            (2, Some("b".into()), 2, None)
        ]
    );
    assert_eq!(tombstones(&back), [(Some(7), 4)]);
}

#[test]
fn a_change_replayed_after_a_hard_truncate_never_brings_its_row_back() {
    let published = apply(
        &empty(),
        &[&[row(1, "a", 1), delete(2, 2, 100)]],
        Deletion::Hard,
    );
    let truncated = apply(
        &published,
        &[&[truncate(4, 100), delete(3, 5, 100)]],
        Deletion::Hard,
    );
    // The truncate's bound covers the tombstones before it.
    assert_eq!(tombstones(&truncated), [(None, 4), (Some(3), 5)]);
    let replayed = apply(
        &truncated,
        &[&[
            row(1, "a", 1),
            row(3, "c", 3),
            truncate(2, 100),
            row(5, "e", 6),
        ]],
        Deletion::Hard,
    );
    assert_eq!(rows(&replayed), [(5, Some("e".into()), 6, None)]);
    // A replayed truncate leaves the bound where it was.
    assert_eq!(tombstones(&replayed), [(None, 4), (Some(3), 5)]);
    let raised = apply(&replayed, &[&[truncate(7, 100)]], Deletion::Hard);
    assert_eq!(rows(&raised), []);
    assert_eq!(tombstones(&raised), [(None, 7)]);
}

#[test]
fn soft_deletes_leave_no_tombstones() {
    let published = apply(&empty(), &[&[row(1, "a", 1)]], soft());
    let deleted = apply(
        &published,
        &[&[delete(1, 3, 100), truncate(4, 200)]],
        soft(),
    );
    assert_eq!(tombstones(&deleted), []);
    let replayed = apply(&deleted, &[&[row(1, "a", 1)]], soft());
    assert_eq!(rows(&replayed), [(1, Some("a".into()), 4, Some(100))]);
}

#[test]
fn tombstones_stored_before_their_key_widened_still_bury_it() {
    let key = key(Deletion::Hard);
    let narrow = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("seq", DataType::Binary, false),
    ]);
    let stored = RecordBatch::try_new(
        Arc::new(narrow),
        vec![
            Arc::new(arrow_array::Int32Array::from(vec![Some(1), None])),
            Arc::new(BinaryArray::from(vec![
                sequence(5).as_slice(),
                sequence(2).as_slice(),
            ])),
        ],
    )
    .expect("a valid batch");
    let incoming = [written(&[row(1, "a", 4), row(2, "b", 1), row(3, "c", 3)])];
    let merged = merge(&stored_schema(), &[], &[stored], &incoming, &key).expect("the rows merge");
    assert_eq!(rows(&merged), [(3, Some("c".into()), 3, None)]);
    assert_eq!(tombstones(&merged), [(None, 2), (Some(1), 5)]);
    let schema = tombstone_schema(&stored_schema(), &key).expect("a tombstone schema");
    assert_eq!(merged.tombstones[0].schema(), schema);
}
