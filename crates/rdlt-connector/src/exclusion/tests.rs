use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, Int64Array, LargeBinaryArray,
    RecordBatch, RecordBatchOptions, StringArray,
};
use arrow_schema::Schema;
use bytes::Bytes;

use super::{Exclusion, Projection};
use crate::change::{OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN, validate_change_batch};
use crate::sink::Push;

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn texts(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn excluding(names: &[&str]) -> Projection {
    Projection::new(Exclusion::new(names.iter().copied()))
}

fn names(batch: &RecordBatch) -> Vec<String> {
    let schema = batch.schema();
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

/// Three rows of `id`, `secret` and `note`.
fn people() -> RecordBatch {
    batch(vec![
        ("id", ints(&[1, 2, 3])),
        ("secret", texts(&["a", "b", "c"])),
        ("note", texts(&["x", "y", "z"])),
    ])
}

#[test]
fn an_exclusion_names_its_columns_once_in_order() {
    let exclusion = Exclusion::new(["b", "a", "b"]);
    assert_eq!(exclusion.columns().collect::<Vec<_>>(), ["a", "b"]);
    assert!(exclusion.contains("a") && !exclusion.contains("c"));
    assert!(!exclusion.is_empty());
    assert!(Exclusion::default().is_empty());
    assert_eq!(Exclusion::new(["a", "b"]), Exclusion::new(["b", "a"]));
}

#[test]
fn every_exclusion_of_no_column_shares_one_allocation() {
    let nothing = Exclusion::default();
    assert!(Arc::ptr_eq(&nothing.columns, &Exclusion::default().columns));
    let named = Exclusion::new(Vec::<String>::new());
    assert!(Arc::ptr_eq(&nothing.columns, &named.columns));
    assert!(!Arc::ptr_eq(
        &nothing.columns,
        &Exclusion::new(["a"]).columns
    ));
}

#[test]
fn a_push_loses_its_excluded_columns_and_shares_the_rest() {
    let pushed = people();
    let projected = excluding(&["secret"]).arrow(pushed.clone()).unwrap();
    assert_eq!(names(&projected), ["id", "note"]);
    assert_eq!(projected.num_rows(), 3);
    assert!(Arc::ptr_eq(projected.column(0), pushed.column(0)));
    assert!(Arc::ptr_eq(projected.column(1), pushed.column(2)));
}

#[test]
fn a_batch_holding_no_excluded_column_is_left_as_it_is() {
    let pushed = people();
    for mut projection in [excluding(&[]), excluding(&["absent"])] {
        let projected = projection.arrow(pushed.clone()).unwrap();
        assert!(Arc::ptr_eq(&projected.schema(), &pushed.schema()));
        assert_eq!(projected, pushed);
    }
}

#[test]
fn batches_of_one_schema_share_the_schema_they_are_projected_to() {
    let mut projection = excluding(&["secret"]);
    let first = projection.arrow(people()).unwrap();
    // Each `people()` has a schema of its own, equal to the others.
    let equal = projection.arrow(people()).unwrap();
    assert!(Arc::ptr_eq(&first.schema(), &equal.schema()));
    let same = people();
    let again = batch(vec![
        ("id", ints(&[4])),
        ("secret", texts(&["d"])),
        ("note", texts(&["w"])),
    ]);
    let again = RecordBatch::try_new(same.schema(), again.columns().to_vec()).unwrap();
    projection.arrow(same).unwrap();
    let shared = projection.arrow(again).unwrap();
    assert!(Arc::ptr_eq(&first.schema(), &shared.schema()));
    assert_eq!(shared.column(0).as_ref(), ints(&[4]).as_ref());
}

#[test]
fn a_batch_already_projected_passes_through() {
    let mut projection = excluding(&["secret"]);
    let projected = projection.arrow(people()).unwrap();
    let again = projection.arrow(projected.clone()).unwrap();
    assert!(Arc::ptr_eq(&again.schema(), &projected.schema()));
    // It still knows the schema the projected batch came from.
    let next = projection.arrow(people()).unwrap();
    assert!(Arc::ptr_eq(&next.schema(), &projected.schema()));
}

#[test]
fn a_schema_that_differs_from_the_last_is_projected_anew() {
    let mut projection = excluding(&["secret"]);
    projection.arrow(people()).unwrap();
    let other = batch(vec![("secret", texts(&["a"])), ("n", ints(&[1]))]);
    let projected = projection.arrow(other).unwrap();
    assert_eq!(names(&projected), ["n"]);
    let projected = projection.arrow(people()).unwrap();
    assert_eq!(names(&projected), ["id", "note"]);
}

#[test]
fn excluding_every_column_keeps_the_rows() {
    let pushed = batch(vec![("secret", texts(&["a", "b"]))]);
    let projected = excluding(&["secret", "note"]).arrow(pushed).unwrap();
    assert_eq!(projected.num_columns(), 0);
    assert_eq!(projected.num_rows(), 2);
}

#[test]
fn a_projection_keeps_the_schema_s_metadata() {
    let pushed = people();
    let metadata = [("origin".to_owned(), "test".to_owned())].into();
    let schema = Arc::new(Schema::new_with_metadata(
        pushed.schema().fields().clone(),
        metadata,
    ));
    let pushed = RecordBatch::try_new(schema, pushed.columns().to_vec()).unwrap();
    let projected = excluding(&["note"]).arrow(pushed.clone()).unwrap();
    assert_eq!(projected.schema().metadata(), pushed.schema().metadata());
}

fn seq(rows: usize) -> ArrayRef {
    let values: Vec<[u8; 16]> = (0..rows).map(|row| (row as u128).to_be_bytes()).collect();
    Arc::new(FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap())
}

/// Bitmaps flagging `ordinals`, a row each; `None` for a null row.
fn flags(rows: &[Option<&[usize]>]) -> BinaryArray {
    rows.iter()
        .map(|row| {
            row.map(|ordinals| {
                let mut bitmap = Vec::new();
                for ordinal in ordinals {
                    bitmap.resize(bitmap.len().max(ordinal / 8 + 1), 0);
                    bitmap[ordinal / 8] |= 1 << (ordinal % 8);
                }
                bitmap
            })
        })
        .collect()
}

/// A change batch of `id`, `secret`, `value` and `n`, then its change columns, the unchanged
/// flags `unchanged`.
fn changes(unchanged: ArrayRef) -> RecordBatch {
    let rows = unchanged.len();
    let ids: Vec<i64> = (0..i64::try_from(rows).unwrap()).collect();
    batch(vec![
        ("id", ints(&ids)),
        ("secret", texts(&vec!["s"; rows])),
        ("value", texts(&vec!["v"; rows])),
        ("n", ints(&ids)),
        (OP_COLUMN, Arc::new(Int8Array::from(vec![1; rows]))),
        (SEQ_COLUMN, seq(rows)),
        (UNCHANGED_COLUMN, unchanged),
    ])
}

#[test]
fn a_change_batch_s_flags_move_with_the_fields_it_keeps() {
    let pushed = changes(Arc::new(flags(&[
        Some(&[1]),
        Some(&[2]),
        Some(&[1, 3]),
        None,
        Some(&[]),
    ])));
    let projected = excluding(&["secret"]).changes(pushed).unwrap();
    validate_change_batch(&projected).unwrap();
    assert_eq!(
        names(&projected),
        ["id", "value", "n", OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN]
    );
    let moved = projected
        .column_by_name(UNCHANGED_COLUMN)
        .unwrap()
        .as_binary::<i32>()
        .clone();
    // A flag on the excluded field goes; those after it move down one ordinal.
    let expected = flags(&[Some(&[]), Some(&[1]), Some(&[2]), None, Some(&[])]);
    assert_eq!(moved, expected);
}

#[test]
fn a_change_batch_whose_flags_are_not_binary_keeps_them_for_its_validation() {
    let large: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(&b"\x02"[..])]));
    let pushed = changes(Arc::clone(&large));
    let projected = excluding(&["secret"]).changes(pushed).unwrap();
    let kept = projected.column_by_name(UNCHANGED_COLUMN).unwrap();
    assert!(Arc::ptr_eq(kept, &large));
    assert!(validate_change_batch(&projected).is_err());
}

#[test]
fn an_arrow_push_keeps_a_column_named_as_the_flags_as_it_is() {
    let data: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"\x02"[..])]));
    let pushed = batch(vec![
        ("secret", texts(&["s"])),
        (UNCHANGED_COLUMN, Arc::clone(&data)),
    ]);
    let projected = excluding(&["secret"]).arrow(pushed).unwrap();
    assert!(Arc::ptr_eq(projected.column(0), &data));
}

#[test]
fn a_push_is_projected_as_its_kind_says() {
    let mut projection = excluding(&["secret"]);
    let json = Bytes::from_static(br#"{"secret":1}"#);
    assert_eq!(
        projection.push(Push::Json(json.clone())).unwrap(),
        Push::Json(json)
    );
    let Push::Arrow(arrow) = projection.push(Push::Arrow(people())).unwrap() else {
        panic!("an Arrow push stays one");
    };
    assert_eq!(names(&arrow), ["id", "note"]);
    let flagged = changes(Arc::new(flags(&[Some(&[2])])));
    let Push::Changes(changed) = projection.push(Push::Changes(flagged)).unwrap() else {
        panic!("a change push stays one");
    };
    let moved = changed.column_by_name(UNCHANGED_COLUMN).unwrap();
    assert_eq!(moved.as_binary::<i32>().value(0), [0b10]);
}

#[test]
fn a_batch_of_no_rows_is_projected_too() {
    let pushed = people().slice(0, 0);
    let options = RecordBatchOptions::new().with_row_count(Some(0));
    let empty =
        RecordBatch::try_new_with_options(pushed.schema(), pushed.columns().to_vec(), &options)
            .unwrap();
    let projected = excluding(&["secret"]).arrow(empty).unwrap();
    assert_eq!(
        (projected.num_rows(), names(&projected)),
        (0, vec!["id".to_owned(), "note".to_owned()])
    );
}
