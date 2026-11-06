use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StringViewArray};

use super::{Bound, allocated_beyond, column_bytes};

fn batch(rows: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("name-{row}")),
    ));
    RecordBatch::try_from_iter([("id", ids), ("name", names)]).unwrap()
}

#[test]
fn a_bound_grows_with_rows_and_the_bytes_of_the_columns_it_names() {
    let batch = batch(10);
    let bound = Bound::new(7).with_rows(3).with_column(1, 2);
    assert_eq!(
        bound.of(&batch),
        7 + 3 * 10 + 2 * column_bytes(batch.column(1).as_ref())
    );
}

#[test]
fn a_bound_naming_a_column_the_batch_lacks_counts_nothing_for_it() {
    assert_eq!(Bound::new(5).with_column(9, 100).of(&batch(4)), 5);
}

#[test]
fn bounds_add_and_scale_term_by_term() {
    let batch = batch(8);
    let one = Bound::new(1).with_rows(2).with_column(0, 1);
    let two = Bound::new(10).with_column(0, 3);
    assert_eq!(
        one.clone().plus(&two).of(&batch),
        one.of(&batch) + two.of(&batch)
    );
    assert_eq!(one.clone().times(4).of(&batch), 4 * one.of(&batch));
}

#[test]
fn a_slice_measures_its_own_rows_not_its_whole_buffer() {
    let whole = batch(1000);
    let slice = whole.slice(0, 10);
    let names = |batch: &RecordBatch| column_bytes(batch.column(1).as_ref());
    assert!(
        names(&slice) * 50 < names(&whole),
        "{} of {}",
        names(&slice),
        names(&whole)
    );
}

#[test]
fn allocated_beyond_counts_new_buffers_and_not_shared_ones() {
    let input = batch(100);
    let shared = input.project(&[1]).unwrap();
    assert_eq!(allocated_beyond(&shared, &input), 0);
    let fresh: ArrayRef = Arc::new(Int64Array::from_iter_values(0..100));
    let grown = RecordBatch::try_from_iter([
        ("name", Arc::clone(input.column(1))),
        ("more", Arc::clone(&fresh)),
    ])
    .unwrap();
    let alone = RecordBatch::try_from_iter([("more", fresh)]).unwrap();
    assert_eq!(
        allocated_beyond(&grown, &input),
        allocated_beyond(&alone, &input)
    );
    assert!(allocated_beyond(&grown, &input) >= 800);
}

#[test]
fn a_view_column_counts_the_text_its_views_reference() {
    let long = "x".repeat(40);
    let views = StringViewArray::from_iter_values((0..100).map(|_| long.as_str()));
    assert!(column_bytes(&views) >= 100 * 40);
    let short = StringViewArray::from_iter_values((0..100).map(|_| "tiny"));
    assert!(column_bytes(&short) < 100 * 40);
    let slice = views.slice(0, 10);
    assert!(column_bytes(&slice) < 20 * 40);
}

#[test]
fn a_view_of_twelve_bytes_is_inline_and_references_nothing() {
    let twelve = StringViewArray::from_iter_values((0..100).map(|_| "123456789012"));
    let short = StringViewArray::from_iter_values((0..100).map(|_| "tiny"));
    assert_eq!(column_bytes(&twelve), column_bytes(&short));
    let thirteen = StringViewArray::from_iter_values((0..100).map(|_| "1234567890123"));
    assert!(column_bytes(&thirteen) >= column_bytes(&short) + 100 * 13);
}
