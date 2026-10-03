use std::ops::Range;
use std::rc::Rc;

use arrow_array::types::Int32Type;
use arrow_array::{Int32Array, RunArray, StringArray};
use arrow_buffer::NullBuffer;

use super::{Ends, Rows, disjoint};

/// The ranges `rows` reads, each as its start and end.
fn ranges(rows: &Rows<'_>) -> Vec<(usize, usize)> {
    rows.ranges()
        .map(|range| (range.start, range.end))
        .collect()
}

/// `spans`, made disjoint, each as its start and end.
fn merged(mut spans: Vec<Range<usize>>) -> Vec<(usize, usize)> {
    disjoint(&mut spans);
    spans.iter().map(|span| (span.start, span.end)).collect()
}

fn listed(rows: &[usize]) -> Rc<Rows<'static>> {
    Rc::new(Rows::Listed(Rc::new(rows.to_vec())))
}

#[test]
fn rows_listed_are_read_as_ranges_of_the_consecutive_ones() {
    assert_eq!(
        ranges(&listed(&[0, 1, 2, 5, 7, 8])),
        [(0, 3), (5, 6), (7, 9)]
    );
    assert_eq!(ranges(&listed(&[4])), [(4, 5)]);
    assert_eq!(ranges(&listed(&[])), []);
}

#[test]
fn valid_rows_are_read_where_a_sliced_null_buffer_sets_them() {
    // Bits 1 and 3 of four, sliced from the first: the slice's rows 0 and 2.
    let nulls = NullBuffer::from(vec![false, true, false, true]).slice(1, 3);
    let all = Rows::Valid(Rc::new(Rows::All(3)), nulls.clone());
    assert_eq!(ranges(&all), [(0, 1), (2, 3)]);
    // Of the rows from the second on, only the third.
    let from_second = std::iter::once(1..3).collect();
    let later = Rows::Valid(Rc::new(Rows::Ranges(Rc::new(from_second))), nulls);
    assert_eq!(ranges(&later), [(2, 3)]);
}

#[test]
fn a_fixed_size_list_s_rows_name_their_items_so_many_a_row() {
    assert_eq!(ranges(&Rows::Fixed(Rc::new(Rows::All(2)), 3)), [(0, 6)]);
    assert_eq!(
        ranges(&Rows::Fixed(listed(&[1, 3, 4]), 3)),
        [(3, 6), (9, 15)]
    );
    assert_eq!(ranges(&Rows::Fixed(listed(&[1]), 0)), []);
}

#[test]
fn runs_name_each_value_once_however_many_of_their_rows_are_named() {
    // Runs of rows 0..5, 5..6 and 6..9.
    let ends = Int32Array::from(vec![5, 6, 9]);
    let values = StringArray::from(vec!["a", "b", "c"]);
    let runs = RunArray::<Int32Type>::try_new(&ends, &values).unwrap();
    let ends = Ends::I32(runs.run_ends());
    // Rows 0 and 2 lie in the first run, rows 3 and 7 in the first and the third.
    assert_eq!(ranges(&Rows::Runs(listed(&[0, 2]), ends)), [(0, 1)]);
    assert_eq!(ranges(&Rows::Runs(listed(&[3, 7]), ends)), [(0, 1), (2, 3)]);
    assert_eq!(ranges(&Rows::Runs(listed(&[4, 5, 6]), ends)), [(0, 3)]);
}

#[test]
fn spans_are_merged_where_they_overlap_or_touch_and_kept_apart_otherwise() {
    let spans = vec![7..9, 4..6, 2..3, 0..2, 1..2, 10..12, 11..11];
    assert_eq!(merged(spans), [(0, 3), (4, 6), (7, 9), (10, 12)]);
    assert_eq!(merged(vec![0..10, 2..3, 5..12]), [(0, 12)]);
    assert_eq!(merged(Vec::new()), []);
}
