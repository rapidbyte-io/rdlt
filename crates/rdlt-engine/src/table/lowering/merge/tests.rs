use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use proptest::prelude::*;

use super::{last_of_each_key, within};

/// The rows a merge keeps, the slow way: the last row of each key, in row order.
fn folded(keys: &[(i64, &str)]) -> Vec<usize> {
    let mut last: BTreeMap<(i64, &str), usize> = BTreeMap::new();
    for (row, key) in keys.iter().enumerate() {
        last.insert(*key, row);
    }
    let mut rows: Vec<usize> = last.into_values().collect();
    rows.sort_unstable();
    rows
}

fn kept(keys: &[(i64, &str)]) -> Vec<usize> {
    let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(keys.iter().map(|key| key.0)));
    let texts: ArrayRef = Arc::new(StringArray::from_iter_values(keys.iter().map(|key| key.1)));
    match last_of_each_key(&[ints, texts]).unwrap() {
        Some(keep) => (0..keep.len()).filter(|row| keep.value(*row)).collect(),
        None => (0..keys.len()).collect(),
    }
}

#[test]
fn every_row_is_kept_where_every_key_is_unique() {
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![3, 1, 2]));
    assert!(last_of_each_key(&[ints]).unwrap().is_none());
}

#[test]
fn the_last_row_of_each_key_of_two_columns_is_kept() {
    let keys = [(1, "a"), (1, "b"), (1, "a"), (2, "a"), (1, "b")];
    assert_eq!(kept(&keys), [2, 3, 4]);
}

proptest! {
    #[test]
    fn a_sort_keeps_what_a_fold_keeps(
        keys in proptest::collection::vec((0_i64..4, prop_oneof!["a", "b"]), 1..64)
    ) {
        let keys: Vec<(i64, &str)> = keys.iter().map(|(int, text)| (*int, text.as_str())).collect();
        prop_assert_eq!(kept(&keys), folded(&keys));
    }
}

#[test]
fn a_mask_of_the_kept_rows_places_its_rows_among_the_received_ones() {
    let outer = BooleanArray::from(vec![true, false, true, true, false, true]);
    let inner = BooleanArray::from(vec![false, true, false, true]);
    let composed = within(Some(&outer), &inner);
    assert_eq!(
        composed.iter().map(Option::unwrap).collect::<Vec<_>>(),
        [false, false, true, false, false, true]
    );
    assert_eq!(within(None, &inner), inner);
}
