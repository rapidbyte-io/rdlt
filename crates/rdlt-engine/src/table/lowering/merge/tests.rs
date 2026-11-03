use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use proptest::prelude::*;

use super::{comparing, last_across, last_of_each_key, within};
use crate::cost::tests::peak_alone;

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

/// Which rows of `batches`, each a batch's key columns, in order, are the last of their key.
fn kept_across(batches: &[Vec<ArrayRef>]) -> Vec<bool> {
    last_across(batches).unwrap().iter().collect()
}

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

#[test]
fn a_row_whose_key_a_later_batch_holds_is_not_the_last_of_its_key() {
    let kept = kept_across(&[vec![ints(&[1, 2, 1])], vec![ints(&[2, 3])]]);
    assert_eq!(kept, [false, false, true, true, true]);
}

#[test]
fn keys_of_different_types_are_never_one_key() {
    let texts: ArrayRef = Arc::new(StringArray::from(vec!["1", "2"]));
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
    let kept = kept_across(&[
        vec![ints(&[1, 1])],
        vec![texts],
        vec![floats],
        vec![ints(&[2])],
    ]);
    // The integer 1 is followed only by itself, and the text and the float keys by none.
    assert_eq!(kept, [false, true, true, true, true, true]);
}

#[test]
fn no_batch_keeps_no_row() {
    assert_eq!(last_across(&[]).unwrap().len(), 0);
}

proptest! {
    #[test]
    fn a_sort_across_batches_keeps_what_a_fold_of_their_rows_keeps(
        keys in proptest::collection::vec((0_i64..4, prop_oneof!["a", "b"]), 1..64),
        cuts in proptest::collection::vec(1_usize..8, 0..8),
    ) {
        let keys: Vec<(i64, &str)> = keys.iter().map(|(int, text)| (*int, text.as_str())).collect();
        let mut batches = Vec::new();
        let mut rest = keys.as_slice();
        for cut in cuts.into_iter().take(8) {
            let (batch, after) = rest.split_at(cut.min(rest.len()));
            batches.push(batch);
            rest = after;
        }
        batches.push(rest);
        let columns: Vec<Vec<ArrayRef>> = batches
            .iter()
            .map(|batch| {
                let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(batch.iter().map(|key| key.0)));
                let texts: ArrayRef = Arc::new(StringArray::from_iter_values(batch.iter().map(|key| key.1)));
                vec![ints, texts]
            })
            .collect();
        let kept: Vec<usize> = kept_across(&columns)
            .into_iter()
            .enumerate()
            .filter_map(|(row, kept)| kept.then_some(row))
            .collect();
        prop_assert_eq!(kept, folded(&keys));
    }
}

#[test]
fn comparing_keys_of_long_texts_in_several_batches_allocates_no_more_than_it_says() {
    // Rows of long texts grown batch by batch would hold their buffer and a larger one at once.
    let batches: Vec<Vec<ArrayRef>> = (0..3)
        .map(|batch| {
            let keys = (0..100_i64).map(|row| row % 7 + batch);
            let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(keys.clone()));
            let texts = keys.map(|key| format!("{key:0>2000}"));
            let texts: ArrayRef = Arc::new(StringArray::from_iter_values(texts));
            vec![ints, texts]
        })
        .collect();
    let (peak, last) = peak_alone(|| last_across(&batches).unwrap());
    assert_eq!(last.count_set_bits(), 9);
    let held = comparing(&batches).unwrap();
    assert!(peak <= held, "allocated {peak}, said {held}");
}
