use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray};
use proptest::prelude::*;
use rdlt_connector::StreamName;

use super::{bits, bits_bytes, reserving};
use crate::cost::tests::peak_alone;

/// A key column of the type `kind` names, from its rows' `keys`: integers, floats of whole values
/// whose zeros carry both signs, or texts of `width` bytes.
fn column(kind: u8, keys: &[u8], width: usize) -> ArrayRef {
    match kind % 3 {
        0 => Arc::new(Int64Array::from_iter_values(
            keys.iter().map(|key| i64::from(*key)),
        )),
        1 => {
            let floats = keys
                .iter()
                .enumerate()
                .map(|(row, key)| match (key, row % 2) {
                    (0, 1) => -0.0,
                    _ => f64::from(*key),
                });
            Arc::new(Float64Array::from_iter_values(floats))
        }
        _ => Arc::new(StringArray::from_iter_values(
            keys.iter().map(|key| format!("{key:0>width$}")),
        )),
    }
}

/// A batch's key columns: one of the type `kind` names, or where `composite`, an integer and a
/// text of `width` bytes beside it.
fn batch(kind: u8, composite: bool, keys: &[u8], width: usize) -> Vec<ArrayRef> {
    if composite {
        vec![column(0, keys, width), column(2, keys, width)]
    } else {
        vec![column(kind, keys, width)]
    }
}

fn names(composite: bool) -> Vec<Arc<str>> {
    if composite {
        vec![Arc::from("id"), Arc::from("tag")]
    } else {
        vec![Arc::from("id")]
    }
}

#[test]
fn a_key_a_later_piece_holds_leaves_no_bit_in_an_earlier_one() {
    let stream = StreamName::new("s").unwrap();
    let columns = vec![batch(1, false, &[0, 1, 0], 0), batch(1, false, &[1, 0], 0)];
    let bits = bits(&stream, &names(false), &columns, vec![3, 2])
        .unwrap()
        .unwrap();
    let set: Vec<Vec<bool>> = bits
        .iter()
        .flatten()
        .map(|bits| bits.iter().collect())
        .collect();
    // The zero of either sign is one key, as the split holds it.
    assert_eq!(set, [vec![false, false, false], vec![true, true]]);
}

#[test]
fn a_key_holding_a_null_leaves_every_piece_without_bits() {
    let stream = StreamName::new("s").unwrap();
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
    let bits = bits(&stream, &names(false), &[vec![ids]], vec![2]).unwrap();
    assert!(bits.is_none());
}

#[test]
fn a_pieces_bits_take_an_array_and_blocks_of_sixty_four_bytes() {
    assert_eq!(bits_bytes(1), 320 + 64);
    assert_eq!(bits_bytes(513), 320 + 128);
}

#[test]
fn comparing_reserves_every_pieces_bits_beside_its_keys() {
    let columns = vec![batch(0, false, &[1, 2, 3], 0), batch(0, false, &[1], 0)];
    let keys = reserving(&columns, &[3, 1]).unwrap();
    let both = reserving(&columns, &[3, 1, 10_000]).unwrap();
    // Each piece adds its bits to what comparing the keys holds.
    assert_eq!(both - keys, bits_bytes(10_000));
    assert!(keys > bits_bytes(3) + bits_bytes(1));
}

proptest! {
    #[test]
    fn comparing_a_flush_s_keys_allocates_no_more_than_it_reserves(
        composite in proptest::bool::ANY,
        batches in proptest::collection::vec(
            (0_u8..3, proptest::collection::vec(0_u8..16, 1..200)),
            1..6,
        ),
        width in 0_usize..80,
    ) {
        let stream = StreamName::new("s").unwrap();
        // A stream's key has its columns in every batch, whose types may differ between batches.
        let columns: Vec<Vec<ArrayRef>> = batches
            .iter()
            .map(|(kind, keys)| batch(*kind, composite, keys, width))
            .collect();
        let rows: Vec<usize> = batches.iter().map(|(_, keys)| keys.len()).collect();
        let reserved = reserving(&columns, &rows).unwrap();
        let names = names(composite);
        let (peak, bits) = peak_alone(|| bits(&stream, &names, &columns, rows.clone()).unwrap());
        prop_assert!(bits.is_some());
        prop_assert!(peak <= reserved, "allocated {peak}, reserved {reserved}");
    }
}
