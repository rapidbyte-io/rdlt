use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Int32Array, UInt32Array};
use arrow_select::filter::FilterBuilder;
use proptest::prelude::*;
use rdlt_connector::{Epoch, SegmentId};

use super::{Columns, Lineage, Parent, Received};

fn positions(values: &[u32]) -> ArrayRef {
    Arc::new(UInt32Array::from(values.to_vec()))
}

fn received(epoch: u64, segment: u64, first_row: u64) -> Received {
    Received {
        epoch: Epoch(epoch),
        segment: SegmentId(segment),
        first_row,
    }
}

/// A child part's lineage: its rows at `row`, their parents at `parent` of part `parent_part`,
/// their roots at `root`, each the first item of its array.
fn child(part: u32, row: &[u32], (parent_part, parent): (u32, &[u32]), root: &[u32]) -> Lineage {
    let count = i32::try_from(row.len()).unwrap();
    Lineage {
        part,
        row: positions(row),
        keys: None,
        parent: Some(Parent {
            path: vec![Arc::from("items")],
            part: parent_part,
            row: positions(parent),
            root_row: positions(root),
            idx: Arc::new(Int32Array::from_iter_values(0..count)),
        }),
    }
}

/// Each value of `array`, an array of ids.
fn ids(array: &ArrayRef) -> Vec<Vec<u8>> {
    let ids = array.as_binary::<i32>();
    (0..ids.len()).map(|row| ids.value(row).to_vec()).collect()
}

/// A positional id's bytes: `head`, the varints of epoch, segment and first row, then `part`
/// and `row`, each under 241, a byte each.
fn id(head: &[u8], part: u8, row: u8) -> Vec<u8> {
    [head, &[part, row]].concat()
}

#[test]
fn a_rows_id_is_its_position_as_numbers_whose_bytes_compare_as_they_do() {
    let lineage = child(2, &[0, 300], (1, &[5, 5]), &[0, 1]);
    let (own, parent) = lineage.columns(&received(7, 3, 241)).unwrap();
    // Epoch 7, segment 3, and 241, the first number that takes two bytes.
    let head = [7_u8, 3, 0xf1, 0x01];
    assert_eq!(
        ids(&own),
        [id(&head, 2, 0), [&head[..], &[2, 0xf1, 0x3c]].concat()],
        "300 is 240 and 60 more"
    );
    let [parents, roots, idx] = parent.expect("a child row has a parent");
    assert_eq!(ids(&parents), [id(&head, 1, 5), id(&head, 1, 5)]);
    assert_eq!(ids(&roots), [id(&head, 0, 0), id(&head, 0, 1)]);
    assert_eq!(idx.as_primitive::<Int32Type>().values(), &[0, 1]);
    let own = ids(&own);
    assert!(own[0] < own[1], "ids order as their rows' positions");
}

#[test]
fn a_merge_streams_roots_are_their_keys_ids_and_its_children_name_them_by_their_roots() {
    let keys: ArrayRef = Arc::new(BinaryArray::from_iter_values([b"k0", b"k1"]));
    let keyed = |lineage: Lineage| Lineage {
        keys: Some(Arc::clone(&keys)),
        ..lineage
    };
    let at = received(1, 1, 0);
    let roots = keyed(Lineage {
        part: 0,
        row: positions(&[1, 0]),
        keys: None,
        parent: None,
    });
    let (own, parent) = roots.columns(&at).unwrap();
    assert_eq!(ids(&own), [b"k1".to_vec(), b"k0".to_vec()]);
    assert!(parent.is_none());
    let items = keyed(child(1, &[0, 1, 2], (0, &[1, 1, 0]), &[1, 1, 0]));
    let (own, parent) = items.columns(&at).unwrap();
    let [parents, roots, _] = parent.unwrap();
    let head = [1_u8, 1, 0];
    assert_eq!(
        ids(&own),
        [id(&head, 1, 0), id(&head, 1, 1), id(&head, 1, 2)]
    );
    let named = [b"k1".to_vec(), b"k1".to_vec(), b"k0".to_vec()];
    assert_eq!(ids(&parents), named, "a root's items name it by its key");
    assert_eq!(ids(&roots), named);
    let tags = keyed(child(2, &[0], (1, &[2]), &[0]));
    let (_, parent) = tags.columns(&at).unwrap();
    let [parents, roots, _] = parent.unwrap();
    assert_eq!(
        ids(&parents),
        [id(&head, 1, 2)],
        "an item's items name it by position"
    );
    assert_eq!(ids(&roots), [b"k0".to_vec()]);
}

#[test]
fn a_parts_ids_take_one_buffer_of_exactly_their_bytes() {
    let lineage = child(3, &[0, 240, 241, 70_000], (1, &[0, 0, 1, 2]), &[0, 0, 1, 2]);
    let (own, _) = lineage.columns(&received(1, 2, 3)).unwrap();
    let ids = own.as_binary::<i32>();
    let bytes: usize = (0..ids.len()).map(|row| ids.value(row).len()).sum();
    assert_eq!(
        bytes,
        4 * 4 + 1 + 1 + 2 + 4,
        "a head of four bytes, then each position's"
    );
    assert_eq!(ids.values().capacity(), bytes);
    assert_eq!(
        ids.offsets().inner().inner().capacity(),
        (ids.len() + 1) * 4
    );
}

#[test]
fn the_widest_id_takes_thirty_seven_bytes() {
    let lineage = Lineage {
        part: u32::MAX,
        row: positions(&[u32::MAX]),
        keys: None,
        parent: None,
    };
    let at = received(u64::MAX, u64::MAX, u64::MAX);
    let (own, _) = lineage.columns(&at).unwrap();
    assert_eq!(ids(&own)[0].len(), 37);
}

/// Every column of `columns` as ids or positions, to compare.
fn flat((own, parent): &Columns) -> Vec<ArrayRef> {
    let mut flat = vec![Arc::clone(own)];
    flat.extend(parent.iter().flatten().cloned());
    flat
}

/// A child part's lineage of `rows` rows whose parents and roots, among eight roots' rows, are
/// drawn, keyed or not.
fn lineage() -> impl Strategy<Value = Lineage> {
    (0_u32..4, 0_usize..48, any::<bool>()).prop_flat_map(|(part, rows, keyed)| {
        let drawn = proptest::collection::vec(0_u32..8, rows);
        (drawn.clone(), drawn).prop_map(move |(parents, roots)| {
            let row: Vec<u32> = (0..u32::try_from(rows).unwrap()).collect();
            let mut lineage = child(part + 1, &row, (part, &parents), &roots);
            if keyed {
                let keys = (0..8_u8).map(|key| [key; 32]);
                lineage.keys = Some(Arc::new(BinaryArray::from_iter_values(keys)));
            }
            lineage
        })
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn a_rows_ids_do_not_depend_on_how_its_part_is_cut_or_what_rows_are_dropped(
        lineage in lineage(),
        cut in any::<prop::sample::Index>(),
        kept in proptest::collection::vec(any::<bool>(), 48),
        epoch in any::<u64>(),
        first_row in any::<u64>(),
    ) {
        let at = received(epoch, 9, first_row);
        let rows = lineage.row.len();
        let whole = flat(&lineage.columns(&at).unwrap());
        let first = cut.index(rows + 1);
        let sliced = flat(&lineage.slice(first, rows - first).columns(&at).unwrap());
        for (sliced, whole) in sliced.iter().zip(&whole) {
            prop_assert_eq!(sliced.to_data(), whole.slice(first, rows - first).to_data());
        }
        let mask = BooleanArray::from(kept[..rows].to_vec());
        let predicate = FilterBuilder::new(&mask).build();
        let filtered = flat(&lineage.filter(&predicate).unwrap().columns(&at).unwrap());
        for (filtered, whole) in filtered.iter().zip(&whole) {
            let expected = arrow_select::filter::filter(whole.as_ref(), &mask).unwrap();
            prop_assert_eq!(filtered.to_data(), expected.to_data());
        }
    }
}
