use arrow_array::cast::AsArray;
use arrow_array::{Array, BooleanArray};
use serde_json::json;

use super::{Dropped, Pruned};
use crate::normalize::Part;
use crate::normalize::tests::{parts, shape};

/// Three orders: the first with items a (tags x and y) and b, the second with item c (a note and
/// tag z), the third with none.
fn orders() -> (Part, Part, Part) {
    let mut parts = parts(
        &[
            json!({"id": 1, "items": [{"sku": "a", "tags": ["x", "y"]}, {"sku": "b"}]}),
            json!({"id": 2, "items": [{"sku": "c", "note": "n", "tags": ["z"]}]}),
            json!({"id": 3, "items": []}),
        ],
        &shape(8),
    )
    .into_iter();
    let mut next = || {
        parts
            .next()
            .expect("the orders, their items and the items' tags")
    };
    (next(), next(), next())
}

/// The text of `part`'s column `name`, a row at a time.
fn texts(part: &Part, name: &str) -> Vec<Option<String>> {
    let index = part
        .columns
        .iter()
        .position(|path| path.to_string() == name)
        .expect("the part has the column");
    let column = part.batch.column(index).as_string::<i32>();
    column
        .iter()
        .take(128)
        .map(|text| text.map(str::to_owned))
        .collect()
}

fn some(texts: &[&str]) -> Vec<Option<String>> {
    texts.iter().map(|text| Some((*text).to_owned())).collect()
}

/// Whether every array of `part`'s lineage holds a value for each of its rows.
fn lined_up(part: &Part) -> bool {
    let rows = part.batch.num_rows();
    let lineage = &part.lineage;
    let parent = lineage.parent.as_ref().map_or_else(Vec::new, |parent| {
        vec![&parent.id, &parent.root, &parent.idx, &parent.row]
    });
    [&lineage.id, &lineage.root_row]
        .into_iter()
        .chain(parent)
        .all(|array| array.len() == rows)
}

#[test]
fn a_dropped_row_takes_its_descendants_at_every_depth() {
    let (root, items, tags) = orders();
    let mut dropped = Dropped::default();
    // The second order goes, and with it item c and tag z.
    dropped.unkept(
        &Pruned::whole(root),
        &BooleanArray::from(vec![true, false, true]),
    );
    let items = dropped.prune(items).unwrap();
    assert_eq!(
        (items.count, texts(&items.part, "sku")),
        (1, some(&["a", "b"]))
    );
    assert!(lined_up(&items.part));
    let tags = dropped.prune(tags).unwrap();
    assert_eq!(
        (tags.count, texts(&tags.part, "value")),
        (1, some(&["x", "y"]))
    );
    assert!(lined_up(&tags.part));
}

#[test]
fn rows_a_policy_drops_from_a_pruned_part_are_known_by_their_place_as_normalized() {
    for (item_kept, count, rows) in [(true, 2, 1), (false, 3, 0)] {
        let (root, items, tags) = orders();
        let mut dropped = Dropped::default();
        // The first order goes with items a and b; c, the third item as normalized, is first left.
        dropped.unkept(
            &Pruned::whole(root),
            &BooleanArray::from(vec![false, true, true]),
        );
        let items = dropped.prune(items).unwrap();
        assert_eq!((items.count, texts(&items.part, "sku")), (2, some(&["c"])));
        dropped.unkept(&items, &BooleanArray::from(vec![item_kept]));
        let tags = dropped.prune(tags).unwrap();
        assert_eq!((tags.count, tags.part.batch.num_rows()), (count, rows));
        if item_kept {
            assert_eq!(texts(&tags.part, "value"), some(&["z"]));
        }
    }
}

#[test]
fn rows_holding_an_array_dropped_for_its_parents_go_with_their_descendants() {
    let (_, items, tags) = orders();
    let mut dropped = Dropped::default();
    // Items a and c hold tags.
    dropped.parents_of(&tags);
    let items = dropped.prune(items).unwrap();
    assert_eq!((items.count, texts(&items.part, "sku")), (2, some(&["b"])));
    let tags = dropped.prune(tags).unwrap();
    assert_eq!((tags.count, tags.part.batch.num_rows()), (3, 0));
}

#[test]
fn a_part_none_of_whose_rows_or_parents_was_dropped_is_whole() {
    let (root, items, tags) = orders();
    let mut dropped = Dropped::default();
    assert!(dropped.is_empty());
    // The third order holds no items: dropping it drops nothing below.
    dropped.unkept(
        &Pruned::whole(root),
        &BooleanArray::from(vec![true, true, false]),
    );
    assert!(!dropped.is_empty());
    for part in [items, tags] {
        let rows = part.batch.num_rows();
        let pruned = dropped.prune(part).unwrap();
        assert_eq!((pruned.count, pruned.part.batch.num_rows()), (0, rows));
    }
}

#[test]
fn a_column_only_dropped_rows_held_goes_with_them() {
    let (root, items, _) = orders();
    let noted = |part: &Part| part.columns.iter().any(|path| path.to_string() == "note");
    assert!(noted(&items));
    let mut dropped = Dropped::default();
    dropped.unkept(
        &Pruned::whole(root),
        &BooleanArray::from(vec![true, false, true]),
    );
    let items = dropped.prune(items).unwrap();
    assert!(!noted(&items.part), "only item c held a note");
}

#[test]
fn rows_dropped_on_either_side_of_a_words_bits_take_their_children() {
    let records: Vec<_> = (0..100)
        .map(|id| json!({"id": id, "items": [{"sku": format!("s{id}")}]}))
        .take(100)
        .collect();
    let mut parts = parts(&records, &shape(8)).into_iter();
    let (root, items) = (parts.next().unwrap(), parts.next().unwrap());
    let gone = [0_usize, 63, 64, 99];
    let kept: BooleanArray = (0..100)
        .map(|row| Some(!gone.contains(&row)))
        .take(100)
        .collect();
    let mut dropped = Dropped::default();
    dropped.unkept(&Pruned::whole(root), &kept);
    let items = dropped.prune(items).unwrap();
    assert_eq!((items.count, items.part.batch.num_rows()), (4, 96));
    let skus = texts(&items.part, "sku");
    for sku in ["s0", "s63", "s64", "s99"] {
        assert!(
            !skus.contains(&Some(sku.to_owned())),
            "{sku} went with its order"
        );
    }
    assert!(lined_up(&items.part));
}

#[test]
fn a_row_one_past_the_last_bit_was_not_dropped() {
    let mut bits = super::Bits::default();
    assert!(!bits.get(0));
    bits.set(7);
    assert!(bits.get(7));
    assert!(!bits.get(6));
    assert!(!bits.get(8));
}
