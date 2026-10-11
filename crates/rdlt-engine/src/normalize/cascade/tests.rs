use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::Field;
use serde_json::json;

use super::{Dropped, Pruned};
use crate::normalize::tests::{parts, shape, shredded};
use crate::normalize::{Part, PushKind, normalize};

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
    let mut dropped = Dropped::new(PushKind::Json);
    // The second order goes, and with it item c and tag z.
    dropped.unkept(
        &Pruned::whole(root, PushKind::Json),
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
        let mut dropped = Dropped::new(PushKind::Json);
        // The first order goes with items a and b; c, the third item as normalized, is first left.
        dropped.unkept(
            &Pruned::whole(root, PushKind::Json),
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
    let mut dropped = Dropped::new(PushKind::Json);
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
    let mut dropped = Dropped::new(PushKind::Json);
    assert!(dropped.is_empty());
    // The third order holds no items: dropping it drops nothing below.
    dropped.unkept(
        &Pruned::whole(root, PushKind::Json),
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
    let mut dropped = Dropped::new(PushKind::Json);
    dropped.unkept(
        &Pruned::whole(root, PushKind::Json),
        &BooleanArray::from(vec![true, false, true]),
    );
    let items = dropped.prune(items).unwrap();
    assert!(!noted(&items.part), "only item c held a note");
}

/// The paths of `part`'s columns, which are its batch's.
fn columns(part: &Part) -> Vec<String> {
    assert_eq!(
        part.batch.num_columns(),
        part.columns.len(),
        "a part's batch holds a column for each path"
    );
    part.columns.iter().map(ToString::to_string).collect()
}

/// The parts of an Arrow batch of `ids`' rows, each holding one item of `skus` and `notes`.
fn noted(ids: Vec<i64>, skus: Vec<&str>, notes: Vec<Option<&str>>) -> Vec<Part> {
    let rows = ids.len();
    let items = StructArray::try_from(vec![
        ("sku", Arc::new(StringArray::from(skus)) as ArrayRef),
        ("note", Arc::new(StringArray::from(notes)) as ArrayRef),
    ])
    .expect("a struct of skus and notes");
    let item = Arc::new(Field::new("item", items.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths(vec![1; rows]);
    let list = ListArray::new(item, offsets, Arc::new(items), None);
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
        ("items", Arc::new(list) as ArrayRef),
    ])
    .expect("a batch of orders");
    normalize(&batch, &shape(8)).expect("the batch normalizes")
}

#[test]
fn a_child_column_no_kept_row_holds_a_value_in_makes_no_column_however_rows_are_grouped() {
    // Order 2 goes, and with it the only note; order 3's item holds a null note.
    let mut together = noted(vec![2, 3], vec!["a", "b"], vec![Some("n"), None]).into_iter();
    let (root, items) = (together.next().unwrap(), together.next().unwrap());
    let mut dropped = Dropped::new(PushKind::Arrow);
    dropped.unkept(
        &Pruned::whole(root, PushKind::Arrow),
        &BooleanArray::from(vec![false, true]),
    );
    let pruned = dropped.prune(items).unwrap();
    let alone = noted(vec![3], vec!["b"], vec![None]);
    let whole = Pruned::whole(alone[1].clone(), PushKind::Arrow);
    assert_eq!(columns(&pruned.part), ["sku"]);
    assert_eq!(columns(&whole.part), ["sku"]);
}

/// The parts of an Arrow batch of `ids`' rows, each holding `texts` and `tags`, an array.
fn tagged(ids: Vec<i64>, texts: Vec<Option<&str>>, tags: &[Option<&str>]) -> Vec<Part> {
    let mut list = ListBuilder::new(StringBuilder::new());
    for tag in tags.iter().take(16) {
        match tag {
            Some(tag) => {
                list.values().append_value(tag);
                list.append(true);
            }
            None => list.append(false),
        }
    }
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
        ("text", Arc::new(StringArray::from(texts)) as ArrayRef),
        ("tags", Arc::new(list.finish()) as ArrayRef),
    ])
    .expect("a batch of tagged rows");
    normalize(&batch, &shape(8)).expect("the batch normalizes")
}

#[test]
fn dropping_rows_never_removes_a_column_of_the_streams_own_table() {
    // Row 2 holds the only text and the tags whose array its rows go for.
    let mut together = tagged(vec![2, 3], vec![Some("t"), None], &[Some("x"), None]).into_iter();
    let (root, tags) = (together.next().unwrap(), together.next().unwrap());
    let mut dropped = Dropped::new(PushKind::Arrow);
    dropped.parents_of(&tags);
    let pruned = dropped.prune(root).unwrap();
    assert_eq!(pruned.part.batch.num_rows(), 1);
    let alone = tagged(vec![3], vec![None], &[None]);
    let whole = Pruned::whole(alone[0].clone(), PushKind::Arrow);
    assert_eq!(columns(&pruned.part), ["id", "text"]);
    assert_eq!(columns(&whole.part), ["id", "text"]);
}

#[test]
fn a_column_of_the_streams_own_table_no_kept_json_row_holds_a_value_in_makes_no_column() {
    // Row 2 holds the only `a` and the tags whose array its rows go for.
    let mut together = parts(
        &[json!({"id": 2, "a": "v", "tags": ["t"]}), json!({"id": 3})],
        &shape(8),
    )
    .into_iter();
    let (root, tags) = (together.next().unwrap(), together.next().unwrap());
    let mut dropped = Dropped::new(PushKind::Json);
    dropped.parents_of(&tags);
    let pruned = dropped.prune(root).unwrap();
    // Shredded in chunks of a record each, row 3's chunk holds `a`, a column of text, all null.
    let cut = shredded(&[json!({"id": 2, "a": "v"}), json!({"id": 3})], 1);
    let alone = normalize(&cut[1], &shape(8)).expect("the batch normalizes");
    assert_eq!(columns(&alone[0]), ["a", "id"]);
    let whole = Pruned::whole(alone[0].clone(), PushKind::Json);
    assert_eq!(columns(&whole.part), ["id"]);
    assert_eq!(columns(&pruned.part), ["id"]);
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
    let mut dropped = Dropped::new(PushKind::Json);
    dropped.unkept(&Pruned::whole(root, PushKind::Json), &kept);
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
