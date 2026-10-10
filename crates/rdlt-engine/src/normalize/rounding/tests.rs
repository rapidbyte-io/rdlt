use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Int32Array, Int64Array, LargeListArray,
    LargeListViewArray, ListArray, ListViewArray, MapArray, RecordBatch, RunArray, StructArray,
    UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields};
use proptest::prelude::*;
use rdlt_connector::{SchemaError, TableSchema};

use super::{Rounding, judge};
use crate::normalize::encodings::batch;
use crate::normalize::{Shape, normalize};
use crate::table::Incoming;

const EDGE: i64 = 1 << 53;

/// Most tables and columns a test reads back.
const SHOWN: usize = 64;

fn shape(max_depth: u8) -> Shape {
    Shape {
        max_depth,
        whole: BTreeSet::new(),
        key: Vec::new(),
    }
}

/// The rounding columns of each table `columns` normalize into, as `shape` says.
fn judged(columns: Vec<(&str, ArrayRef)>, shape: &Shape) -> Vec<(Vec<String>, Vec<String>)> {
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    let mut rounding = Rounding::new();
    judge(std::slice::from_ref(&batch), shape, &mut rounding).unwrap();
    shown(&rounding)
}

fn shown(rounding: &Rounding) -> Vec<(Vec<String>, Vec<String>)> {
    rounding
        .iter()
        .take(SHOWN)
        .map(|(table, columns)| {
            let table = table.iter().take(SHOWN).map(ToString::to_string).collect();
            let columns = columns
                .iter()
                .take(SHOWN)
                .map(ToString::to_string)
                .collect();
            (table, columns)
        })
        .collect()
}

fn integers(values: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn table(path: &[&str], columns: &[&str]) -> (Vec<String>, Vec<String>) {
    let strings = |items: &[&str]| items.iter().map(ToString::to_string).collect();
    (strings(path), strings(columns))
}

fn object(name: &str, values: ArrayRef, nulls: Option<NullBuffer>) -> ArrayRef {
    let field = Field::new(name, values.data_type().clone(), true);
    Arc::new(StructArray::try_new(Fields::from(vec![field]), vec![values], nulls).unwrap())
}

fn list(values: ArrayRef, offsets: Vec<i32>, nulls: Option<NullBuffer>) -> ArrayRef {
    let item = Arc::new(Field::new("item", values.data_type().clone(), true));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    Arc::new(ListArray::try_new(item, offsets, values, nulls).unwrap())
}

#[test]
fn a_flush_s_integers_are_judged_by_the_path_of_their_table() {
    assert_eq!(
        judged(vec![("n", integers(vec![1 << 60, 1]))], &shape(2)),
        [table(&[], &["n"])]
    );
    assert!(judged(vec![("n", integers(vec![1, 2, 3]))], &shape(2)).is_empty());
}

#[test]
fn an_integer_in_a_child_table_rounds_that_table_s_column() {
    let items = list(
        object("m", integers(vec![1, EDGE + 1]), None),
        vec![0, 2],
        None,
    );
    assert_eq!(
        judged(vec![("items", items)], &shape(2)),
        [table(&["items"], &["m"])]
    );
    let values = list(integers(vec![EDGE + 1]), vec![0, 1], None);
    assert_eq!(
        judged(vec![("values", values)], &shape(2)),
        [table(&["values"], &["value"])]
    );
    let inner = list(integers(vec![EDGE + 1]), vec![0, 1], None);
    let grid = list(inner, vec![0, 1], None);
    assert_eq!(
        judged(vec![("grid", grid)], &shape(2)),
        [table(&["grid", "value"], &["value"])]
    );
}

#[test]
fn a_value_under_a_null_object_is_not_read() {
    let hidden = object(
        "a",
        integers(vec![1, EDGE + 1]),
        Some(NullBuffer::from(vec![true, false])),
    );
    assert!(judged(vec![("s", Arc::clone(&hidden))], &shape(2)).is_empty());
    // Under a null item of a list, a run-end encoded field is not read either.
    let runs: ArrayRef = Arc::new(
        RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 2]),
            &Int64Array::from(vec![1, EDGE + 1]),
        )
        .unwrap(),
    );
    let items = object("r", runs, Some(NullBuffer::from(vec![true, false])));
    let items = list(items, vec![0, 2], None);
    assert!(judged(vec![("items", items)], &shape(3)).is_empty());
    let shown = object("a", integers(vec![1, EDGE + 1]), None);
    assert_eq!(
        judged(vec![("s", shown)], &shape(2)),
        [table(&[], &["s.a"])]
    );
}

#[test]
fn a_null_list_s_items_are_not_read_in_any_layout() {
    let values = || integers(vec![1, EDGE + 1]);
    let nulls = || Some(NullBuffer::from(vec![true, false]));
    let item = || Arc::new(Field::new("item", DataType::Int64, true));
    let layouts: Vec<ArrayRef> = vec![
        list(values(), vec![0, 1, 2], nulls()),
        Arc::new(
            LargeListArray::try_new(
                item(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 1, 2])),
                values(),
                nulls(),
            )
            .unwrap(),
        ),
        Arc::new(FixedSizeListArray::try_new(item(), 1, values(), nulls()).unwrap()),
        Arc::new(
            ListViewArray::try_new(
                item(),
                ScalarBuffer::from(vec![0, 1]),
                ScalarBuffer::from(vec![1, 1]),
                values(),
                nulls(),
            )
            .unwrap(),
        ),
    ];
    for layout in layouts {
        let kind = layout.data_type().clone();
        assert!(judged(vec![("l", layout)], &shape(2)).is_empty(), "{kind}");
    }
}

#[test]
fn a_list_view_naming_its_items_out_of_order_reads_each_row_s_items() {
    let item = || Arc::new(Field::new("item", DataType::Int64, true));
    let view = |offsets: Vec<i32>| -> ArrayRef {
        let values = integers(vec![1, EDGE + 1, 5]);
        let sizes = ScalarBuffer::from(vec![1; offsets.len()]);
        Arc::new(
            ListViewArray::try_new(item(), ScalarBuffer::from(offsets), sizes, values, None)
                .unwrap(),
        )
    };
    // The rows name the third item, then the first: the second, which rounds, is named by none.
    assert!(judged(vec![("l", view(vec![2, 0]))], &shape(2)).is_empty());
    assert_eq!(
        judged(vec![("l", view(vec![2, 1, 0]))], &shape(2)),
        [table(&["l"], &["value"])]
    );
    assert_eq!(
        judged(vec![("l", view(vec![1, 1]))], &shape(2)),
        [table(&["l"], &["value"])]
    );
    // Runs are mapped in row order, so a view naming a run before an earlier one is read a row
    // at a time too.
    let runs: ArrayRef = Arc::new(
        RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 2, 3]),
            &Int64Array::from(vec![1, EDGE + 1, 5]),
        )
        .unwrap(),
    );
    let item = Arc::new(Field::new("item", runs.data_type().clone(), true));
    let (offsets, sizes) = (
        ScalarBuffer::from(vec![2, 1]),
        ScalarBuffer::from(vec![1, 1]),
    );
    let view: ArrayRef =
        Arc::new(ListViewArray::try_new(item, offsets, sizes, runs, None).unwrap());
    assert_eq!(
        judged(vec![("l", view)], &shape(2)),
        [table(&["l"], &["value"])]
    );
}

#[test]
fn a_list_view_of_list_views_out_of_order_is_read_in_time_linear_in_its_rows() {
    // Each outer row names one inner row, the last first: reading an outer row's items must not
    // read the inner view's other rows, or the walk takes the square of its rows.
    const ROWS: i32 = 200_000;
    let item = |data_type: &DataType| Arc::new(Field::new("item", data_type.clone(), true));
    let ones = || ScalarBuffer::from(vec![1; ROWS as usize]);
    let mut values = vec![1; ROWS as usize];
    values[0] = EDGE + 1;
    let firsts = ScalarBuffer::from_iter(0..ROWS);
    let inner = ListViewArray::try_new(
        item(&DataType::Int64),
        firsts,
        ones(),
        integers(values),
        None,
    );
    let inner: ArrayRef = Arc::new(inner.unwrap());
    let lasts = ScalarBuffer::from_iter((0..ROWS).rev());
    let outer = ListViewArray::try_new(item(inner.data_type()), lasts, ones(), inner, None);
    let outer: ArrayRef = Arc::new(outer.unwrap());
    assert_eq!(
        judged(vec![("l", outer)], &shape(2)),
        [table(&["l", "value"], &["value"])]
    );
}

#[test]
fn an_integer_in_an_array_of_any_layout_rounds_its_child_table_s_column() {
    let values = || integers(vec![1, EDGE + 1]);
    let item = || Arc::new(Field::new("item", DataType::Int64, true));
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![0, 1]));
    let pairs = Fields::from(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::Int64, true),
    ]);
    let entries = StructArray::try_new(pairs, vec![keys, values()], None).unwrap();
    let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map = MapArray::try_new(entry, OffsetBuffer::from_lengths([2]), entries, None, false);
    let layouts: Vec<ArrayRef> = vec![
        list(values(), vec![0, 2], None),
        Arc::new(
            LargeListArray::try_new(item(), OffsetBuffer::from_lengths([2]), values(), None)
                .unwrap(),
        ),
        Arc::new(FixedSizeListArray::try_new(item(), 2, values(), None).unwrap()),
        Arc::new(
            ListViewArray::try_new(
                item(),
                ScalarBuffer::from(vec![0]),
                ScalarBuffer::from(vec![2]),
                values(),
                None,
            )
            .unwrap(),
        ),
        Arc::new(
            LargeListViewArray::try_new(
                item(),
                ScalarBuffer::from(vec![0]),
                ScalarBuffer::from(vec![2]),
                values(),
                None,
            )
            .unwrap(),
        ),
        Arc::new(map.unwrap()),
    ];
    for layout in layouts {
        let kind = layout.data_type().clone();
        let flush = [RecordBatch::try_from_iter([("l", layout)]).unwrap()];
        let mut walked = Rounding::new();
        judge(&flush, &shape(2), &mut walked).unwrap();
        assert_eq!(shown(&walked), [table(&["l"], &["value"])], "{kind}");
        assert_eq!(
            shown(&walked),
            shown(&split_rounding(&flush, &shape(2))),
            "{kind}"
        );
    }
}

#[test]
fn a_container_one_level_past_the_depth_limit_is_one_column_wherever_it_nests() {
    let rounding = || object("n", integers(vec![EDGE + 1]), None);
    // Each innermost object is one level past `limit`: in an object, as a list's items, and in
    // a list's item objects.
    let nested = [
        (object("t", rounding(), None), 1, table(&[], &["s.t.n"])),
        (list(rounding(), vec![0, 1], None), 1, table(&["s"], &["n"])),
        (
            list(object("t", rounding(), None), vec![0, 1], None),
            2,
            table(&["s"], &["t.n"]),
        ),
        // A list of lists: each level of items is a grandchild table under `value`.
        (
            list(list(rounding(), vec![0, 1], None), vec![0, 1], None),
            2,
            table(&["s", "value"], &["n"]),
        ),
        (
            list(
                list(list(rounding(), vec![0, 1], None), vec![0, 1], None),
                vec![0, 1],
                None,
            ),
            3,
            table(&["s", "value", "value"], &["n"]),
        ),
        (
            list(
                list(object("t", rounding(), None), vec![0, 1], None),
                vec![0, 1],
                None,
            ),
            3,
            table(&["s", "value"], &["t.n"]),
        ),
    ];
    for (column, limit, flattened) in nested {
        let kind = column.data_type().clone();
        let flush = [RecordBatch::try_from_iter([("s", column)]).unwrap()];
        for (max_depth, expected) in [(limit, vec![]), (limit + 1, vec![flattened])] {
            let mut walked = Rounding::new();
            judge(&flush, &shape(max_depth), &mut walked).unwrap();
            assert_eq!(shown(&walked), expected, "{kind}, depth {max_depth}");
            let split = split_rounding(&flush, &shape(max_depth));
            assert_eq!(shown(&walked), shown(&split), "{kind}, depth {max_depth}");
        }
    }
}

#[test]
fn a_container_beyond_the_depth_limit_or_kept_whole_is_one_column() {
    let nested = || object("a", integers(vec![EDGE + 1]), None);
    // A column of objects is not one of integers, whatever it holds.
    assert!(judged(vec![("s", nested())], &shape(0)).is_empty());
    let whole = Shape {
        whole: BTreeSet::from([Arc::from("s"), Arc::from("n")]),
        ..shape(2)
    };
    assert!(judged(vec![("s", nested())], &whole).is_empty());
    assert_eq!(
        judged(vec![("n", integers(vec![EDGE + 1]))], &whole),
        [table(&[], &["n"])]
    );
}

#[test]
fn a_column_with_no_logical_type_is_refused_wherever_normalizing_puts_it() {
    let unions = |len: usize| -> ArrayRef {
        let fields =
            UnionFields::try_new(vec![0_i8], vec![Field::new("i", DataType::Int32, true)]).unwrap();
        let children: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1; len]))];
        let types = ScalarBuffer::from(vec![0_i8; len]);
        Arc::new(UnionArray::try_new(fields, types, None, children).unwrap())
    };
    let union = || unions(1);
    let items = list(object("u", union(), None), vec![0, 1], None);
    // Whatever rows name: arrays all empty, or a view's null rows naming items out of order.
    let empty = list(object("u", union(), None), vec![0, 0], None);
    let item = Arc::new(Field::new("item", union().data_type().clone(), true));
    let (offsets, sizes) = (
        ScalarBuffer::from(vec![0, 0]),
        ScalarBuffer::from(vec![1, 1]),
    );
    let nulls = Some(NullBuffer::from(vec![false, false]));
    let view = ListViewArray::try_new(Arc::clone(&item), offsets, sizes, union(), nulls).unwrap();
    // Or a view's rows naming items out of order, each read alone.
    let (offsets, sizes) = (
        ScalarBuffer::from(vec![2, 1]),
        ScalarBuffer::from(vec![1, 1]),
    );
    let unordered = ListViewArray::try_new(item, offsets, sizes, unions(3), None).unwrap();
    let columns = [
        union(),
        object("u", union(), None),
        items,
        empty,
        Arc::new(view),
        Arc::new(unordered),
    ];
    for column in columns {
        let batch = RecordBatch::try_from_iter([("c", column)]).unwrap();
        let refused = judge(
            std::slice::from_ref(&batch),
            &shape(2),
            &mut Rounding::new(),
        );
        assert!(
            matches!(refused, Err(SchemaError::Unsupported(_))),
            "{refused:?}"
        );
    }
}

/// The rounding columns of each table the parts of `batches`, concatenated and normalized as
/// `shape`, hold: the judgement's oracle, which splits and scans.
fn split_rounding(batches: &[RecordBatch], shape: &Shape) -> Rounding {
    let joined = arrow_select::concat::concat_batches(&batches[0].schema(), batches).unwrap();
    let mut rounding = Rounding::new();
    for part in normalize(&joined, shape).unwrap() {
        let schema = TableSchema::from_arrow(&part.batch.schema()).unwrap();
        let incoming = Incoming::of(
            schema,
            part.columns.clone(),
            std::slice::from_ref(&part.batch),
        );
        if !incoming.rounding.is_empty() {
            rounding
                .entry(part.path)
                .or_default()
                .extend(incoming.rounding);
        }
    }
    rounding
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(512)))]

    #[test]
    fn the_walk_judges_each_table_as_scanning_the_split_flush_does(
        drawn in crate::normalize::encodings::drawn(),
        max_depth in 0_u8..=4,
        keyed in proptest::collection::vec(any::<bool>(), 3),
        whole in proptest::collection::vec(any::<bool>(), 3),
        cut in 0_usize..7,
    ) {
        let names = |chosen: &[bool]| -> Vec<Arc<str>> {
            drawn.0.iter().zip(chosen).filter(|(_, chosen)| **chosen)
                .map(|((name, _), _)| Arc::from(name.as_str())).take(SHOWN).collect()
        };
        let shape = Shape {
            max_depth,
            whole: names(&whole).into_iter().collect(),
            key: names(&keyed),
        };
        let flush = batch(&drawn, Clone::clone);
        let cut = cut.min(flush.num_rows());
        let batches = [flush.slice(0, cut), flush.slice(cut, flush.num_rows() - cut)];
        let mut walked = Rounding::new();
        judge(&batches, &shape, &mut walked).expect("a drawn batch has a table schema");
        prop_assert_eq!(shown(&walked), shown(&split_rounding(&batches, &shape)));
    }
}
