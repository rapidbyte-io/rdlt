//! Every layout nested in every other, over leaves of each kind, with offsets that are not
//! zero at each level: what a sender must send as its rows whatever part of it is cut.

use std::sync::Arc;

use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Decimal256Array, DictionaryArray,
    FixedSizeBinaryArray, FixedSizeListArray, Int32Array, LargeBinaryArray, LargeListArray,
    LargeListViewArray, LargeStringArray, ListArray, ListViewArray, MapArray, NullArray,
    PrimitiveArray, RunArray, StringArray, StringViewArray, StructArray, UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer, i256};
use arrow_schema::{DataType, Field, Fields, UnionFields};

/// A column and what it nests, outermost first.
pub(crate) type Named = (String, ArrayRef);

fn item(data_type: &DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type.clone(), true))
}

fn narrow(count: usize) -> i32 {
    i32::try_from(count).unwrap()
}

/// `count` integers, every fourth null.
fn ints(count: usize) -> ArrayRef {
    let values = (0..narrow(count)).map(|at| (at % 4 != 3).then_some(at));
    Arc::new(Int32Array::from_iter(values))
}

/// `count` values of three bytes, every fourth null.
fn fixed_binary(count: usize) -> ArrayRef {
    let values = (0..narrow(count)).map(|at| (at % 4 != 3).then(|| at.to_le_bytes()));
    let values = values.map(|value| value.map(|bytes| [bytes[0], bytes[1], 7]));
    Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(values, 3).unwrap())
}

/// `count` decimals of 256 bits, every fourth null.
fn decimals(count: usize) -> ArrayRef {
    let values = (0..narrow(count)).map(|at| (at % 4 != 3).then(|| i256::from(at)));
    let values = Decimal256Array::from_iter(values);
    Arc::new(values.with_precision_and_scale(40, 2).unwrap())
}

/// Nine texts: long enough to leave a view, null, and short.
fn texts() -> Vec<Option<String>> {
    let text = |at: usize| match at % 4 {
        0 => Some(format!(
            "a string long enough to leave its view, number {at}"
        )),
        1 => None,
        2 => Some("tiny".to_owned()),
        _ => Some(format!("another string long enough to leave a view {at}")),
    };
    (0..9).map(text).collect()
}

/// Five hundred texts as views, every fourth null: their bytes take far more than a frame's
/// overhead, so a part of them sent with all of them is seen by its bytes.
fn long_views() -> ArrayRef {
    let text =
        |at: usize| (at % 4 != 1).then(|| format!("a string long enough to leave its view, {at}"));
    Arc::new(StringViewArray::from_iter((0..500).map(text)))
}

/// A leaf of each kind, some sliced from longer ones.
fn leaves() -> Vec<Named> {
    let texts = texts();
    let flags = (0..9).map(|at| (at % 3 != 1).then_some(at % 2 == 0));
    let bytes = texts.iter().map(|text| text.as_ref().map(String::as_bytes));
    let leaves: Vec<(&str, ArrayRef)> = vec![
        ("null", Arc::new(NullArray::new(9))),
        ("bool", Arc::new(BooleanArray::from_iter(flags))),
        ("int", ints(9)),
        ("int sliced", ints(12).slice(2, 9)),
        ("utf8", Arc::new(StringArray::from(texts.clone()))),
        (
            "utf8 sliced",
            Arc::new(StringArray::from(texts.clone()).slice(2, 6)),
        ),
        ("binary", Arc::new(BinaryArray::from_iter(bytes.clone()))),
        (
            "large utf8 sliced",
            Arc::new(LargeStringArray::from(texts.clone()).slice(1, 7)),
        ),
        (
            "large binary",
            Arc::new(LargeBinaryArray::from_iter(bytes.clone())),
        ),
        ("fixed binary sliced", fixed_binary(12).slice(3, 9)),
        ("decimal sliced", decimals(12).slice(1, 9)),
        ("view", Arc::new(StringViewArray::from(texts.clone()))),
        (
            "binview",
            Arc::new(BinaryViewArray::from(bytes.collect::<Vec<_>>())),
        ),
        ("view sliced", long_views().slice(250, 6)),
    ];
    let named = leaves.into_iter();
    named.map(|(name, leaf)| (name.to_owned(), leaf)).collect()
}

/// A validity mask of `rows` rows, null where a row is one beyond a multiple of `modulus`.
fn masked(rows: usize, modulus: usize) -> NullBuffer {
    let valid = (0..rows).map(|row| row % modulus != 1);
    NullBuffer::from(valid.collect::<Vec<_>>())
}

/// Lists and maps of `c`: their first offset is not zero, and null lists span items.
fn lists(c: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let n = narrow(c.len());
    let offsets = vec![1, 1, 2, 3, 3, n];
    let nulls = Some(NullBuffer::from(vec![true, false, true, true, false]));
    let wide: Vec<i64> = offsets.iter().map(|offset| i64::from(*offset)).collect();
    let field = item(c.data_type());
    let list = ListArray::new(
        Arc::clone(&field),
        OffsetBuffer::new(offsets.clone().into()),
        Arc::clone(c),
        nulls.clone(),
    );
    let large = LargeListArray::new(
        field,
        OffsetBuffer::new(wide.into()),
        Arc::clone(c),
        nulls.clone(),
    );
    let keys: Vec<String> = (0..c.len()).map(|at| format!("k{at}")).collect();
    let keys: ArrayRef = Arc::new(StringArray::from(keys));
    let fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", c.data_type().clone(), true),
    ]);
    let entries = StructArray::new(fields, vec![keys, Arc::clone(c)], None);
    let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map = MapArray::new(
        entry,
        OffsetBuffer::new(offsets.into()),
        entries,
        nulls,
        false,
    );
    vec![
        ("list", Arc::new(list)),
        ("large list", Arc::new(large)),
        ("map", Arc::new(map)),
    ]
}

/// List views of `c`: out of order, overlapping, null with a size, and naming nothing.
fn list_views(c: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let n = narrow(c.len());
    let (offsets, sizes) = (vec![n - 1, 0, 1, 0, 2, 0], vec![1, n, 2, 0, 1, 0]);
    let nulls = Some(NullBuffer::from(vec![true, true, false, true, true, true]));
    let wide =
        |values: &[i32]| -> ScalarBuffer<i64> { values.iter().map(|v| i64::from(*v)).collect() };
    let field = item(c.data_type());
    let large = LargeListViewArray::new(
        Arc::clone(&field),
        wide(&offsets),
        wide(&sizes),
        Arc::clone(c),
        nulls.clone(),
    );
    let views = ListViewArray::new(
        Arc::clone(&field),
        offsets.into(),
        sizes.into(),
        Arc::clone(c),
        nulls,
    );
    let nothing = ListViewArray::new(
        field,
        vec![0, 1, 2].into(),
        vec![0, 0, 0].into(),
        Arc::clone(c),
        None,
    );
    vec![
        ("list view", Arc::new(views)),
        ("large list view", Arc::new(large)),
        ("list view of nothing", Arc::new(nothing)),
    ]
}

/// A fixed-size list, a struct and unions of `c`.
fn fixed_and_unions(c: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let n = c.len();
    let pairs = n / 2;
    let fixed = FixedSizeListArray::new(
        item(c.data_type()),
        2,
        c.slice(n - 2 * pairs, 2 * pairs),
        Some(masked(pairs, 3)),
    );
    let fields = Fields::from(vec![
        Field::new("c", c.data_type().clone(), true),
        Field::new("n", DataType::Int32, true),
    ]);
    let parent = StructArray::new(fields, vec![Arc::clone(c), ints(n)], Some(masked(n, 3)));
    let fields = UnionFields::try_new(
        vec![5, 7],
        vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", c.data_type().clone(), true),
        ],
    )
    .unwrap();
    let last = narrow(n) - 1;
    let dense = |ids: Vec<i8>, offsets: Vec<i32>| -> ArrayRef {
        let children = vec![ints(4), Arc::clone(c)];
        let union = UnionArray::try_new(fields.clone(), ids.into(), Some(offsets.into()), children);
        Arc::new(union.unwrap())
    };
    let ids: ScalarBuffer<i8> = (0..n).map(|row| if row % 3 == 0 { 5 } else { 7 }).collect();
    let sparse = UnionArray::try_new(fields.clone(), ids, None, vec![ints(n), Arc::clone(c)]);
    vec![
        ("fixed list", Arc::new(fixed)),
        ("struct", Arc::new(parent)),
        (
            "dense",
            dense(vec![7, 5, 7, 7, 5, 5, 7], vec![last, 3, 0, last, 0, 3, 1]),
        ),
        ("dense unnamed", dense(vec![5, 5, 5], vec![2, 0, 1])),
        ("sparse", Arc::new(sparse.unwrap())),
    ]
}

/// Runs of two, one, three and one rows over `c`, their ends of type `R`.
fn run_ends<R: RunEndIndexType>(c: &ArrayRef) -> ArrayRef
where
    R::Native: TryFrom<i64>,
{
    let lengths = [2, 1, 3, 1];
    let mut end = 0_i64;
    let ends = (0..c.len()).map(|at| {
        end += lengths[at % 4];
        R::Native::try_from(end).ok().unwrap()
    });
    let ends = PrimitiveArray::<R>::from_iter_values(ends);
    Arc::new(RunArray::<R>::try_new(&ends, c).unwrap())
}

/// A dictionary of `values` whose keys of type `K` repeat, skip and are null.
fn keyed<K: ArrowDictionaryKeyType>(values: &ArrayRef) -> ArrayRef
where
    K::Native: TryFrom<usize>,
{
    let n = values.len();
    let keys = [
        Some(n - 1),
        None,
        Some(0),
        Some(n - 1),
        Some(1 % n),
        Some(1 % n),
    ];
    let keys = keys
        .iter()
        .map(|key| key.map(|key| K::Native::try_from(key).ok().unwrap()));
    let keys: PrimitiveArray<K> = keys.collect();
    Arc::new(DictionaryArray::<K>::try_new(keys, Arc::clone(values)).unwrap())
}

/// Run-end columns and dictionaries of `c`: every type of end, and with `every_key` every
/// type of key; a dictionary's values are also a part of `c`.
fn runs_and_dictionaries(c: &ArrayRef, every_key: bool) -> Vec<(&'static str, ArrayRef)> {
    let ends = run_ends::<Int32Type>(c);
    let mut out = vec![
        ("ree16", run_ends::<Int16Type>(c)),
        ("ree32 sliced", ends.slice(1, ends.len() - 3)),
        ("ree32", ends),
        ("ree64", run_ends::<Int64Type>(c)),
    ];
    if matches!(c.data_type(), DataType::Dictionary(..)) {
        return out;
    }
    out.push(("dict i8", keyed::<Int8Type>(c)));
    out.push((
        "dict of a part",
        keyed::<Int8Type>(&c.slice(1, c.len() - 1)),
    ));
    if every_key {
        out.push(("dict i16", keyed::<Int16Type>(c)));
        out.push(("dict i32", keyed::<Int32Type>(c)));
        out.push(("dict i64", keyed::<Int64Type>(c)));
        out.push(("dict u8", keyed::<UInt8Type>(c)));
        out.push(("dict u16", keyed::<UInt16Type>(c)));
        out.push(("dict u32", keyed::<UInt32Type>(c)));
        out.push(("dict u64", keyed::<UInt64Type>(c)));
    }
    out
}

/// Every layout over `c`, which has at least three rows.
fn wrappers(c: &ArrayRef, every_key: bool) -> Vec<(&'static str, ArrayRef)> {
    let mut out = lists(c);
    out.extend(list_views(c));
    out.extend(fixed_and_unions(c));
    out.extend(runs_and_dictionaries(c, every_key));
    out
}

/// Every layout over the leaf named `leaf`, and every layout over each of those.
pub(crate) fn over(leaf: &str) -> Vec<Named> {
    let mut columns = Vec::new();
    let leaves = leaves().into_iter();
    for (leaf_name, leaf) in leaves.filter(|(name, _)| name == leaf) {
        for (first_name, first) in wrappers(&leaf, true) {
            let name = format!("{first_name}<{leaf_name}>");
            if first.len() >= 3 {
                for (second_name, second) in wrappers(&first, false) {
                    columns.push((format!("{second_name}<{name}>"), second));
                }
            }
            columns.push((name, first));
        }
    }
    assert!(!columns.is_empty(), "no leaf is named {leaf}");
    columns
}

/// Every layout over every leaf, and every layout over each of those.
pub(crate) fn columns() -> Vec<Named> {
    let leaves = leaves().into_iter();
    leaves.flat_map(|(name, _)| over(&name)).collect()
}
