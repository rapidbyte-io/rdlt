use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int64Array, PrimitiveArray, RecordBatch, RunArray,
    StringArray, UInt32Array,
};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;
use proptest::prelude::*;
use rdlt_connector::{ColumnPath, TableSchema};

use super::{EXACT_IN_FLOAT, rounding, rounds};

const EDGE: i64 = 1 << 53;

/// The columns of `batches`, all of the first's schema, that round.
fn rounded(batches: &[RecordBatch]) -> Vec<String> {
    let schema = TableSchema::from_arrow(&batches[0].schema()).unwrap();
    let paths: Vec<ColumnPath> = schema
        .fields()
        .iter()
        .map(|field| ColumnPath::from(field.name()))
        .collect();
    rounding(&schema, &paths, batches)
        .iter()
        .map(ToString::to_string)
        .collect()
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
fn integers_within_2_to_the_53_either_way_are_exact_and_any_beyond_rounds() {
    assert_eq!(EXACT_IN_FLOAT, 1 << 53);
    let exact = [0, EDGE, -EDGE, EDGE - 1, 7];
    for beyond in [EDGE + 1, -EDGE - 1, i64::MAX, i64::MIN] {
        let values: Vec<i64> = exact.iter().copied().chain([beyond]).collect();
        let columns = vec![
            (
                "exact",
                Arc::new(Int64Array::from(exact.to_vec())) as ArrayRef,
            ),
            (
                "beyond",
                Arc::new(Int64Array::from(values[1..].to_vec())) as ArrayRef,
            ),
        ];
        assert_eq!(rounded(&[batch(columns)]), ["beyond"], "{beyond}");
    }
}

#[test]
fn a_value_hidden_by_a_null_or_a_slice_is_not_read() {
    let nulled = Int64Array::new(vec![1, i64::MAX].into(), Some(vec![true, false].into()));
    let sliced = Int64Array::from(vec![i64::MAX, 1, 2]).slice(1, 2);
    let columns = vec![
        ("nulled", Arc::new(nulled) as ArrayRef),
        ("sliced", Arc::new(sliced) as ArrayRef),
    ];
    assert!(rounded(&[batch(columns)]).is_empty());
}

#[test]
fn any_batch_rounding_makes_the_column_round_and_other_types_never_do() {
    let exact = batch(vec![
        ("n", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (
            "s",
            Arc::new(StringArray::from(vec!["9007199254740993"])) as ArrayRef,
        ),
    ]);
    let beyond = batch(vec![
        ("n", Arc::new(Int64Array::from(vec![EDGE + 1])) as ArrayRef),
        ("s", Arc::new(StringArray::from(vec!["x"])) as ArrayRef),
    ]);
    assert!(rounded(std::slice::from_ref(&exact)).is_empty());
    assert_eq!(rounded(&[exact, beyond]), ["n"]);
}

#[test]
fn an_encoded_column_rounds_where_a_value_a_row_holds_does() {
    let dictionary = |values: Vec<i64>| {
        let keys = arrow_array::Int32Array::from(vec![0, 0]);
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(Int64Array::from(values)))
                .unwrap(),
        ) as ArrayRef
    };
    assert!(
        rounded(&[batch(vec![("d", dictionary(vec![1, EDGE + 1]))])]).is_empty(),
        "a value no row refers to is not read"
    );
    assert_eq!(
        rounded(&[batch(vec![("d", dictionary(vec![EDGE + 1, 1]))])]),
        ["d"]
    );
    let run_end = |values: Vec<i64>| {
        let ends = arrow_array::Int32Array::from(vec![1, 3]);
        Arc::new(RunArray::<Int32Type>::try_new(&ends, &Int64Array::from(values)).unwrap())
            as ArrayRef
    };
    assert!(rounded(&[batch(vec![("r", run_end(vec![1, 2]))])]).is_empty());
    assert_eq!(
        rounded(&[batch(vec![("r", run_end(vec![1, EDGE + 1]))])]),
        ["r"]
    );
}

/// A dictionary of `values` keyed by `K`, its rows naming `keys`, `None` a null key.
fn keyed<K: ArrowDictionaryKeyType>(keys: &[Option<usize>], values: ArrayRef) -> ArrayRef {
    let keys: PrimitiveArray<K> = keys
        .iter()
        .map(|key| key.map(|key| K::Native::from_usize(key).expect("a key fits its type")))
        .collect();
    Arc::new(DictionaryArray::<K>::try_new(keys, values).expect("every key names a value"))
}

/// `values` in runs ending at `ends`, of `E`.
fn runs<E: RunEndIndexType>(ends: &[usize], values: &dyn Array) -> ArrayRef {
    let ends: PrimitiveArray<E> = ends
        .iter()
        .map(|end| Some(E::Native::from_usize(*end).expect("an end fits its type")))
        .collect();
    Arc::new(RunArray::<E>::try_new(&ends, values).expect("valid runs"))
}

/// A dictionary builder, as `keyed` of one key type.
type Dictionary = fn(&[Option<usize>], ArrayRef) -> ArrayRef;

/// A run builder, as `runs` of one end type.
type Runs = fn(&[usize], &dyn Array) -> ArrayRef;

fn integers(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

#[test]
fn a_dictionary_of_every_key_type_reads_only_the_values_its_rows_name() {
    let values = || integers(&[1, EDGE + 1]);
    let cases: [(&str, Dictionary); 8] = [
        ("Int8", keyed::<Int8Type>),
        ("Int16", keyed::<Int16Type>),
        ("Int32", keyed::<Int32Type>),
        ("Int64", keyed::<Int64Type>),
        ("UInt8", keyed::<UInt8Type>),
        ("UInt16", keyed::<UInt16Type>),
        ("UInt32", keyed::<UInt32Type>),
        ("UInt64", keyed::<UInt64Type>),
    ];
    for (key, dictionary) in cases {
        assert!(
            !rounds(dictionary(&[Some(0), Some(0)], values()).as_ref()),
            "{key}"
        );
        assert!(
            rounds(dictionary(&[Some(0), Some(1)], values()).as_ref()),
            "{key}"
        );
        assert!(
            !rounds(dictionary(&[Some(0), None], values()).as_ref()),
            "{key}: a null key"
        );
    }
}

#[test]
fn runs_with_ends_of_every_width_read_the_values_their_rows_fall_in() {
    let values = integers(&[1, EDGE + 1]);
    let cases: [(&str, Runs); 3] = [
        ("Int16", runs::<Int16Type>),
        ("Int32", runs::<Int32Type>),
        ("Int64", runs::<Int64Type>),
    ];
    for (end, encoded) in cases {
        let column = encoded(&[2, 5], values.as_ref());
        assert!(rounds(column.as_ref()), "{end}");
        assert!(
            !rounds(column.slice(0, 2).as_ref()),
            "{end}: the first run alone"
        );
        assert!(
            rounds(column.slice(1, 2).as_ref()),
            "{end}: across the runs"
        );
    }
}

#[test]
fn a_sliced_dictionary_or_run_reads_only_the_rows_the_slice_keeps() {
    let dictionary = keyed::<Int32Type>(&[Some(1), Some(0), Some(0)], integers(&[1, EDGE + 1]));
    assert!(rounds(dictionary.as_ref()));
    assert!(!rounds(dictionary.slice(1, 2).as_ref()));
    let ran = runs::<Int32Type>(&[1, 3], integers(&[EDGE + 1, 1]).as_ref());
    assert!(!rounds(ran.slice(1, 2).as_ref()));
    assert!(rounds(ran.slice(0, 1).as_ref()));
}

#[test]
fn a_null_value_a_valid_key_names_does_not_round() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
    let hidden: ArrayRef = Arc::new(Int64Array::new(
        vec![1, i64::MAX].into(),
        Some(vec![true, false].into()),
    ));
    assert!(!rounds(
        keyed::<Int32Type>(&[Some(0), Some(1)], values).as_ref()
    ));
    assert!(!rounds(keyed::<Int32Type>(&[Some(1)], hidden).as_ref()));
}

#[test]
fn unsigned_32_bit_integers_never_round_however_they_are_encoded() {
    let unsigned: ArrayRef = Arc::new(UInt32Array::from(vec![u32::MAX, 0]));
    assert!(!rounds(unsigned.as_ref()));
    assert!(!rounds(
        keyed::<Int32Type>(&[Some(0)], Arc::clone(&unsigned)).as_ref()
    ));
    assert!(!rounds(
        runs::<Int32Type>(&[1, 2], unsigned.as_ref()).as_ref()
    ));
}

#[test]
fn nested_encodings_read_only_the_values_their_rows_name() {
    let beyond = integers(&[1, EDGE + 1]);
    // A dictionary of runs: the key names a run's row, which names its value.
    let ran = runs::<Int16Type>(&[1, 2], beyond.as_ref());
    assert!(!rounds(
        keyed::<Int8Type>(&[Some(0), Some(0)], Arc::clone(&ran)).as_ref()
    ));
    assert!(rounds(keyed::<Int8Type>(&[Some(1)], ran).as_ref()));
    // Runs of a dictionary: each run's row is a key.
    let dictionary = keyed::<UInt16Type>(&[Some(0), Some(1)], Arc::clone(&beyond));
    let ran = runs::<Int64Type>(&[3, 4], dictionary.as_ref());
    assert!(!rounds(ran.slice(0, 3).as_ref()));
    assert!(rounds(ran.as_ref()));
}

#[test]
fn an_empty_column_never_rounds() {
    assert!(!rounds(integers(&[]).as_ref()));
    assert!(!rounds(
        keyed::<Int32Type>(&[], integers(&[EDGE + 1])).as_ref()
    ));
}

/// What judging `array` allocates at its peak, beside what was allocated before.
fn judging_peak(array: &ArrayRef) -> (bool, u64) {
    let heap = &crate::cost::tests::HEAP;
    heap.reset_peak_usage();
    let before = heap.current_usage();
    let judged = rounds(array.as_ref());
    let peak = heap.peak_usage().saturating_sub(before);
    (judged, u64::try_from(peak).unwrap())
}

#[test]
fn judging_a_long_encoded_column_holds_nothing_a_row() {
    const ROWS: usize = 4 << 20;
    let exact = || integers(&[EDGE]);
    let keys = arrow_array::Int32Array::from(vec![0; ROWS]);
    let columns = [
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, exact()).unwrap()) as ArrayRef,
        runs::<Int32Type>(&[ROWS], exact().as_ref()),
        Arc::new(UInt32Array::from(vec![u32::MAX; ROWS])) as ArrayRef,
    ];
    for column in columns {
        let (judged, peak) = judging_peak(&column);
        assert!(!judged, "{}", column.data_type());
        assert!(
            peak < 8 << 10,
            "judging {} held {peak} bytes",
            column.data_type()
        );
    }
}

/// How a drawn column of 64-bit integers is encoded, one layer over the next.
#[derive(Clone, Copy, Debug)]
enum Layer {
    /// A dictionary keyed by the key type at this place in `keyed`'s table.
    Keys(u8),
    /// Runs of one row each, ends of the width at this place in `runs`' table.
    Runs(u8),
}

/// `plain` in `layers`, the first innermost; a dictionary over the plain values also holds a
/// value no row names, which rounds.
fn layered(plain: &Int64Array, layers: &[Layer]) -> ArrayRef {
    let mut array: ArrayRef = Arc::new(plain.clone());
    for (depth, layer) in layers.iter().enumerate() {
        let rows = array.len();
        array = match *layer {
            Layer::Keys(key) => {
                let values = if depth == 0 {
                    let mut values: Vec<Option<i64>> = plain.iter().collect();
                    values.push(Some(i64::MAX));
                    Arc::new(Int64Array::from(values)) as ArrayRef
                } else {
                    Arc::clone(&array)
                };
                let keys: Vec<Option<usize>> = (0..rows).map(Some).collect();
                match key % 4 {
                    0 => keyed::<Int8Type>(&keys, values),
                    1 => keyed::<UInt16Type>(&keys, values),
                    2 => keyed::<Int32Type>(&keys, values),
                    _ => keyed::<UInt64Type>(&keys, values),
                }
            }
            Layer::Runs(width) => {
                let ends: Vec<usize> = (1..=rows).collect();
                match width % 3 {
                    0 => runs::<Int16Type>(&ends, array.as_ref()),
                    1 => runs::<Int32Type>(&ends, array.as_ref()),
                    _ => runs::<Int64Type>(&ends, array.as_ref()),
                }
            }
        };
    }
    array
}

/// Whether `array` holds, decoded, a value a 64-bit float would round: the judgement's oracle.
fn decoded_rounds(array: &ArrayRef) -> bool {
    let decoded = crate::table::convert::decoded(array).expect("an encoded column decodes");
    let plain = arrow_cast::cast(&decoded, &DataType::Int64).expect("integers cast");
    plain
        .as_primitive::<Int64Type>()
        .iter()
        .flatten()
        .any(|value| value.unsigned_abs() > EXACT_IN_FLOAT)
}

fn value() -> impl Strategy<Value = Option<i64>> {
    prop_oneof![
        1 => Just(None),
        4 => (-EDGE..=EDGE).prop_map(Some),
        1 => any::<i64>().prop_map(Some),
    ]
}

fn layer() -> impl Strategy<Value = Layer> {
    prop_oneof![
        any::<u8>().prop_map(Layer::Keys),
        any::<u8>().prop_map(Layer::Runs)
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(512)))]

    #[test]
    fn the_judgement_equals_a_scan_of_the_values_the_rows_hold_decoded(
        values in proptest::collection::vec(value(), 1..100),
        layers in proptest::collection::vec(layer(), 0..3),
        cut in (0_usize..100, 0_usize..100),
    ) {
        let plain = Int64Array::from(values);
        let column = layered(&plain, &layers);
        let offset = cut.0 % column.len();
        let column = column.slice(offset, cut.1 % (column.len() - offset + 1));
        prop_assert_eq!(rounds(column.as_ref()), decoded_rounds(&column), "{:?}", layers);
    }
}
