use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{ColumnPath, TableSchema};

use super::{EXACT_IN_FLOAT, rounding};

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
        Arc::new(
            arrow_array::RunArray::<Int32Type>::try_new(&ends, &Int64Array::from(values)).unwrap(),
        ) as ArrayRef
    };
    assert!(rounded(&[batch(vec![("r", run_end(vec![1, 2]))])]).is_empty());
    assert_eq!(
        rounded(&[batch(vec![("r", run_end(vec![1, EDGE + 1]))])]),
        ["r"]
    );
}
