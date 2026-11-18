use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Int32Array, Int64Array, RecordBatch, StringArray};
use bytes::Bytes;

use super::{Run, comparable, described, parted, runs};
use crate::sink::Push;
use crate::testing::limits::RENDERED_BYTES;

/// A push of `rows` rows, each naming one value of `bytes` bytes.
fn keyed(rows: usize, bytes: usize) -> Push {
    let values = StringArray::from(vec!["x".repeat(bytes)]);
    let keys = Int32Array::from(vec![0; rows]);
    let column: ArrayRef =
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());
    Push::Arrow(RecordBatch::try_from_iter([("value", column)]).unwrap())
}

#[test]
fn pushes_are_compared_only_while_what_they_expand_to_fits_what_a_comparison_renders() {
    let mebibyte = 1 << 20;
    let quarter = keyed(RENDERED_BYTES / 4 / mebibyte, mebibyte);
    assert!(comparable([&quarter, &quarter, &quarter]));
    assert!(!comparable([&quarter; 5]));
    // Four mebibytes of keys naming one value of 128 bytes: 128 MiB once each row holds its own.
    assert!(!comparable([&keyed(1 << 20, 128)]));
    let text = Push::Json(Bytes::from(vec![b' '; RENDERED_BYTES + 1]));
    assert!(
        comparable([&text]),
        "JSON is compared as the text it is held as"
    );
}

/// A batch of the ids `values`.
fn ids(values: &[i64]) -> RecordBatch {
    let column: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    RecordBatch::try_from_iter([("id", column)]).unwrap()
}

fn arrow(values: &[i64]) -> Push {
    Push::Arrow(ids(values))
}

fn json(text: &'static str) -> Push {
    Push::Json(Bytes::from_static(text.as_bytes()))
}

/// A batch of one dictionary column whose `keys` index `values`.
fn encoded(keys: &[i32], values: &[&str]) -> Push {
    let keys = Int32Array::from(keys.to_vec());
    let values = Arc::new(StringArray::from(values.to_vec()));
    let column: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
    Push::Arrow(RecordBatch::try_from_iter([("value", column)]).unwrap())
}

/// The row, counted from 0, at which `actual`'s rows first part from `expected`'s.
fn parts(expected: &[Push], actual: &[Push]) -> Option<usize> {
    parted(&runs(expected), &runs(actual)).map(|parted| parted.row)
}

#[test]
fn the_same_rows_compare_alike_whatever_pushes_carry_them() {
    let empty = arrow(&[]);
    let cases = [
        (
            vec![arrow(&[0, 1, 2]), arrow(&[3, 4, 5])],
            vec![arrow(&[0, 1]), empty.clone(), arrow(&[2, 3, 4, 5])],
        ),
        (vec![arrow(&[0, 1]), empty.clone()], vec![arrow(&[0, 1])]),
        (
            vec![json(r#"[{"page":0},{"page":1}]"#), json(r#"[{"page":2}]"#)],
            vec![json(r#"[{"page":0}]"#), json(r#"[{"page":1},{"page":2}]"#)],
        ),
        // The same values, encoded through dictionaries of their own.
        (
            vec![encoded(&[0, 1, 0], &["a", "b"])],
            vec![encoded(&[1], &["b", "a"]), encoded(&[0, 1], &["b", "a"])],
        ),
        (Vec::new(), vec![empty, json("[]")]),
    ];
    for (expected, actual) in cases {
        assert_eq!(parts(&expected, &actual), None, "{expected:?} {actual:?}");
        assert_eq!(parts(&actual, &expected), None, "{actual:?} {expected:?}");
    }
}

#[test]
fn rows_part_at_the_first_that_differs_or_that_one_read_lacks() {
    let cases = [
        (
            vec![arrow(&[0, 1, 2]), arrow(&[3, 4, 5])],
            vec![arrow(&[0, 1]), arrow(&[2, 3, 9, 5])],
            4,
        ),
        // A row parts within a run either read resumed part way through.
        (
            vec![arrow(&[0, 1, 2, 3])],
            vec![arrow(&[0]), arrow(&[1, 9, 3])],
            2,
        ),
        (
            vec![arrow(&[0]), arrow(&[1, 9, 3])],
            vec![arrow(&[0, 1, 2, 3])],
            2,
        ),
        (vec![arrow(&[0, 1, 2])], vec![arrow(&[0, 1])], 2),
        (vec![arrow(&[0])], vec![arrow(&[0]), arrow(&[1])], 1),
        // The same rows sent as changes are other rows.
        (vec![arrow(&[0, 1])], vec![Push::Changes(ids(&[0, 1]))], 0),
        (
            vec![json(r#"[{"page":0},{"page":1}]"#)],
            vec![json(r#"[{"page":0},{"page":2}]"#)],
            1,
        ),
        (vec![json(r#"[{"id":0}]"#)], vec![arrow(&[0])], 0),
    ];
    for (expected, actual, row) in cases {
        let parted = parts(&expected, &actual);
        assert_eq!(parted, Some(row), "{expected:?} {actual:?}");
    }
}

#[tokio::test]
async fn a_parted_row_is_described_as_each_read_sent_it() {
    let pushes = [arrow(&[0, 1, 2]), arrow(&[0, 1, 7])];
    let (expected, actual) = (runs(&pushes[..1]), runs(&pushes[1..]));
    let parted = parted(&expected, &actual).unwrap();
    assert!(described(parted.expected).await.contains("id=2"));
    assert!(described(parted.actual).await.contains("id=7"));
    assert_eq!(described(None).await, "nothing");
    let batch = ids(&[4]);
    let run = |changes| Run::Batch {
        changes,
        batch: &batch,
        at: 0,
    };
    assert_eq!(described(Some(run(false))).await, "{id=4}");
    assert_eq!(described(Some(run(true))).await, "the change {id=4}");
    let record = Run::Record(br#"{"page":3}"#);
    assert_eq!(described(Some(record)).await, r#"{"page":3}"#);
}
