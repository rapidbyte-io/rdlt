use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray};
use bytes::Bytes;

use super::comparable;
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
