use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch,
};

use super::{ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN, validate_change_batch};
use crate::error::ConnectorErrorKind;

fn seq(n: usize) -> ArrayRef {
    let values: Vec<[u8; 16]> = (0..n).map(|i| (i as u128).to_be_bytes()).collect();
    Arc::new(FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap())
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
fn op_codes_round_trip() {
    for op in [ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete] {
        assert_eq!(ChangeOp::from_code(op.code()), Some(op));
    }
    assert_eq!(ChangeOp::from_code(3), None);
}

#[test]
fn a_well_formed_change_batch_is_accepted() {
    let valid = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2]))),
        (OP_COLUMN, Arc::new(Int8Array::from(vec![0, 2]))),
        (SEQ_COLUMN, seq(2)),
        (
            UNCHANGED_COLUMN,
            Arc::new(BinaryArray::from(vec![None, Some(&b"\x01"[..])])),
        ),
    ]);
    validate_change_batch(&valid).unwrap();
}

#[test]
fn malformed_change_batches_are_data_errors() {
    let id: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let cases = [
        batch(vec![("id", id.clone()), (SEQ_COLUMN, seq(1))]),
        batch(vec![
            (OP_COLUMN, Arc::new(Int64Array::from(vec![0]))),
            (SEQ_COLUMN, seq(1)),
        ]),
        batch(vec![
            (OP_COLUMN, Arc::new(Int8Array::from(vec![None]))),
            (SEQ_COLUMN, seq(1)),
        ]),
        batch(vec![
            (OP_COLUMN, Arc::new(Int8Array::from(vec![7]))),
            (SEQ_COLUMN, seq(1)),
        ]),
        batch(vec![
            (OP_COLUMN, Arc::new(Int8Array::from(vec![0]))),
            ("id", id.clone()),
        ]),
        batch(vec![
            (OP_COLUMN, Arc::new(Int8Array::from(vec![0]))),
            (SEQ_COLUMN, id.clone()),
        ]),
        batch(vec![
            (OP_COLUMN, Arc::new(Int8Array::from(vec![0]))),
            (
                SEQ_COLUMN,
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        vec![None::<[u8; 16]>].into_iter(),
                        16,
                    )
                    .unwrap(),
                ),
            ),
        ]),
        batch(vec![
            (OP_COLUMN, Arc::new(Int8Array::from(vec![0]))),
            (SEQ_COLUMN, seq(1)),
            (UNCHANGED_COLUMN, id),
        ]),
    ];
    for (index, case) in cases.iter().enumerate() {
        let error = validate_change_batch(case).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "case {index}");
        assert_eq!(error.code(), Some("change_batch"), "case {index}");
    }
}
