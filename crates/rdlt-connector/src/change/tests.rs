use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch,
};
use proptest::prelude::*;

use super::{
    ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN, UnchangedFlags, remap_unchanged,
    validate_change_batch,
};
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
    for op in [
        ChangeOp::Insert,
        ChangeOp::Update,
        ChangeOp::Delete,
        ChangeOp::Truncate,
    ] {
        assert_eq!(ChangeOp::from_code(op.code()), Some(op));
    }
    assert_eq!(ChangeOp::from_code(4), None);
    assert_eq!(ChangeOp::from_code(-1), None);
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

#[test]
fn only_the_bytes_holding_the_fields_are_listed() {
    // Eleven fields take two bytes: of the second, only the three low bits name fields, and
    // every byte after it is set.
    let bitmap = [0b0000_0001, 0b1111_1010, 0xFF, 0xFF];
    let listed: Vec<usize> = UnchangedFlags::new(&bitmap)
        .ordinals_below(11)
        .take(64)
        .collect();
    assert_eq!(listed, [0, 9]);
    assert_eq!(UnchangedFlags::new(&[0xFF]).ordinals_below(0).count(), 0);
}

#[test]
fn a_flag_past_the_fields_after_a_zero_tail_is_not_listed() {
    let mut bitmap = vec![0_u8; 4096];
    bitmap[0] = 0b10;
    bitmap[4095] = 0x80;
    let listed: Vec<usize> = UnchangedFlags::new(&bitmap)
        .ordinals_below(16)
        .take(64)
        .collect();
    assert_eq!(listed, [1]);
}

/// A bitmap's byte, zero as often as not, so set bytes sit between zero ones.
fn flag_byte() -> impl Strategy<Value = u8> {
    prop_oneof![Just(0_u8), any::<u8>()]
}

proptest! {
    #[test]
    fn a_bitmap_lists_exactly_the_ordinals_it_contains(
        bitmap in proptest::collection::vec(flag_byte(), 0..8),
    ) {
        let flags = UnchangedFlags::new(&bitmap);
        let listed: Vec<usize> = flags.ordinals().collect();
        let contained: Vec<usize> = (0..bitmap.len() * 8 + 16)
            .filter(|ordinal| flags.contains(*ordinal))
            .collect();
        prop_assert_eq!(listed, contained);
    }

    #[test]
    fn a_bitmap_lists_exactly_the_ordinals_it_contains_below_a_count_of_fields(
        bitmap in proptest::collection::vec(flag_byte(), 0..8),
        fields in 0_usize..80,
    ) {
        let flags = UnchangedFlags::new(&bitmap);
        let listed: Vec<usize> = flags.ordinals_below(fields).take(80).collect();
        let contained: Vec<usize> = (0..fields)
            .filter(|ordinal| flags.contains(*ordinal))
            .collect();
        prop_assert_eq!(listed, contained);
    }

    #[test]
    fn a_remap_flags_the_target_of_each_flagged_ordinal_and_nothing_else(
        bitmaps in proptest::collection::vec(
            proptest::option::of(proptest::collection::vec(flag_byte(), 0..8)),
            0..8,
        ),
        to in proptest::collection::vec(proptest::option::of(0_usize..40), 0..40),
    ) {
        let flags = BinaryArray::from_iter(bitmaps.iter().map(|bitmap| bitmap.as_deref()));
        let remapped = remap_unchanged(&flags, &to);
        prop_assert_eq!(remapped.len(), bitmaps.len());
        for (row, bitmap) in bitmaps.iter().enumerate() {
            let Some(bitmap) = bitmap else {
                prop_assert!(remapped.is_null(row));
                continue;
            };
            let expected: BTreeSet<usize> = UnchangedFlags::new(bitmap)
                .ordinals()
                .filter_map(|ordinal| to.get(ordinal).copied().flatten())
                .collect();
            prop_assert!(remapped.is_valid(row), "a row with flags stays a row, empty or not");
            let found: BTreeSet<usize> = UnchangedFlags::new(remapped.value(row)).ordinals().collect();
            prop_assert_eq!(&found, &expected);
            let length = expected.last().map_or(0, |last| last / 8 + 1);
            prop_assert_eq!(remapped.value(row).len(), length);
        }
    }
}
