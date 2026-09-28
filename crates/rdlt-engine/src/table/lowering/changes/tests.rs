use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int8Array, Int64Array,
    RecordBatch,
};
use rdlt_connector::{ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN};

use super::{ChangeRows, compact_changes};

fn seq(position: u8) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[15] = position;
    bytes
}

/// A pushed change batch: `a`, the op column, `b`, the sequence, the unchanged flags, `c`, so
/// the change columns sit between the data's.
fn pushed(ops: &[ChangeOp], flags: Vec<Option<Vec<u8>>>) -> RecordBatch {
    let rows = i64::try_from(ops.len()).unwrap();
    let ints = |start: i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|row| start + row),
        ))
    };
    let seqs = (0..rows).map(|row| seq(u8::try_from(row + 1).unwrap()));
    let seqs = FixedSizeBinaryArray::try_from_iter(seqs).unwrap();
    RecordBatch::try_from_iter([
        ("a", ints(0)),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from_iter_values(ops.iter().map(|op| op.code()))) as ArrayRef,
        ),
        ("b", ints(10)),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (
            UNCHANGED_COLUMN,
            Arc::new(flags.into_iter().collect::<BinaryArray>()) as ArrayRef,
        ),
        ("c", ints(20)),
    ])
    .unwrap()
}

#[test]
fn a_split_keeps_the_data_and_moves_the_flags_to_its_ordinals() {
    // Pushed field 2 is `b`, data field 1; pushed field 5 is `c`, data field 2; pushed field 1
    // is the op column, which no data field is.
    let batch = pushed(
        &[ChangeOp::Update, ChangeOp::Insert],
        vec![Some(vec![0b0010_0110]), None],
    );
    let (data, changes) = ChangeRows::split(&batch).unwrap();
    let names: Vec<String> = data
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["a", "b", "c"]);
    let flags = changes.unchanged.as_ref().unwrap();
    assert_eq!(flags.value(0), [0b110]);
    assert!(flags.is_null(1));
    assert_eq!(changes.flagged(), [1, 2]);
    assert_eq!(changes.seq.value(1), seq(2));
    assert_eq!(changes.op(0), Some(ChangeOp::Update));
}

#[test]
fn flags_past_the_eighth_field_are_read_from_their_byte() {
    // Eleven data fields, then the change columns; the first row flags field 2, the second
    // fields 1 and 10.
    let mut columns: Vec<(String, ArrayRef)> = (0..11)
        .map(|field| {
            (
                format!("f{field}"),
                Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef,
            )
        })
        .collect();
    columns.push((OP_COLUMN.into(), Arc::new(Int8Array::from(vec![1, 1]))));
    let seqs = FixedSizeBinaryArray::try_from_iter([seq(1), seq(2)].into_iter()).unwrap();
    columns.push((SEQ_COLUMN.into(), Arc::new(seqs)));
    let flags: BinaryArray = [Some(vec![0b100]), Some(vec![0b10, 0b100])]
        .into_iter()
        .collect();
    columns.push((UNCHANGED_COLUMN.into(), Arc::new(flags)));
    let (_, changes) = ChangeRows::split(&RecordBatch::try_from_iter(columns).unwrap()).unwrap();
    assert_eq!(changes.flagged(), [1, 2, 10]);
    let flags = changes.unchanged.as_ref().unwrap();
    assert_eq!(flags.value(1), [0b10, 0b100]);
    // Written one field along, field 10 is written as field 11.
    let written: Vec<Option<usize>> = (1..12).map(Some).collect();
    let written = changes.unchanged_over(&written);
    assert_eq!(written.as_binary::<i32>().value(1), [0b100, 0b1000]);
    // Written in another order, field 1 as field 11 and field 10 as field 0.
    let mut reordered: Vec<Option<usize>> = vec![None; 11];
    reordered[1] = Some(11);
    reordered[10] = Some(0);
    let reordered = changes.unchanged_over(&reordered);
    assert_eq!(reordered.as_binary::<i32>().value(1), [0b1, 0b1000]);
}

#[test]
fn flags_move_to_the_fields_their_columns_are_written_as() {
    let batch = pushed(&[ChangeOp::Update], vec![Some(vec![0b0010_0100])]);
    let (_, changes) = ChangeRows::split(&batch).unwrap();
    // Data field 1 (`b`) is written as field 9, and `c` is not written.
    let written = changes.unchanged_over(&[Some(0), Some(9), None]);
    let written = written.as_binary::<i32>();
    assert_eq!(written.value(0), [0, 0b10]);
    let none = ChangeRows::split(&pushed(&[ChangeOp::Insert], vec![None]))
        .unwrap()
        .1;
    assert!(none.unchanged_over(&[Some(0)]).is_null(0));
}

#[test]
fn deletes_and_truncates_record_when_other_rows_do_not() {
    let ops = [
        ChangeOp::Insert,
        ChangeOp::Delete,
        ChangeOp::Truncate,
        ChangeOp::Update,
    ];
    let (_, changes) = ChangeRows::split(&pushed(&ops, vec![None; 4])).unwrap();
    let at = changes.deleted_at(7);
    let at = at.as_primitive::<arrow_array::types::TimestampMicrosecondType>();
    let values: Vec<Option<i64>> = at.iter().collect();
    assert_eq!(values, [None, Some(7), Some(7), None]);
    assert!(changes.truncates(2) && !changes.truncates(1));
    let kept = changes
        .filter(&BooleanArray::from(vec![false, true, false, true]))
        .unwrap();
    assert_eq!(kept.op.len(), 2);
    assert_eq!(kept.op(1), Some(ChangeOp::Update));
}

/// A merge batch: key `k`, op, sequence and unchanged flags, one row per `(key, op, seq, flags)`,
/// where the flags are one byte, or none.
fn merge_batch(rows: &[(i64, ChangeOp, u8, Option<u8>)]) -> RecordBatch {
    let flags: BinaryArray = rows
        .iter()
        .map(|row| row.3.map(|byte| vec![byte]))
        .collect();
    RecordBatch::try_from_iter([
        (
            "k",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
        ),
        (
            "op",
            Arc::new(Int8Array::from_iter_values(
                rows.iter().map(|row| row.1.code()),
            )) as ArrayRef,
        ),
        (
            "seq",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| seq(row.2)),
            )) as ArrayRef,
        ),
        ("unchanged", Arc::new(flags) as ArrayRef),
    ])
    .unwrap()
}

fn kept(batch: &RecordBatch, hard: bool) -> Vec<(i64, u8)> {
    let compacted = compact_changes(batch, &[0], [1, 2, 3], hard).unwrap();
    let keys = compacted.column(0).as_primitive::<Int64Type>();
    let seqs = compacted.column(2).as_binary::<i32>();
    (0..compacted.num_rows())
        .map(|row| (keys.value(row), seqs.value(row)[15]))
        .collect()
}

#[test]
fn a_later_whole_row_supersedes_the_rows_of_its_key_before_it() {
    use ChangeOp::{Delete, Insert, Update};
    let batch = merge_batch(&[
        (1, Insert, 1, None),
        (2, Insert, 2, None),
        (1, Update, 3, Some(1)),
        (1, Update, 4, None),
        (2, Delete, 5, None),
    ]);
    assert_eq!(kept(&batch, true), [(1, 4), (2, 5)]);
    // A soft delete keeps the row's last values, so the rows before it stay.
    assert_eq!(kept(&batch, false), [(2, 2), (1, 4), (2, 5)]);
}

#[test]
fn a_partial_row_supersedes_nothing_and_a_truncate_keeps_every_row() {
    use ChangeOp::{Insert, Truncate, Update};
    let partial = merge_batch(&[(1, Insert, 1, None), (1, Update, 2, Some(1))]);
    assert_eq!(kept(&partial, true), [(1, 1), (1, 2)]);
    let truncated = merge_batch(&[
        (1, Insert, 1, None),
        (0, Truncate, 2, None),
        (1, Update, 3, None),
    ]);
    assert_eq!(kept(&truncated, true), [(1, 1), (0, 2), (1, 3)]);
    let lone = merge_batch(&[(1, Insert, 1, None)]);
    assert_eq!(kept(&lone, true), [(1, 1)]);
}

#[test]
fn a_whole_row_supersedes_a_lone_row_before_it_even_with_empty_flags() {
    use ChangeOp::{Insert, Update};
    let pair = merge_batch(&[(1, Insert, 1, None), (1, Update, 2, None)]);
    assert_eq!(kept(&pair, true), [(1, 2)]);
    // Flags that flag nothing leave nothing unchanged.
    let flagless = merge_batch(&[(1, Insert, 1, Some(0)), (1, Update, 2, Some(0))]);
    assert_eq!(kept(&flagless, true), [(1, 2)]);
}
