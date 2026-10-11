use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int8Array, Int64Array,
    RecordBatch,
};
use proptest::prelude::Strategy;
use rdlt_connector::{ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN};

use super::ChangeRows;

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
fn a_split_keeps_the_data_and_reads_the_flags_through_its_ordinals() {
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
    assert_eq!(changes.flagged(), [1, 2]);
    let over = changes.unchanged_over(&[Some(0), Some(1), Some(2)]);
    let over = over.as_binary::<i32>();
    assert_eq!(over.value(0), [0b110]);
    assert!(over.is_null(1));
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
fn flags_on_change_columns_and_past_the_fields_are_dropped() {
    // Pushed ordinals 1, 3 and 4 are the op, sequence and flags; 6 and 7 are past the six
    // fields, and 15 is in a second byte; only 0 (`a`) is a data field's.
    let flags = vec![
        Some(vec![0b1101_1011, 0b1000_0000, 0]),
        Some(vec![0b1101_1010]),
        Some(vec![]),
    ];
    let batch = pushed(&[ChangeOp::Update; 3], flags);
    let (_, changes) = ChangeRows::split(&batch).unwrap();
    assert_eq!(changes.flagged(), [0]);
    let written = changes.unchanged_over(&[Some(4), Some(5), Some(6)]);
    let written = written.as_binary::<i32>();
    assert_eq!(written.value(0), [0b1_0000]);
    // A row with flags, none of them kept, is an empty value, not a null.
    assert!(written.is_valid(1) && written.value(1).is_empty());
    assert!(written.is_valid(2) && written.value(2).is_empty());
}

proptest::proptest! {
    #[test]
    fn one_remap_writes_what_remapping_through_the_data_does(
        rows in proptest::collection::vec(
            proptest::option::of(proptest::collection::vec(proptest::prelude::any::<u8>(), 0..6)),
            0..8,
        ),
        (data, change) in (1..12_usize).prop_flat_map(|data| {
            // The op, sequence and flag columns' places among `data + 3` fields.
            let places: Vec<usize> = (0..data + 3).collect();
            (proptest::strategy::Just(data), proptest::sample::subsequence(places, 3))
        }),
        written in proptest::collection::vec(proptest::option::of(0..40_usize), 12),
    ) {
        let fields = data + 3;
        let ops = Int8Array::from(vec![ChangeOp::Update.code(); rows.len()]);
        let seqs = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            (0..rows.len()).map(|row| Some(seq(u8::try_from(row).unwrap()))),
            16,
        )
        .unwrap();
        let flags: BinaryArray = rows.iter().cloned().collect();
        let mut named = 0;
        let columns: Vec<(String, ArrayRef)> = (0..fields)
            .map(|field| match change.iter().position(|at| *at == field) {
                Some(0) => (OP_COLUMN.to_owned(), Arc::new(ops.clone()) as ArrayRef),
                Some(1) => (SEQ_COLUMN.to_owned(), Arc::new(seqs.clone()) as ArrayRef),
                Some(_) => (UNCHANGED_COLUMN.to_owned(), Arc::new(flags.clone()) as ArrayRef),
                None => {
                    named += 1;
                    let values = Int64Array::from(vec![0; rows.len()]);
                    (format!("d{named}"), Arc::new(values) as ArrayRef)
                }
            })
            .collect();
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let (_, changes) = ChangeRows::split(&batch).unwrap();
        let written = &written[..data];
        let to_data = super::data_ordinals(&batch);
        let twice = rdlt_connector::remap_unchanged(
            &rdlt_connector::remap_unchanged(&flags, &to_data),
            written,
        );
        let once = changes.unchanged_over(written);
        proptest::prop_assert_eq!(once.to_data(), twice.to_data());
    }
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

/// A merge batch's key column and its change rows: one row per `(key, op, seq, flags)`, the
/// flags one byte over two data fields, `k` (bit 0) and `v` (bit 1), or none.
fn merge_rows(rows: &[(i64, ChangeOp, u8, Option<u8>)]) -> (ArrayRef, ChangeRows) {
    let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0)));
    let changes = ChangeRows {
        op: Int8Array::from_iter_values(rows.iter().map(|row| row.1.code())),
        seq: BinaryArray::from_iter_values(rows.iter().map(|row| seq(row.2))),
        unchanged: Some(
            rows.iter()
                .map(|row| row.3.map(|byte| vec![byte]))
                .collect(),
        ),
        ordinals: Arc::from([Some(0), Some(1)]),
    };
    (keys, changes)
}

/// The keys and last sequence bytes of the rows `unsuperseded` keeps, where data field `i` is
/// written as `written[i]`.
fn kept_with(
    rows: &[(i64, ChangeOp, u8, Option<u8>)],
    written: &[Option<usize>],
    hard: bool,
) -> Vec<(i64, u8)> {
    let (keys, changes) = merge_rows(rows);
    let keep = changes
        .unsuperseded(&[Arc::clone(&keys)], written, hard)
        .unwrap();
    let keys = keys.as_primitive::<Int64Type>();
    (0..keys.len())
        .filter(|row| keep.as_ref().is_none_or(|keep| keep.value(*row)))
        .map(|row| (keys.value(row), changes.seq.value(row)[15]))
        .collect()
}

fn kept(rows: &[(i64, ChangeOp, u8, Option<u8>)], hard: bool) -> Vec<(i64, u8)> {
    kept_with(rows, &[Some(0), Some(1)], hard)
}

#[test]
fn a_later_whole_row_supersedes_the_rows_of_its_key_before_it() {
    use ChangeOp::{Delete, Insert, Update};
    let rows = [
        (1, Insert, 1, None),
        (2, Insert, 2, None),
        (1, Update, 3, Some(1)),
        (1, Update, 4, None),
        (2, Delete, 5, None),
    ];
    assert_eq!(kept(&rows, true), [(1, 4), (2, 5)]);
    // A soft delete keeps the row's last values, so the rows before it stay.
    assert_eq!(kept(&rows, false), [(2, 2), (1, 4), (2, 5)]);
}

#[test]
fn a_partial_row_supersedes_nothing_and_a_truncate_keeps_every_row() {
    use ChangeOp::{Insert, Truncate, Update};
    let partial = [(1, Insert, 1, None), (1, Update, 2, Some(1))];
    assert_eq!(kept(&partial, true), [(1, 1), (1, 2)]);
    let truncated = [
        (1, Insert, 1, None),
        (0, Truncate, 2, None),
        (1, Update, 3, None),
    ];
    assert_eq!(kept(&truncated, true), [(1, 1), (0, 2), (1, 3)]);
    let lone = [(1, Insert, 1, None)];
    assert_eq!(kept(&lone, true), [(1, 1)]);
}

#[test]
fn a_whole_row_supersedes_a_lone_row_before_it_even_with_empty_flags() {
    use ChangeOp::{Insert, Update};
    let pair = [(1, Insert, 1, None), (1, Update, 2, None)];
    assert_eq!(kept(&pair, true), [(1, 2)]);
    // Flags that flag nothing leave nothing unchanged.
    let flagless = [(1, Insert, 1, Some(0)), (1, Update, 2, Some(0))];
    assert_eq!(kept(&flagless, true), [(1, 2)]);
}

#[test]
fn a_row_flagging_only_fields_the_table_does_not_write_supersedes() {
    use ChangeOp::{Insert, Update};
    let rows = [(1, Insert, 1, None), (1, Update, 2, Some(0b10))];
    // `v` is written, and the update leaves it unchanged: both rows stay.
    assert_eq!(
        kept_with(&rows, &[Some(0), Some(1)], true),
        [(1, 1), (1, 2)]
    );
    // `v` is not written, so the update flags nothing the table holds and supersedes.
    assert_eq!(kept_with(&rows, &[Some(0), None], true), [(1, 2)]);
}
