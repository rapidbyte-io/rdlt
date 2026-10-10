use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, RunArray, StringArray};
use arrow_schema::DataType;
use rdlt_connector::Permit;
use rdlt_connector::cost::Rendering;

use super::super::Held;
use super::{Lowered, Pieces, RowTooLarge};
use crate::budget::MemoryBudget;

/// A batch of `rows` rows numbered from `first`, beside a run of one 1 KB value.
fn encoded(first: i64, rows: i32) -> RecordBatch {
    let runs = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![rows]),
        &StringArray::from(vec!["x".repeat(1_000)]),
    )
    .unwrap();
    let ids: Vec<i64> = (first..first + i64::from(rows)).collect();
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
        ("run", Arc::new(runs) as ArrayRef),
    ])
    .unwrap()
}

/// What holds a unit of no batches of its own under one permit of `bytes`.
fn held(budget: &MemoryBudget, bytes: u64) -> Held {
    let permit: Permit = Box::new(budget.try_acquire_working(bytes).unwrap());
    Held::of(vec![permit], &[])
}

/// A destination storing every value as it is.
fn native() -> Rendering {
    Rendering::new(rdlt_testkit::drawn::KINDS)
}

/// How batches are measured before their table is known: pieces of `max` bytes, of rows of
/// `limit` at most.
fn unplanned(max: u64, limit: u64) -> Lowered {
    Lowered {
        rendering: native(),
        stored: Vec::new(),
        row: 0,
        item: 0,
        max,
        limit,
    }
}

/// `units` cut into pieces of at most `max` bytes, under a budget no row exceeds.
fn sliced(units: Vec<(Vec<RecordBatch>, Held)>, max: u64) -> Vec<(Vec<RecordBatch>, Held)> {
    let cut = |(parts, held)| super::sliced(parts, held, unplanned(max, u64::MAX)).unwrap();
    units.into_iter().flat_map(cut).collect()
}

/// What `batch` expands to.
fn expanded(batch: &RecordBatch) -> u64 {
    native().expanded(batch, 0..batch.num_rows(), u64::MAX)
}

fn ids(pieces: &[(Vec<RecordBatch>, Held)]) -> Vec<i64> {
    pieces
        .iter()
        .flat_map(|(parts, _)| parts)
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn a_unit_larger_than_a_slice_is_cut_in_order_and_holds_its_permits_to_the_last_piece() {
    let budget = crate::budget::budget(1 << 30);
    let parts = vec![encoded(0, 5_000), encoded(5_000, 3_000)];
    let unit = (parts, held(&budget, 64));
    let max = 200_000;
    let pieces = sliced(vec![unit], max);
    assert!(pieces.len() > 1);
    for (parts, _) in &pieces {
        let bytes: u64 = parts.iter().map(expanded).sum();
        assert!(bytes <= max, "{bytes}");
    }
    assert_eq!(ids(&pieces), (0..8_000).collect::<Vec<_>>());
    let (last, others) = pieces.split_last().unwrap();
    assert_eq!(last.1.permits.len(), 1);
    assert!(others.iter().all(|(_, held)| held.permits.is_empty()));
    assert_eq!(budget.reserved(), 64);
    drop(pieces);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn a_unit_within_a_slice_stays_whole_and_a_row_larger_than_one_is_its_own_piece() {
    let budget = crate::budget::budget(1 << 30);
    let small = vec![encoded(0, 10), encoded(10, 10)];
    let pieces = sliced(vec![(small, held(&budget, 64))], 1 << 20);
    assert_eq!(pieces.len(), 1);
    assert_eq!(pieces[0].0.len(), 2);
    let rows = sliced(vec![(vec![encoded(0, 3)], held(&budget, 64))], 10);
    assert_eq!(rows.len(), 3);
    assert_eq!(ids(&rows), [0, 1, 2]);
}

#[test]
fn pieces_fill_a_slice_to_its_last_byte() {
    // Two batches of ten 8-byte integers are one piece of exactly 160 bytes.
    let budget = crate::budget::budget(1 << 30);
    let plain = |first: i64| {
        let ids: Vec<i64> = (first..first + 10).collect();
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as ArrayRef)]).unwrap()
    };
    let unit = (vec![plain(0), plain(10)], held(&budget, 8));
    let pieces = sliced(vec![unit], 160);
    assert_eq!(pieces.len(), 1);
    assert_eq!(ids(&pieces), (0..20).collect::<Vec<_>>());
    let empty = plain(0).slice(0, 0);
    drop(pieces);
    // A unit of no rows has no piece, and what held it is released.
    let pieces = sliced(vec![(vec![empty], held(&budget, 8))], 160);
    assert!(pieces.is_empty());
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn a_skewed_run_is_cut_by_each_row_s_own_value() {
    // Two thousand rows over a 4 KB value, then a thousand one-row runs over one byte each.
    let ends = Int32Array::from_iter_values((0..=1_000).map(|run| 2_000 + run));
    let values = StringArray::from_iter_values(
        std::iter::once("x".repeat(4_096)).chain((0..1_000).map(|_| "y".to_owned())),
    );
    let runs = RunArray::<Int32Type>::try_new(&ends, &values).unwrap();
    let numbers: Vec<i64> = (0..3_000).collect();
    let skewed = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(numbers)) as ArrayRef),
        ("run", Arc::new(runs) as ArrayRef),
    ])
    .unwrap();
    let budget = crate::budget::budget(1 << 30);
    let max = 64 << 10;
    let pieces = sliced(vec![(vec![skewed], held(&budget, 64))], max);
    assert_eq!(ids(&pieces), (0..3_000).collect::<Vec<_>>());
    for (parts, _) in &pieces {
        let decoded: u64 = parts
            .iter()
            .map(|part| {
                let text = arrow_cast::cast(part.column(1), &DataType::Utf8).unwrap();
                let bytes = text.to_data().get_slice_memory_size().unwrap();
                u64::try_from(bytes + 8 * part.num_rows()).unwrap()
            })
            .sum();
        assert!(decoded <= max, "{decoded}");
    }
}

#[test]
fn rows_fill_each_slice_to_its_last_byte() {
    // Twenty 8-byte integers over a slice of 80 bytes are two slices of ten.
    let budget = crate::budget::budget(1 << 30);
    let numbers: Vec<i64> = (0..20).collect();
    let plain =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(numbers)) as ArrayRef)])
            .unwrap();
    let pieces = sliced(vec![(vec![plain], held(&budget, 8))], 80);
    let rows: Vec<usize> = pieces
        .iter()
        .map(|(parts, _)| parts.iter().map(RecordBatch::num_rows).sum())
        .collect();
    assert_eq!(rows, [10, 10]);
}

#[test]
fn a_row_expanding_beyond_what_a_row_may_take_is_refused_and_one_within_it_is_its_own_piece() {
    let budget = crate::budget::budget(1 << 30);
    // Three rows of a 1 KB value, in slices of 100 bytes.
    let cut = |limit: u64| {
        super::sliced(
            vec![encoded(0, 3)],
            held(&budget, 64),
            unplanned(100, limit),
        )
    };
    assert_eq!(cut(2_000).unwrap().len(), 3);
    let Err(refused) = cut(500) else {
        panic!("a row beyond the limit was cut");
    };
    assert_eq!(refused.limit, 500);
    assert!(refused.expanded > 500, "{refused:?}");
    // A row of exactly the limit is within it.
    let row = native().expanded(&encoded(0, 1), 0..1, u64::MAX);
    assert_eq!(cut(row).unwrap().len(), 3);
    assert_eq!(
        cut(row - 1).err(),
        Some(RowTooLarge {
            expanded: row,
            limit: row - 1
        })
    );
}

/// A million small integers in one column.
fn small() -> RecordBatch {
    let column: ArrayRef = Arc::new(arrow_array::Int8Array::from(vec![1_i8; 1_000_000]));
    RecordBatch::try_from_iter([("n", column)]).unwrap()
}

/// The rows and bytes of each piece of `batch`, cut as `lowered` measures it.
fn pieces(batch: RecordBatch, lowered: Lowered) -> Vec<(usize, u64)> {
    let mut pieces = Pieces::new(vec![batch], lowered);
    let mut cut = Vec::new();
    while let Some(piece) = pieces.next().unwrap() {
        cut.push((piece.rows, piece.bytes));
    }
    cut
}

#[test]
fn a_unit_is_cut_by_what_lowering_it_into_its_table_holds() {
    use rdlt_connector::cost::Stored;
    use rdlt_connector::{DecimalType, LogicalType};
    const MAX: u64 = 1 << 20;
    // As it arrives, a million bytes are one piece.
    assert_eq!(
        pieces(small(), unplanned(MAX, u64::MAX)),
        [(1_000_000, 1_000_000)]
    );
    // In a column of 256-bit decimals each is 33 bytes while it is lowered: the byte it is and
    // the 32 it becomes.
    let wide = Stored {
        column: LogicalType::Decimal(DecimalType::new(76, 0).unwrap()),
        text: false,
        read: false,
    };
    let stored = Lowered {
        stored: vec![Some(wide.clone())],
        ..unplanned(MAX, u64::MAX)
    };
    let cut = pieces(small(), stored);
    assert_eq!(cut.len(), 32, "{cut:?}");
    assert!(
        cut.iter()
            .all(|(rows, bytes)| *bytes == 33 * *rows as u64 && *bytes <= MAX)
    );
    assert_eq!(cut.iter().map(|(rows, _)| rows).sum::<usize>(), 1_000_000);
    // Stored as text there, each takes its text beside, and each row the columns of the table
    // the unit holds nothing in.
    let text = Lowered {
        stored: vec![Some(Stored { text: true, ..wide })],
        row: 100,
        ..unplanned(MAX, u64::MAX)
    };
    let cut = pieces(small(), text);
    assert!(cut.len() > 200, "{} pieces", cut.len());
    assert!(
        cut.iter()
            .all(|(rows, bytes)| *bytes >= 215 * *rows as u64 && *bytes <= MAX)
    );
}

#[test]
fn a_piece_of_a_unit_shares_the_allocations_the_unit_holds() {
    let budget = crate::budget::budget(1 << 30);
    let parts = vec![encoded(0, 100)];
    let unit = Held::of(held(&budget, 64).permits, &parts);
    let pieces = sliced(vec![(parts, unit)], 2_000);
    assert!(pieces.len() > 1);
    let (last, others) = pieces.split_last().unwrap();
    for (_, held) in others {
        assert!(Arc::ptr_eq(&held.allocations, &last.1.allocations));
    }
}

#[test]
fn each_piece_takes_what_its_rows_were_measured_to_and_items_cost_what_they_are_told() {
    let row = native().expanded(&encoded(0, 1), 0..1, u64::MAX);
    assert_eq!(
        pieces(encoded(0, 3), unplanned(100, u64::MAX)),
        [(1, row), (1, row), (1, row)]
    );
    // A list's items take what each is said to take beside itself, where they become rows.
    let lists = arrow_array::ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..10).map(|_| Some(vec![Some(1), Some(2), Some(3)])),
    );
    let lists = RecordBatch::try_from_iter([("list", Arc::new(lists) as ArrayRef)]).unwrap();
    let measured = |item: u64| {
        let lowered = Lowered {
            item,
            ..unplanned(u64::MAX, u64::MAX)
        };
        pieces(lists.clone(), lowered)
    };
    let (plain, itemized) = (measured(0), measured(100));
    assert_eq!(plain.len(), 1);
    assert_eq!(itemized[0].1 - plain[0].1, 30 * 100);
}

#[test]
fn a_piece_its_rows_fill_to_the_byte_takes_what_its_stretches_were_measured_to() {
    // Two strings, each measured alone, fill a piece exactly, or with a byte to spare: the piece
    // takes the two measures, though the two measured together take less, an offset each.
    let column: ArrayRef = Arc::new(StringArray::from(vec!["abc"; 4]));
    let strings = RecordBatch::try_from_iter([("s", column)]).unwrap();
    let row = native().expanded(&strings, 0..1, u64::MAX);
    assert!(native().expanded(&strings, 0..2, u64::MAX) < 2 * row);
    for max in [2 * row, 2 * row + 1] {
        let lowered = unplanned(max, u64::MAX);
        assert_eq!(
            pieces(strings.clone(), lowered),
            [(2, 2 * row), (2, 2 * row)],
            "{max}"
        );
    }
}
