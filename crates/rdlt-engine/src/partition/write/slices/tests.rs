use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, RunArray, StringArray};
use arrow_schema::DataType;
use rdlt_connector::{Permit, decoded_bytes};

use super::super::Held;
use super::{MIN_SLICE, slice_bytes, sliced};
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

fn held(budget: &MemoryBudget, bytes: u64) -> Held {
    let permit: Permit = Box::new(budget.charge(bytes));
    Held {
        permits: vec![permit],
        bytes,
    }
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
    let budget = MemoryBudget::new(1 << 30);
    let parts = vec![encoded(0, 5_000), encoded(5_000, 3_000)];
    let unit = (parts, held(&budget, 64));
    let max = 200_000;
    let pieces = sliced(vec![unit], max);
    assert!(pieces.len() > 1);
    for (parts, _) in &pieces {
        let bytes: u64 = parts.iter().map(decoded_bytes).sum();
        assert!(bytes <= max, "{bytes}");
    }
    assert_eq!(ids(&pieces), (0..8_000).collect::<Vec<_>>());
    let (last, others) = pieces.split_last().unwrap();
    assert_eq!(last.1.permits.len(), 1);
    assert!(
        others
            .iter()
            .all(|(_, held)| held.permits.is_empty() && held.bytes == 0)
    );
    drop(pieces);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn a_unit_within_a_slice_stays_whole_and_a_row_larger_than_one_is_its_own_piece() {
    let budget = MemoryBudget::new(1 << 30);
    let small = vec![encoded(0, 10), encoded(10, 10)];
    let pieces = sliced(vec![(small, held(&budget, 64))], 1 << 20);
    assert_eq!(pieces.len(), 1);
    assert_eq!(pieces[0].0.len(), 2);
    let rows = sliced(vec![(vec![encoded(0, 3)], held(&budget, 64))], 10);
    assert_eq!(rows.len(), 3);
    assert_eq!(ids(&rows), [0, 1, 2]);
}

#[test]
fn slices_share_the_budget_among_a_window_of_lowerings() {
    assert_eq!(slice_bytes(&MemoryBudget::new(256 << 20)), 16 << 20);
    assert_eq!(slice_bytes(&MemoryBudget::new(1 << 10)), MIN_SLICE);
}

#[test]
fn pieces_fill_a_slice_to_its_last_byte() {
    // Two batches of ten 8-byte integers are one piece of exactly 160 bytes.
    let budget = MemoryBudget::new(1 << 30);
    let plain = |first: i64| {
        let ids: Vec<i64> = (first..first + 10).collect();
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as ArrayRef)]).unwrap()
    };
    let unit = (vec![plain(0), plain(10)], held(&budget, 8));
    let pieces = sliced(vec![unit], 160);
    assert_eq!(pieces.len(), 1);
    assert_eq!(ids(&pieces), (0..20).collect::<Vec<_>>());
    let empty = plain(0).slice(0, 0);
    let pieces = sliced(vec![(vec![empty], held(&budget, 8))], 160);
    assert_eq!(pieces.len(), 1);
    assert_eq!(pieces[0].0[0].num_rows(), 0);
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
    let budget = MemoryBudget::new(1 << 30);
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
