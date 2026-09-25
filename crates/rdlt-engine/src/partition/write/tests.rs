use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{Partition, Permit, StreamName};

use super::{LOWERING_WINDOW, charge_growth, hold, share_growth, shred_failed, windows};
use crate::budget::MemoryBudget;
use crate::error::ErrorKind;
use crate::partition::PartitionJob;
use crate::shred::ShredError;
use crate::table::Prepared;

fn job() -> PartitionJob {
    PartitionJob {
        index: 0,
        stream: StreamName::new("events").unwrap(),
        table: 0,
        partition: Partition::single(),
        cursor: None,
        on_demand: false,
    }
}

#[test]
fn a_refused_push_is_a_source_error_and_a_shredder_bug_an_internal_one() {
    let refused = shred_failed(&job(), &ShredError::NotObject);
    assert_eq!(
        (refused.kind(), refused.code()),
        (ErrorKind::Source, Some("json_not_object"))
    );
    assert_eq!(refused.stream(), Some(&job().stream));
    assert!(
        refused.to_string().contains("a JSON push cannot be loaded"),
        "{refused}"
    );
    let bug = shred_failed(&job(), &ShredError::Internal("a bug".to_owned()));
    assert_eq!(
        (bug.kind(), bug.code()),
        (ErrorKind::Internal, Some("shred_internal"))
    );
}

#[tokio::test]
async fn shredded_batches_are_charged_before_the_pushes_they_came_from_are_released() {
    let budget = MemoryBudget::new(1 << 20);
    let pushed: Permit = Box::new(budget.acquire(400).await);
    let batches: Vec<RecordBatch> = [10, 1000]
        .into_iter()
        .map(|rows| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
            RecordBatch::try_from_iter([("id", ids)]).unwrap()
        })
        .collect();
    let sizes: Vec<u64> = batches
        .iter()
        .map(|batch| u64::try_from(batch.get_array_memory_size()).unwrap())
        .collect();
    let held = hold(&budget, &batches, vec![pushed]);
    assert_eq!(budget.peak(), 400 + sizes[0] + sizes[1]);
    assert_eq!(budget.reserved(), sizes[0] + sizes[1]);
    assert_eq!(
        held.iter().map(|held| held.bytes).collect::<Vec<_>>(),
        sizes
    );
    drop(held);
    assert_eq!(budget.reserved(), 0);
}

fn ids(rows: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    RecordBatch::try_from_iter([("id", ids)]).unwrap()
}

#[test]
fn lowered_batches_are_charged_in_full_before_any_is_queued() {
    let budget = MemoryBudget::new(1 << 20);
    let shredded = [ids(10), ids(1000)];
    let held = hold(&budget, &shredded, Vec::new());
    let lowered = [ids(100), ids(10)].map(|batch| Prepared {
        batch,
        discarded_rows: 0,
        discarded_values: 0,
        kept: None,
    });
    let sizes = [
        u64::try_from(lowered[0].batch.get_array_memory_size()).unwrap(),
        u64::try_from(shredded[1].get_array_memory_size()).unwrap(),
    ];
    let charged: Vec<_> = lowered
        .iter()
        .zip(held)
        .map(|(prepared, held)| charge_growth(&budget, prepared, held))
        .collect();
    assert_eq!(budget.reserved(), sizes[0] + sizes[1]);
    drop(charged);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn units_are_lowered_in_order_in_windows_of_a_bounded_size() {
    let units: Vec<usize> = (0..20).collect();
    let windows = windows(units);
    assert!(
        windows
            .iter()
            .all(|window| (1..=LOWERING_WINDOW).contains(&window.len()))
    );
    assert_eq!(windows.concat(), (0..20).collect::<Vec<_>>());
    assert_eq!(windows.len(), 20_usize.div_ceil(LOWERING_WINDOW));
    assert!(super::windows(Vec::<usize>::new()).is_empty());
}

#[test]
fn a_units_parts_hold_its_memory_until_the_last_is_staged() {
    let budget = MemoryBudget::new(1 << 20);
    let held = hold(&budget, &[ids(10)], Vec::new()).remove(0);
    let parts: Vec<(usize, Prepared)> = [ids(100), ids(50)]
        .into_iter()
        .enumerate()
        .map(|(table, batch)| {
            let prepared = Prepared {
                batch,
                discarded_rows: 0,
                discarded_values: 0,
                kept: None,
            };
            (table, prepared)
        })
        .collect();
    let lowered: u64 = parts
        .iter()
        .map(|(_, part)| u64::try_from(part.batch.get_array_memory_size()).unwrap())
        .sum();
    let shared = share_growth(&budget, &parts, held);
    assert_eq!(
        budget.reserved(),
        lowered,
        "the parts' bytes, not the unit's"
    );
    let first = Arc::clone(&shared);
    drop(shared);
    assert_eq!(
        budget.reserved(),
        lowered,
        "held while a part waits to be staged"
    );
    drop(first);
    assert_eq!(budget.reserved(), 0);
}
