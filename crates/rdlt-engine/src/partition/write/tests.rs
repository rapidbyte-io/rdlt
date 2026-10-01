use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::cost::{Allocations, Rendering};
use rdlt_connector::{Partition, Permit, StreamName};

use super::normalized::{charge_parts, judge, part_growth, share_growth};
use super::{Held, LOWERING_WINDOW, charge_growth, hold, shred_failed, windows};
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
        changes: None,
        stop: tokio_util::sync::CancellationToken::new(),
        follow: false,
        reset_retention: false,
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

/// A destination storing every value as it is.
fn native() -> Rendering {
    Rendering::new(rdlt_testkit::drawn::KINDS)
}

/// What `batch` is charged.
fn charge(batch: &RecordBatch) -> u64 {
    native().cost(batch, u64::MAX).charge()
}

/// The bytes `batch` keeps alive.
fn allocated(batch: &RecordBatch) -> u64 {
    Allocations::of(batch).bytes()
}

fn ids(rows: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    RecordBatch::try_from_iter([("id", ids)]).unwrap()
}

/// `batch` as a table of no columns of its own would hold it: after the two constant columns
/// every table leads its metadata with.
fn prepared(batch: &RecordBatch) -> Prepared {
    let constant = || arrow_array::new_null_array(&arrow_schema::DataType::Null, batch.num_rows());
    let columns = [("load", constant()), ("at", constant())]
        .into_iter()
        .chain([("id", Arc::clone(batch.column(0)))]);
    let batch = RecordBatch::try_from_iter(columns).unwrap();
    Prepared {
        batch,
        view: crate::table::testing::view("t"),
        discarded_rows: 0,
        discarded_values: 0,
    }
}

#[tokio::test]
async fn shredded_batches_are_charged_before_the_pushes_they_came_from_are_released() {
    let budget = MemoryBudget::new(1 << 20);
    let pushed: Permit = Box::new(budget.acquire(400).await);
    let batches = [ids(10), ids(1000)];
    let sizes = [charge(&batches[0]), charge(&batches[1])];
    assert!(sizes[1] >= 8_000);
    let held = hold(&budget, &native(), &batches, vec![pushed]);
    assert_eq!(budget.peak(), 400 + sizes[0] + sizes[1]);
    assert_eq!(budget.reserved(), sizes[0] + sizes[1]);
    // Each holds what its batch keeps alive, and spares what it was charged beyond that.
    for ((held, batch), size) in held.iter().zip(&batches).zip(sizes) {
        assert_eq!(held.allocations.lock().bytes(), allocated(batch));
        assert_eq!(held.spare, size - allocated(batch));
    }
    drop(held);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn a_lowered_batch_is_charged_for_what_its_unit_did_not_hold() {
    let budget = MemoryBudget::new(1 << 20);
    let unit = ids(1000);
    let held = hold(&budget, &native(), std::slice::from_ref(&unit), Vec::new()).remove(0);
    let charged = budget.reserved();
    // A batch lowered as it is keeps alive only what its unit holds already.
    let held = charge_growth(&budget, &prepared(&unit.slice(10, 20)), held);
    assert_eq!(budget.reserved(), charged);
    // One lowered into buffers of its own is charged for them.
    let converted = ids(500);
    let held = charge_growth(&budget, &prepared(&converted), held);
    assert_eq!(budget.reserved(), charged + allocated(&converted));
    // And once only, however many pieces keep them alive.
    let held = charge_growth(&budget, &prepared(&converted.slice(0, 5)), held);
    assert_eq!(budget.reserved(), charged + allocated(&converted));
    drop(held);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn a_units_spare_charge_pays_for_what_lowering_allocates() {
    let budget = MemoryBudget::new(1 << 20);
    let unit = ids(10);
    let size = allocated(&unit);
    let permit: Permit = Box::new(budget.charge(size + 100));
    let held = Held::of(vec![permit], size + 100, std::slice::from_ref(&unit));
    assert_eq!(held.spare, 100);
    let lowered = ids(100);
    let held = charge_growth(&budget, &prepared(&lowered), held);
    assert_eq!(budget.reserved(), size + allocated(&lowered));
    assert_eq!(held.spare, 0);
    // A piece of the unit shares what it holds, and spares nothing.
    let piece = held.piece();
    assert!(piece.permits.is_empty());
    assert_eq!(piece.spare, 0);
    let grown = budget.reserved();
    drop(charge_growth(&budget, &prepared(&lowered), piece));
    assert_eq!(budget.reserved(), grown);
}

#[test]
fn the_loads_constant_columns_count_for_no_growth() {
    let rows = ids(100);
    let constant = ids(100);
    let columns = [
        ("load", Arc::clone(constant.column(0))),
        ("at", Arc::clone(constant.column(0))),
        ("id", Arc::clone(rows.column(0))),
    ];
    let prepared = Prepared {
        batch: RecordBatch::try_from_iter(columns).unwrap(),
        view: crate::table::testing::view("t"),
        discarded_rows: 0,
        discarded_values: 0,
    };
    let mut held = Allocations::default();
    assert_eq!(prepared.growth(&mut held), allocated(&rows));
    assert_eq!(held.bytes(), allocated(&rows));
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
    let unit = ids(10);
    let held = hold(&budget, &native(), std::slice::from_ref(&unit), Vec::new()).remove(0);
    let charged = budget.reserved();
    let parts: Vec<(usize, Prepared)> = [ids(100), ids(50)]
        .iter()
        .map(prepared)
        .enumerate()
        .collect();
    let lowered = allocated(&ids(100)) + allocated(&ids(50));
    let shared = share_growth(&budget, &parts, held);
    assert_eq!(
        budget.reserved(),
        charged + lowered,
        "the unit's bytes and its parts'"
    );
    let first = Arc::clone(&shared);
    drop(shared);
    assert_eq!(
        budget.reserved(),
        charged + lowered,
        "held while a part waits to be staged"
    );
    drop(first);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn normalized_parts_are_charged_before_they_wait_on_their_tables() {
    let budget = MemoryBudget::new(1 << 22);
    let unit = ids(1000);
    let held = hold(&budget, &native(), std::slice::from_ref(&unit), Vec::new()).remove(0);
    let charged = budget.reserved();
    let shape = crate::normalize::Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let parts = crate::normalize::normalize(&unit, &shape).unwrap();
    // The part keeps the unit's column alive, which is charged already: its lineage is new.
    let lineage: usize = parts
        .iter()
        .map(|part| {
            let lineage = &part.lineage;
            lineage
                .id
                .to_data()
                .buffers()
                .iter()
                .map(arrow_buffer::Buffer::capacity)
                .sum::<usize>()
                + lineage.root_row.to_data().buffers()[0].capacity()
        })
        .sum();
    let held = charge_parts(&budget, &parts, held);
    assert_eq!(budget.reserved(), charged + u64::try_from(lineage).unwrap());
    drop(held);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn judged_parts_are_charged_while_they_are_judged_and_released_after() {
    let budget = MemoryBudget::new(1 << 30);
    let shape = crate::normalize::Shape {
        max_depth: 2,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let integers = |values: Vec<i64>| {
        let values: ArrayRef = Arc::new(Int64Array::from(values));
        RecordBatch::try_from_iter([("n", values)]).unwrap()
    };
    let units: Vec<Vec<crate::normalize::Part>> = [vec![1_i64 << 60], vec![1, 2, 3]]
        .into_iter()
        .map(|values| crate::normalize::normalize(&integers(values), &shape).unwrap())
        .collect();
    let mut allocations = Allocations::default();
    let bytes: u64 = units
        .iter()
        .flatten()
        .map(|part| part_growth(part, &mut allocations))
        .sum();
    assert!(bytes > 0);
    let rounding = judge(&job(), &budget, units).unwrap();
    assert_eq!(budget.peak(), bytes);
    assert_eq!(budget.reserved(), 0);
    let root: Vec<String> = rounding[&Vec::new()]
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(root, ["n"]);
}

#[test]
fn a_row_expanding_beyond_the_budget_is_a_source_error_naming_its_stream() {
    let row = super::slices::RowTooLarge {
        expanded: 9_000,
        budget: 4_096,
    };
    let error = super::row_too_large(&job(), &row);
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Source, Some("row_exceeds_budget"))
    );
    assert_eq!(error.stream(), Some(&job().stream));
}

#[test]
fn written_bytes_count_a_slices_own_rows() {
    let whole = ids(1000);
    assert_eq!(super::written_bytes(&whole), 8_000);
    assert_eq!(super::written_bytes(&whole.slice(0, 10)), 80);
}
