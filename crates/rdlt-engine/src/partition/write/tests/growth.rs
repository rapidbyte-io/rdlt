//! A piece is charged for the allocations it keeps alive beyond its unit's batches, each once,
//! however earlier pieces of the unit were freed and wherever the allocator placed it.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_buffer::{Buffer, NullBuffer, ScalarBuffer};
use proptest::prelude::*;
use rdlt_connector::cost::Allocations;

use super::super::Held;
use super::super::normalized::{kept, made, part_growth};
use super::{allocated, buffers, ids};
use crate::table::Prepared;

#[test]
fn the_loads_constant_columns_and_the_units_batches_count_for_no_growth() {
    let rows = ids(100);
    let prepared = lowered(Arc::clone(rows.column(0)));
    assert_eq!(
        prepared.growth(&Allocations::default()),
        buffers(&rows),
        "the constant columns count nothing"
    );
    // What the unit's batches keep alive is no growth, however many pieces keep it alive.
    let unit = Held::of(vec![rows.clone()], Vec::new());
    assert_eq!(prepared.growth(&unit.allocations), 0);
    assert_eq!(prepared.growth(&unit.allocations), 0);
    assert_eq!(unit.allocations.bytes(), allocated(&rows));
}

/// A batch lowered from a unit: the load's two constant columns, then `column`.
fn lowered(column: ArrayRef) -> Prepared {
    lowered_all(vec![column])
}

/// A batch lowered from a unit: the load's two constant columns, then `columns`, of as many rows
/// as the first.
fn lowered_all(columns: Vec<ArrayRef>) -> Prepared {
    let constant = ids(i64::try_from(columns[0].len()).unwrap());
    let constants = [("load", constant.column(0)), ("at", constant.column(0))];
    let mut named: Vec<(String, ArrayRef)> = constants
        .into_iter()
        .map(|(name, column)| (name.to_owned(), Arc::clone(column)))
        .collect();
    named.extend(
        columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| (format!("c{index}"), column)),
    );
    Prepared {
        batch: RecordBatch::try_from_iter(named).unwrap(),
        view: crate::table::testing::view("t"),
        discarded_rows: 0,
        discarded_values: 0,
    }
}

/// Integers in the very allocation `freed` held, as many as it holds, which nothing else holds:
/// what the allocator does when it hands out again the chunk a freed piece held.
fn in_place_of(freed: Buffer) -> ArrayRef {
    let rows = freed.len() / 8;
    let mut reused = freed
        .into_mutable()
        .expect("nothing else holds the allocation");
    reused.truncate(rows * 8);
    reused.typed_data_mut::<i64>().fill(7);
    let values = ScalarBuffer::new(reused.into(), 0, rows);
    Arc::new(Int64Array::new(values, None))
}

#[test]
fn a_piece_allocated_where_an_earlier_piece_of_its_unit_was_freed_is_charged_in_full() {
    let held = Held::of(vec![ids(100)], Vec::new());
    let first: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1_000));
    let bytes = buffers(&RecordBatch::try_from_iter([("id", Arc::clone(&first))]).unwrap());
    let values = first.to_data().buffers()[0].clone();
    let address = values.as_ptr();
    let piece = lowered(first);
    assert_eq!(piece.growth(&held.allocations), bytes);
    // The piece is written and freed, and the next piece of the unit is lowered where it was.
    drop(piece);
    let second = lowered(in_place_of(values));
    assert_eq!(
        second.batch.column(2).to_data().buffers()[0].as_ptr(),
        address
    );
    assert_eq!(second.growth(&held.allocations), bytes);
}

/// A unit's shape: normalized to depth 8, keyless.
fn keyless() -> crate::normalize::Shape {
    crate::normalize::Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    }
}

#[test]
fn a_split_made_where_an_earlier_split_of_its_unit_was_freed_is_charged_in_full() {
    let unit = ids(1_000);
    let held = Held::of(vec![unit.clone()], Vec::new());
    let made = |parts: &[crate::normalize::Part]| made(parts, &held.allocations);
    let first = crate::normalize::normalize(&unit, &keyless()).unwrap();
    let charged = made(&first);
    let rows = first[0].lineage.root_row.to_data().buffers()[0].clone();
    // The split's parts are written and freed, and the unit's next split is made where they were.
    drop(first);
    let mut second = crate::normalize::normalize(&unit, &keyless()).unwrap();
    second[0].lineage.root_row = in_place_of(rows);
    assert_eq!(made(&second), charged);
}

#[test]
fn what_holds_a_split_keeps_the_parts_it_planned_alive_until_it_goes() {
    let unit = ids(1_000);
    let parts = crate::normalize::normalize(&unit, &keyless()).unwrap();
    let lineage = parts[0].lineage.id.to_data().buffers()[1].clone();
    let held = kept(&parts);
    drop(parts);
    assert!(
        lineage.strong_count() > 1,
        "the split's lineage lives while what holds it does"
    );
    drop(held);
    assert_eq!(lineage.strong_count(), 1);
}

#[test]
fn a_units_parts_grow_it_by_their_lineage_and_their_schemas() {
    let unit = ids(1000);
    let held = Held::of(vec![unit.clone()], Vec::new());
    let parts = crate::normalize::normalize(&unit, &keyless()).unwrap();
    // The roots' ids are one buffer of exactly a row's 32 bytes each.
    let ids = parts[0].lineage.id.to_data();
    assert_eq!(ids.buffers()[1].capacity(), 1000 * 32);
    // The part keeps the unit's column alive, which is held already: its lineage is new.
    let lineage: usize = parts
        .iter()
        .map(|part| {
            let lineage = &part.lineage;
            lineage
                .id
                .to_data()
                .buffers()
                .iter()
                .map(Buffer::capacity)
                .sum::<usize>()
                + lineage.root_row.to_data().buffers()[0].capacity()
        })
        .sum();
    // Each part's batch has a schema of its own, which it keeps alive too.
    let schemas: u64 = parts
        .iter()
        .map(|part| rdlt_connector::cost::schema_bytes(&part.batch.schema()))
        .sum();
    let mut piece = Allocations::default();
    let grown: u64 = parts
        .iter()
        .map(|part| part_growth(part, &held.allocations, &mut piece))
        .sum();
    assert_eq!(grown, u64::try_from(lineage).unwrap() + schemas);
    // Counted once within the piece.
    let again: u64 = parts
        .iter()
        .map(|part| part_growth(part, &held.allocations, &mut piece))
        .sum();
    assert_eq!(again, 0);
    // None of it joined the unit's set, which holds only the unit's batches.
    assert_eq!(made(&parts, &held.allocations), grown);
    assert_eq!(held.allocations.bytes(), allocated(&unit));
}

/// What one column of a piece is.
#[derive(Clone, Copy, Debug)]
enum Column {
    /// One of the unit's columns, its rows from `first`.
    Unit { index: usize, first: usize },
    /// Integers made for the piece, with nulls or without.
    Made { nulls: bool },
    /// The piece's column before it, again; made where it is the first.
    Again,
    /// Integers made in the allocation a piece written and freed before held, where one is left;
    /// made anew where none is.
    InPlace,
}

fn column() -> impl Strategy<Value = Column> {
    prop_oneof![
        (0_usize..3, 0_usize..1_000).prop_map(|(index, first)| Column::Unit { index, first }),
        any::<bool>().prop_map(|nulls| Column::Made { nulls }),
        Just(Column::Again),
        Just(Column::InPlace),
    ]
}

/// Where each allocation `arrays` keep alive starts, and its bytes: every buffer of every node,
/// nulls included, walked anew.
fn allocations_of<'a>(arrays: impl IntoIterator<Item = &'a ArrayRef>) -> BTreeMap<usize, u64> {
    let mut starts = BTreeMap::new();
    for array in arrays {
        let mut pending = vec![array.to_data()];
        while let Some(data) = pending.pop() {
            let nulls = data.nulls().map(|nulls| nulls.buffer().clone());
            for buffer in data.buffers().iter().chain(&nulls) {
                if buffer.capacity() > 0 {
                    let bytes = u64::try_from(buffer.capacity()).unwrap();
                    starts.insert(buffer.data_ptr().as_ptr().addr(), bytes);
                }
            }
            pending.extend(data.child_data().iter().cloned());
        }
    }
    starts
}

/// The bytes of the allocations `columns` keep alive that `unit`'s batches do not, each once.
fn afresh(unit: &[RecordBatch], columns: &[ArrayRef]) -> u64 {
    let held = allocations_of(unit.iter().flat_map(RecordBatch::columns));
    allocations_of(columns)
        .into_iter()
        .filter(|(start, _)| !held.contains_key(start))
        .map(|(_, bytes)| bytes)
        .sum()
}

proptest! {
    /// Each piece of a unit is charged the allocations it keeps alive beyond the unit's batches,
    /// counted afresh: whatever it shares with the unit or repeats, and wherever it was made,
    /// in the very allocation a piece written before it held among them.
    #[test]
    fn each_piece_s_growth_is_its_allocations_beyond_its_unit_counted_afresh(
        unit in proptest::collection::vec(1_500_usize..2_000, 1..4),
        rows in 1_usize..500,
        pieces in proptest::collection::vec(
            (proptest::collection::vec(column(), 1..5), any::<bool>()),
            1..8,
        ),
    ) {
        let unit: Vec<RecordBatch> = unit
            .into_iter()
            .map(|rows| ids(i64::try_from(rows).unwrap()))
            .collect();
        let held = Held::of(unit.clone(), Vec::new());
        let made_anew = |nulls: bool| -> ArrayRef {
            let values = (0..i64::try_from(rows).unwrap()).collect::<Vec<_>>();
            let nulls = nulls.then(|| NullBuffer::from_iter((0..rows).map(|row| row % 3 > 0)));
            Arc::new(Int64Array::new(ScalarBuffer::from(values), nulls))
        };
        let mut freed: Vec<Buffer> = Vec::new();
        for (columns, written) in pieces {
            let mut made: Vec<ArrayRef> = Vec::new();
            let mut own: Option<Buffer> = None;
            for column in columns {
                let (array, anew) = match (column, made.last()) {
                    (Column::Unit { index, first }, _) => {
                        (unit[index % unit.len()].column(0).slice(first, rows), false)
                    }
                    (Column::Again, Some(before)) => (Arc::clone(before), false),
                    (Column::InPlace, _) if !freed.is_empty() => {
                        (in_place_of(freed.pop().unwrap()), true)
                    }
                    (Column::Made { nulls }, _) => (made_anew(nulls), true),
                    (Column::Again | Column::InPlace, _) => (made_anew(false), true),
                };
                if anew && own.is_none() {
                    own = Some(array.to_data().buffers()[0].clone());
                }
                made.push(array);
            }
            let piece = lowered_all(made.clone());
            prop_assert_eq!(piece.growth(&held.allocations), afresh(&unit, &made));
            if written {
                // Written and freed: what it made alone may be handed out again.
                drop((piece, made));
                freed.extend(own);
            }
        }
        let mut alive = Allocations::default();
        for batch in &unit {
            alive.add(batch);
        }
        prop_assert_eq!(held.allocations.as_ref(), &alive);
    }
}
