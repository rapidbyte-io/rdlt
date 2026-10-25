//! A piece of a wide table is lowered within what its rows reserved, however many parts it
//! joins: a change stream's flags remapped over the table's columns, and every row a filter keeps
//! copied as what it was copied from goes.

use std::sync::Arc;

use arrow_array::{Int8Array, RecordBatch};
use rdlt_connector::{ChangeOp, OP_COLUMN, StreamName};

use crate::cost::tests::lowering::SLACK;
use crate::cost::tests::peak_alone;
use crate::fixtures::lowering::{Case, Storage, stamp};
use crate::partition::ChangeMode;
use crate::plan::{DeleteMode, OnTruncate};
use crate::table::ChangeLayout;

/// Columns of the wide table: a row's flags over them take 125 bytes.
const COLUMNS: usize = 1_000;

/// Rows of the piece measured.
const ROWS: u32 = 2_048;

/// How a stream reads changes into a table that holds them as `layout` says: as a log, or
/// merged by key into a destination that keeps a column an update flags unchanged, ignoring
/// deletes and truncates.
fn mode(layout: ChangeLayout) -> ChangeMode {
    ChangeMode {
        merge: layout != ChangeLayout::Log,
        deletes: DeleteMode::Ignore,
        truncates: OnTruncate::Ignore,
        partial_updates: true,
    }
}

/// `batch`, a change batch, its first row and every sixty-fourth after it a delete.
fn with_deletes(batch: &RecordBatch) -> RecordBatch {
    let ops = (0..batch.num_rows()).map(|row| {
        let op = if row % 64 == 0 {
            ChangeOp::Delete
        } else {
            ChangeOp::Update
        };
        op.code()
    });
    let index = batch.schema().index_of(OP_COLUMN).unwrap();
    let mut columns = batch.columns().to_vec();
    columns[index] = Arc::new(Int8Array::from_iter_values(ops));
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

/// What lowering `batch`, `case`'s rows, as a piece of two parts read as `mode` holds at its
/// peak, and the rows it makes.
fn lowered(case: &Case, batch: &RecordBatch, mode: Option<ChangeMode>) -> (u64, usize) {
    let half = batch.num_rows() / 2;
    let parts = [
        batch.slice(0, half),
        batch.slice(half, batch.num_rows() - half),
    ];
    let stream = StreamName::new("events").unwrap();
    let (peak, prepared) =
        peak_alone(|| super::super::lower(&stream, mode, &parts, case.plan(), &stamp()));
    (peak, prepared.unwrap().batch.num_rows())
}

/// The rows lowering the case `wide` makes of `ROWS` rows, each batch as `pushed` makes it and
/// read as `mode`, where what it holds a row is within what its rows reserved, and how far it
/// is not otherwise: a piece of a row a part holds what a piece's arrays and schema take
/// whatever its rows, which is left out.
fn within(
    wide: impl Fn(u32) -> Case,
    pushed: impl Fn(&RecordBatch) -> RecordBatch,
    mode: Option<ChangeMode>,
) -> Result<usize, String> {
    let (whole, least) = (wide(ROWS), wide(2));
    let (peak, rows) = lowered(&whole, &pushed(whole.batch()), mode);
    let (fixed, _) = lowered(&least, &pushed(least.batch()), mode);
    let (held, reserved) = (peak.saturating_sub(fixed), whole.charge() - least.charge());
    if held > reserved + SLACK {
        return Err(format!("{reserved} reserved for {rows} rows, {held} held"));
    }
    Ok(rows)
}

#[test]
fn a_wide_change_piece_holds_no_more_a_row_than_it_reserved_a_row() {
    let mut found = Vec::new();
    for layout in [ChangeLayout::Log, ChangeLayout::Merge { soft: false }] {
        for storage in [Storage::Native, Storage::Text] {
            let wide = |rows| Case::wide_changes(rows, COLUMNS, layout, storage);
            let all = within(wide, RecordBatch::clone, Some(mode(layout)));
            // Rows the stream ignores are dropped from the parts joined into one batch.
            let deletes = within(wide, with_deletes, Some(mode(layout)));
            for (what, within) in [("updates", all), ("updates and deletes", deletes)] {
                if let Err(beyond) = within {
                    found.push(format!("{layout:?} stored {storage:?}, {what}: {beyond}"));
                }
            }
        }
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}

#[test]
fn a_wide_merge_of_several_parts_keeps_the_last_row_of_each_key_within_what_its_rows_reserved() {
    let wide = |rows| Case::wide_merge(rows, COLUMNS);
    let kept = wide(ROWS).kept().rows;
    assert_eq!(within(wide, RecordBatch::clone, None), Ok(kept));
}

#[test]
fn a_wide_piece_of_several_parts_drops_rows_within_what_its_rows_reserved() {
    let wide = |rows| Case::wide_dropped_rows(rows, COLUMNS);
    let kept = wide(ROWS).kept().rows;
    assert_eq!(within(wide, RecordBatch::clone, None), Ok(kept));
}
