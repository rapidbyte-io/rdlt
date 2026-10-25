//! A change stream's piece is lowered within what it reserved, however wide its table: its
//! unchanged flags remapped over the table's columns among what it holds.

use rdlt_connector::StreamName;

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

/// What lowering `case`'s batch as a piece of two parts holds at its peak, and what the write
/// path reserved for the piece.
fn lowered(case: &Case, layout: ChangeLayout) -> (u64, u64) {
    let batch = case.batch();
    let half = batch.num_rows() / 2;
    let parts = [
        batch.slice(0, half),
        batch.slice(half, batch.num_rows() - half),
    ];
    let stream = StreamName::new("events").unwrap();
    let (peak, prepared) = peak_alone(|| {
        super::super::lower(&stream, Some(mode(layout)), &parts, case.plan(), &stamp())
    });
    prepared.unwrap();
    (peak, case.charge())
}

#[test]
fn a_wide_change_piece_holds_no_more_a_row_than_it_reserved_a_row() {
    let mut beyond = Vec::new();
    for layout in [ChangeLayout::Log, ChangeLayout::Merge { soft: false }] {
        for storage in [Storage::Native, Storage::Text] {
            let wide = |rows| Case::wide_changes(rows, COLUMNS, layout, storage);
            let (peak, charge) = lowered(&wide(ROWS), layout);
            // A piece's arrays and its schema's fields take what they take whatever its rows:
            // a piece of a row a part holds them, and next to nothing a row.
            let (fixed, least) = lowered(&wide(2), layout);
            let (held, reserved) = (peak.saturating_sub(fixed), charge - least);
            if held > reserved + SLACK {
                beyond.push(format!(
                    "{layout:?} stored {storage:?}: {reserved} reserved for its rows, {held} held"
                ));
            }
        }
    }
    assert!(beyond.is_empty(), "{}", beyond.join("\n"));
}
