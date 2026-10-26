//! What a wide table's rows hold, however few, is within what was charged for them.
//!
//! Cutting keeps nothing a column beside the unit it cuts; and a piece holds no more than it
//! reserved, however many parts it joins: a change stream's flags remapped over the table's
//! columns, every row a filter keeps copied as what it was copied from goes, and the arrays of
//! each batch lowering holds of the piece. Once lowered, a piece keeps no more than it is
//! charged.

use std::sync::Arc;

use arrow_array::{Int8Array, RecordBatch};
use rdlt_connector::cost::Allocations;
use rdlt_connector::{ChangeOp, OP_COLUMN, StreamName};

use super::super::Held;
use super::super::pieces::Pieces;
use crate::cost::tests::{held_alone, peak_alone};
use crate::fixtures::lowering::{Case, Storage, stamp};
use crate::partition::ChangeMode;
use crate::plan::{DeleteMode, OnTruncate};
use crate::table::ChangeLayout;

/// Columns of a wide table: a row's flags over them take 125 bytes.
const COLUMNS: usize = 1_000;

/// Columns of the widest table a schema may have at the default budget.
const WIDEST: usize = 7_489;

/// Rows of a piece of many: what each holds beyond its charge adds up past the room a piece's
/// fixed parts leave.
const ROWS: u32 = 2_048;

/// Rows of a piece of many whose every value is converted to text or hashed, which lowers far
/// slower than integers kept as they are: fewer, what they take still far beyond a piece's fixed
/// parts.
const SLOW_ROWS: u32 = 256;

/// A piece measured: the columns of its table, its rows, and how many parts they are cut into.
#[derive(Clone, Copy, Debug)]
struct Cut {
    columns: usize,
    rows: u32,
    parts: usize,
}

/// The pieces of a wide table measured: `many` rows in two parts, a row in one part, and two rows
/// in two.
fn pieces(many: u32) -> [Cut; 3] {
    let cut = |rows, parts| Cut {
        columns: COLUMNS,
        rows,
        parts,
    };
    [cut(many, 2), cut(1, 1), cut(2, 2)]
}

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

/// `batch` cut into `parts` slices of as many rows each, the last of what is left.
fn sliced(batch: &RecordBatch, parts: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    let each = rows.div_ceil(parts);
    let firsts = (0..rows).step_by(each);
    firsts
        .map(|first| batch.slice(first, each.min(rows - first)))
        .collect()
}

/// What the write path reserves for `parts`, rows of `case`'s table read as `mode` says, as one
/// piece.
fn reserved(case: &Case, parts: &[RecordBatch], mode: Option<ChangeMode>) -> u64 {
    let bounds = (u64::MAX, u64::MAX);
    let lowered = super::super::measured(case.plan(), case.rendering(), &parts[0], mode, bounds);
    let mut pieces = Pieces::new(Held::of(parts.to_vec(), Vec::new()), lowered);
    let piece = pieces.next().unwrap().expect("the parts have rows");
    let rows: usize = parts.iter().map(RecordBatch::num_rows).sum();
    assert_eq!((piece.rows, piece.parts.len()), (rows, parts.len()));
    piece.bytes
}

/// The rows `case`'s plan makes of `batch` cut into `parts` parts and read as `mode` says, where
/// lowering them holds no more at its peak than the piece reserved, slicing them included, and
/// keeps no more beyond `batch` than it is charged once lowered; how far it passes either
/// otherwise.
fn within(
    case: &Case,
    batch: &RecordBatch,
    parts: usize,
    mode: Option<ChangeMode>,
) -> Result<usize, String> {
    let stream = StreamName::new("events").unwrap();
    let lower = || {
        let parts = sliced(batch, parts);
        super::super::lower(&stream, mode, &parts, case.plan(), &stamp()).unwrap()
    };
    let unit = Allocations::of(batch);
    let reserved = reserved(case, &sliced(batch, parts), mode);
    // What the piece keeps once lowered is counted while it holds what it reserved.
    let (peak, (prepared, charged)) = peak_alone(|| {
        let prepared = lower();
        let charged = prepared.growth(&unit);
        (prepared, charged)
    });
    let rows = prepared.batch.num_rows();
    if peak > reserved {
        return Err(format!("{reserved} reserved for {rows} rows, {peak} held"));
    }
    let (kept, _) = held_alone(lower);
    if kept > charged {
        return Err(format!(
            "{charged} charged for {rows} rows lowered, {kept} kept"
        ));
    }
    Ok(rows)
}

/// Where a wide change stream's pieces, held as `layout` says and stored natively and as text,
/// hold more than they reserved or keep more than they are charged.
fn changes_beyond(layout: ChangeLayout) -> Vec<String> {
    let mut found = Vec::new();
    for (storage, many) in [(Storage::Native, ROWS), (Storage::Text, SLOW_ROWS)] {
        for piece in pieces(many) {
            let case = Case::wide_changes(piece.rows, piece.columns, layout, storage);
            // Rows the stream ignores are dropped from the parts joined into one batch.
            let pushed = [
                ("updates", case.batch().clone()),
                ("updates and deletes", with_deletes(case.batch())),
            ];
            for (what, batch) in pushed {
                if let Err(beyond) = within(&case, &batch, piece.parts, Some(mode(layout))) {
                    let at = format!("stored {storage:?}, {what}, {piece:?}");
                    found.push(format!("{at}: {beyond}"));
                }
            }
        }
    }
    found
}

#[test]
fn a_wide_change_log_piece_holds_no_more_than_it_reserved() {
    let found = changes_beyond(ChangeLayout::Log);
    assert!(found.is_empty(), "{}", found.join("\n"));
}

#[test]
fn a_wide_merged_change_piece_holds_no_more_than_it_reserved() {
    let found = changes_beyond(ChangeLayout::Merge { soft: false });
    assert!(found.is_empty(), "{}", found.join("\n"));
}

#[test]
fn a_wide_merge_keeps_the_last_row_of_each_key_within_what_it_reserved() {
    for piece in pieces(ROWS) {
        let case = Case::wide_merge(piece.rows, piece.columns);
        let kept = case.kept().rows;
        let within = within(&case, case.batch(), piece.parts, None);
        assert_eq!(within, Ok(kept), "{piece:?}");
    }
}

#[test]
fn a_wide_piece_drops_rows_within_what_it_reserved() {
    for piece in pieces(ROWS) {
        let case = Case::wide_dropped_rows(piece.rows, piece.columns);
        let kept = case.kept().rows;
        let within = within(&case, case.batch(), piece.parts, None);
        assert_eq!(within, Ok(kept), "{piece:?}");
    }
}

#[test]
fn a_wide_history_piece_hashes_its_versions_within_what_it_reserved() {
    // Fields built again for each piece, one a column, pass what a piece of a row leaves over
    // only at the widest table's width.
    let widest = Cut {
        columns: WIDEST,
        rows: 1,
        parts: 1,
    };
    for piece in pieces(SLOW_ROWS).into_iter().chain([widest]) {
        let case = Case::wide_history(piece.rows, piece.columns);
        let kept = case.kept().rows;
        let within = within(&case, case.batch(), piece.parts, None);
        assert_eq!(within, Ok(kept), "{piece:?}");
    }
}

#[test]
fn cutting_a_wide_unit_keeps_nothing_of_it_beside_it_between_its_pieces() {
    let layout = ChangeLayout::Log;
    for columns in [COLUMNS, WIDEST] {
        let case = Case::wide_changes(4, columns, layout, Storage::Native);
        let batch = case.batch();
        let measured = |bounds| {
            let mode = Some(mode(layout));
            super::super::measured(case.plan(), case.rendering(), batch, mode, bounds)
        };
        // Pieces of a row each: a piece's rows may take a byte.
        let whole = measured((u64::MAX, u64::MAX));
        let lowered = measured((whole.piece + whole.part + 1, u64::MAX));
        let unit = || Held::of(vec![batch.clone()], Vec::new());
        let (alone, _) = held_alone(|| (unit(), lowered.clone()));
        let (cutting, _) = held_alone(|| {
            let mut pieces = Pieces::new(unit(), lowered.clone());
            let piece = pieces.next().unwrap().expect("the unit has rows");
            assert_eq!(piece.rows, 1);
            pieces
        });
        let beside = cutting.saturating_sub(alone);
        assert!(
            beside < 1_024,
            "{columns} columns: cutting keeps {beside} bytes"
        );
    }
}
