//! What a wide table's rows hold, however few, is within what was charged for them.
//!
//! A push holds no more than it is admitted for, in its source's process or decoded from its
//! frame, and a JSON push's batches no more than they keep charged; a normalized split no more
//! than it keeps charged once its parts are planned; cutting keeps nothing a column beside the
//! unit it cuts; and a piece holds no more than it reserved, however many parts it joins: a
//! change stream's flags remapped over the table's columns, every row a filter keeps copied as
//! what it was copied from goes, the arrays of each batch lowering holds of the piece, and the
//! lists of its views' data buffers. Once lowered, a piece keeps no more than it is charged.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, Int8Array, Int64Array, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::cost::push_charge;
use rdlt_connector::{ChangeOp, OP_COLUMN, Push, StreamName};
use rdlt_wire::{Decoder, Encoder, Limits};

use super::super::Held;
use super::super::held::shredded;
use super::super::normalized::split::{kept, made, split};
use super::super::pieces::Pieces;
use crate::cost::tests::{held_alone, peak_alone};
use crate::fixtures::lowering::wide::{dictionaries, flags, integers, text, views};
use crate::fixtures::lowering::{Case, Storage, stamp};
use crate::normalize::{Dropped, Part, PartSchemas, PushKind, Shape};
use crate::partition::ChangeMode;
use crate::plan::{DeleteMode, OnTruncate};
use crate::table::ChangeLayout;

/// Columns of a wide table: a row's flags over them take 125 bytes.
const COLUMNS: usize = 1_000;

/// Columns of the widest table a schema may have at the default budget.
const WIDEST: usize = 6_241;

/// Rows of a piece of many: what each holds beyond its charge adds up past the room a piece's
/// fixed parts leave.
const ROWS: u32 = 2_048;

/// Rows of a piece of many whose every value is converted to text or hashed, which lowers far
/// slower than integers kept as they are: fewer, what they take still far beyond a piece's fixed
/// parts.
const SLOW_ROWS: u32 = 256;

/// Data buffers each column of views holds: Arrow copies their list as it joins and converts
/// them, whatever its rows name.
const VIEW_BUFFERS: usize = 16;

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
    let reserved = reserved(case, &sliced(batch, parts), mode);
    // What the piece keeps once lowered is counted while it holds what it reserved.
    let (peak, (prepared, charged)) = peak_alone(|| {
        let parts = sliced(batch, parts);
        let prepared = super::super::lower(&stream, mode, &parts, case.plan(), &stamp()).unwrap();
        let charged = prepared.growth(&parts);
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
fn a_wide_piece_of_views_holds_no_more_than_it_reserved() {
    // Lowering converts a column at a time, so the list a piece of one part copies passes what
    // its arrays leave room for where a column holds many buffers.
    let mut found = Vec::new();
    for (columns, buffers) in [(COLUMNS, VIEW_BUFFERS), (1, 4_096)] {
        let case = Case::wide_views(2, columns, buffers);
        for parts in [1, 2] {
            if let Err(beyond) = within(&case, case.batch(), parts, None) {
                let at = format!("{columns} columns of {buffers} buffers in {parts} parts");
                found.push(format!("{at}: {beyond}"));
            }
        }
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}

/// What `batch` holds once decoded from the frame it is sent in, the frame's allocation among it,
/// and the batch: its schema and dictionaries are the decoder's, which its read keeps.
fn decoded_alone(batch: &RecordBatch) -> (u64, RecordBatch) {
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(Limits::default());
    decoder
        .schema(&encoder.schema(&batch.schema()).unwrap())
        .unwrap();
    let mut frames = encoder.batch(batch).unwrap();
    let own = frames.pop().expect("a batch's own frame");
    for dictionary in frames {
        decoder.shaped(&dictionary.aligned()).unwrap();
    }
    held_alone(|| decoder.shaped(&own.aligned()).unwrap().0.expect("a batch"))
}

#[test]
fn a_wide_push_holds_no_more_than_it_is_admitted_for() {
    let mut found = Vec::new();
    for columns in [COLUMNS, WIDEST] {
        // Booleans of many rows whose validity, holding no null, Arrow's data of them leaves out,
        // views each holding several data buffers, and text with a validity, whose arrays take
        // all their charge leaves beside its schema.
        let pushes: [(&str, &dyn Fn() -> RecordBatch); 5] = [
            ("integers", &|| integers(1, columns, |row| row)),
            ("dictionaries", &|| dictionaries(1, columns)),
            ("booleans", &|| flags(1_024, columns)),
            ("views", &|| views(1, columns, VIEW_BUFFERS)),
            ("text", &|| text(2, columns)),
        ];
        for (what, make) in pushes {
            let (held, built) = held_alone(make);
            let (decoded, read) = decoded_alone(&built);
            for (how, held, batch) in [("in process", held, built), ("decoded", decoded, read)] {
                let charged = push_charge(&Push::Arrow(batch));
                if held > charged {
                    let at = format!("{columns} columns of {what}, {how}");
                    found.push(format!("{at}: {charged} admitted, {held} held"));
                }
            }
        }
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}

#[tokio::test]
async fn a_wide_json_push_s_batches_hold_no_more_than_they_keep_charged_once_shredded() {
    for columns in [COLUMNS, WIDEST] {
        let budget = crate::budget::budget(1 << 30);
        let (held, batch) = held_alone(|| integers(1, columns, |row| row));
        // Text of as many bytes as the batch holds, admitted for three times them.
        let text = usize::try_from(held).unwrap();
        let pushed = vec![super::json(&budget, text).await];
        let shredded = shredded(pushed, None, vec![batch]);
        let charged = budget.reserved();
        assert!(
            held <= charged,
            "{columns} columns: {charged} charged, {held} held"
        );
        drop(shredded);
    }
}

#[test]
fn a_wide_split_keeps_no_more_than_it_is_charged_once_its_parts_are_planned() {
    let stream = StreamName::new("events").unwrap();
    let mut found = Vec::new();
    for columns in [COLUMNS, WIDEST] {
        for key in [None, Some("id")] {
            let shape = Shape {
                max_depth: 8,
                whole: BTreeSet::new(),
                key: key.into_iter().map(Arc::from).collect(),
            };
            // Views split as they are, their buffers' list joined, whatever the table's width: a
            // stream keyed by `id` has none of them.
            let integers = |rows| integers(rows, columns, |row| row);
            let views = |rows| views(rows, columns, VIEW_BUFFERS);
            let mut batches: Vec<&dyn Fn(u32) -> RecordBatch> = vec![&integers];
            if key.is_none() && columns == COLUMNS {
                batches.push(&views);
            }
            for batch in batches {
                within_split(&stream, &shape, batch, &mut found);
            }
        }
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}

/// Where splitting the rows `batch` makes, normalized as `shape`, in one part and in two, keeps
/// more than it is charged once its parts are planned, what it held and was charged, added to
/// `found`: two rows as they are, and `KEPT` rows after one that planning drops.
fn within_split(
    stream: &StreamName,
    shape: &Shape,
    batch: &dyn Fn(u32) -> RecordBatch,
    found: &mut Vec<String>,
) {
    let cases = [
        ("every row", batch(2)),
        ("its first row dropped", with_new_array(&batch(KEPT + 1))),
    ];
    for (what, batch) in &cases {
        for parts in [1, 2] {
            // The piece cut before the split, its slices of the unit, and what the split's parts
            // keep once they are planned, while their pieces are lowered.
            let (held, (_, _, charged)) = held_alone(|| {
                let piece = sliced(batch, parts);
                let split = split(stream, &piece, shape, &mut PartSchemas::default()).unwrap();
                let planned = planned(split);
                let charged = made(&piece, &planned);
                (piece, kept(&planned), charged)
            });
            if held > charged {
                let (columns, key) = (batch.num_columns(), &shape.key);
                let at = format!(
                    "{columns} columns of {} keyed by {key:?}, {what}, in {parts} parts",
                    batch.schema().field(0).data_type()
                );
                found.push(format!("{at}: {charged} charged, {held} held"));
            }
        }
    }
}

/// Rows a split keeps after one that planning drops: the copies of them take more than what is
/// charged for their arrays leaves over.
const KEPT: u32 = 64;

/// The column of an array whose rows a stream drops.
const NEW: &str = "new";

/// `batch` beside a column of arrays `NEW`, only its first row holding one.
fn with_new_array(batch: &RecordBatch) -> RecordBatch {
    let rows = batch.num_rows();
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let offsets = OffsetBuffer::from_lengths((0..rows).map(|row| usize::from(row == 0)));
    let held = NullBuffer::from_iter((0..rows).map(|row| row == 0));
    let new = ListArray::new(
        item,
        offsets,
        Arc::new(Int64Array::from(vec![7])),
        Some(held),
    );
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(NEW, new.data_type().clone(), true)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(new));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// A split's `parts` planned as a stream that drops the rows holding a new array plans them: the
/// array's part goes, and every other loses the rows that held it and their descendants, pruned
/// parents first.
fn planned(parts: Vec<Part>) -> Vec<Part> {
    let mut dropped = Dropped::new(PushKind::Arrow);
    let mut kept = Vec::with_capacity(parts.len());
    for part in parts {
        if matches!(part.path.as_slice(), [column] if column.as_ref() == NEW) {
            dropped.parents_of(&part);
        } else {
            kept.push(part);
        }
    }
    kept.into_iter()
        .map(|part| dropped.prune(part).unwrap().part)
        .collect()
}

#[test]
fn cutting_a_wide_unit_keeps_nothing_a_column_beside_its_batches() {
    let mut found = Vec::new();
    for columns in [COLUMNS, WIDEST] {
        // A change stream's unit, its table's columns aligned to its batches', and a merge's.
        let layout = ChangeLayout::Log;
        let changes = Case::wide_changes(4, columns, layout, Storage::Native);
        let cases = [
            ("a change stream's", changes, Some(mode(layout))),
            ("a merge's", Case::wide_merge(4, columns), None),
        ];
        for (what, case, mode) in &cases {
            let batch = case.batch();
            let measured = |bounds| {
                super::super::measured(case.plan(), case.rendering(), batch, *mode, bounds)
            };
            // Pieces of a row each: a piece's rows may take a byte.
            let whole = measured((u64::MAX, u64::MAX));
            let bounds = (whole.piece + whole.part + 1, u64::MAX);
            let (alone, _) = held_alone(|| vec![batch.clone()]);
            // What holds the unit and the cutter, which a plan tells how to measure it.
            let (cutting, _) = held_alone(|| {
                let unit = Held::of(vec![batch.clone()], Vec::new());
                let mut pieces = Pieces::new(unit, measured(bounds));
                let piece = pieces.next().unwrap().expect("the unit has rows");
                assert_eq!(piece.rows, 1);
                pieces
            });
            let beside = cutting.saturating_sub(alone);
            if beside >= 1_024 {
                found.push(format!(
                    "{what} of {columns} columns: {beside} bytes beside"
                ));
            }
        }
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}
