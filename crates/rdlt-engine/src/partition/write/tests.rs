use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::cost::Allocations;
use rdlt_connector::{LogicalType, Partition, Permit, StreamName};

use super::Held;
use super::held::shredded;
use super::normalized::{judge, part_growth};
use super::queue::written_bytes;
use super::units::shred_failed;
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

/// The bytes `batch` keeps alive, its schema with its buffers.
fn allocated(batch: &RecordBatch) -> u64 {
    Allocations::of(batch).bytes()
}

/// The bytes the columns of `batch` keep alive.
fn buffers(batch: &RecordBatch) -> u64 {
    allocated(batch) - rdlt_connector::cost::schema_bytes(&batch.schema())
}

fn ids(rows: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    RecordBatch::try_from_iter([("id", ids)]).unwrap()
}

/// What admitted a JSON push of `bytes` of text under `budget`.
async fn json(budget: &MemoryBudget, bytes: usize) -> Permit {
    let admission = crate::cost::Charging::new(budget.clone().read_by(1));
    let text = bytes::Bytes::from(vec![b' '; bytes]);
    let push = rdlt_connector::SourceEvent::Push(rdlt_connector::Push::Json(text));
    let admitted = rdlt_connector::Admission::admit(&admission, &push).await;
    admitted.unwrap().unwrap()
}

#[tokio::test]
async fn shredded_batches_hold_what_they_keep_alive_of_what_their_pushes_were_admitted_for() {
    let budget = MemoryBudget::new(1 << 24);
    // Two pushes of text, each admitted for its text and the batches it becomes.
    let pushed = vec![json(&budget, 40_000).await, json(&budget, 2_000).await];
    assert_eq!(budget.reserved(), 3 * 42_000);
    let batches = [ids(10), ids(1000)];
    let alive = allocated(&batches[0]) + allocated(&batches[1]);
    assert!(alive > 8_000 && alive < 3 * 40_000);
    let held = shredded(pushed, None, &batches);
    // The text is gone: the pushes hold what the batches keep alive, and nothing was asked of
    // the budget for them.
    assert_eq!((budget.reserved(), budget.peak()), (alive, 3 * 42_000));
    for (held, batch) in held.iter().zip(&batches) {
        assert_eq!(held.allocations.lock().bytes(), allocated(batch));
    }
    // The batches share what holds them until the last is written.
    let mut held = held.into_iter();
    drop(held.next());
    assert_eq!(budget.reserved(), alive);
    drop(held);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test]
async fn shredded_batches_keeping_more_alive_than_was_admitted_hold_what_was_admitted() {
    let budget = MemoryBudget::new(1 << 24);
    let pushed = vec![json(&budget, 100).await, json(&budget, 100).await];
    let batch = ids(1000);
    assert!(allocated(&batch) > 600);
    let held = shredded(pushed, None, std::slice::from_ref(&batch));
    assert_eq!((budget.reserved(), budget.peak()), (600, 600));
    drop(held);
    assert_eq!(budget.reserved(), 0);
    // Permits of another's making are kept as they are.
    let other: Permit = Box::new(7_u8);
    let foreign = shredded(vec![other], None, std::slice::from_ref(&batch));
    assert_eq!(foreign.len(), 1);
    assert_eq!(foreign[0].permits.len(), 1);
}

#[tokio::test]
async fn shredded_batches_hold_what_was_reserved_beyond_their_pushes_for_what_they_keep_alive() {
    let budget = MemoryBudget::new(1 << 24);
    let pushed = vec![json(&budget, 100).await];
    let batch = ids(1000);
    let alive = allocated(&batch);
    let beyond = budget.acquire_working(alive).await.unwrap();
    assert_eq!(budget.reserved(), 300 + alive);
    let held = shredded(pushed, Some(beyond), std::slice::from_ref(&batch));
    // The pushes hold all they were admitted for, and the reservation the rest of what the
    // batch keeps alive; what it reserved beyond that goes back.
    assert_eq!(budget.reserved(), alive);
    assert_eq!(held[0].permits.len(), 1);
    drop(held);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test]
async fn a_piece_reserved_twice_over_gives_half_to_its_frame_in_the_log() {
    let budget = MemoryBudget::new(1 << 20);
    let mut piece = budget.acquire_working(1_000).await.unwrap();
    assert!(super::frame_part(&mut piece, 1).is_none());
    assert_eq!(piece.bytes(), 1_000);
    let frame = super::frame_part(&mut piece, 2).unwrap();
    assert_eq!(
        (piece.bytes(), frame.bytes(), budget.reserved()),
        (500, 500, 1_000)
    );
    // Each is released by whoever holds it: the lane its piece, the log's writer its frame.
    drop(piece);
    assert_eq!(budget.reserved(), 500);
    drop(frame);
    assert_eq!(budget.reserved(), 0);
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
    assert_eq!(prepared.growth(&mut held), buffers(&rows));
    assert_eq!(held.bytes(), buffers(&rows));
    // What a unit holds already is no growth, however many pieces keep it alive.
    assert_eq!(prepared.growth(&mut held), 0);
}

#[test]
fn a_units_parts_grow_it_by_their_lineage_and_their_schemas() {
    let unit = ids(1000);
    let held = Held::of(Vec::new(), std::slice::from_ref(&unit));
    let shape = crate::normalize::Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let parts = crate::normalize::normalize(&unit, &shape).unwrap();
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
                .map(arrow_buffer::Buffer::capacity)
                .sum::<usize>()
                + lineage.root_row.to_data().buffers()[0].capacity()
        })
        .sum();
    // Each part's batch has a schema of its own, which it keeps alive too.
    let schemas: u64 = parts
        .iter()
        .map(|part| rdlt_connector::cost::schema_bytes(&part.batch.schema()))
        .sum();
    let mut allocations = held.allocations.lock();
    let made: u64 = parts
        .iter()
        .map(|part| part_growth(part, &mut allocations))
        .sum();
    assert_eq!(made, u64::try_from(lineage).unwrap() + schemas);
    // Counted once: the parts are held from then on.
    let again: u64 = parts
        .iter()
        .map(|part| part_growth(part, &mut allocations))
        .sum();
    assert_eq!(again, 0);
}

#[test]
fn a_units_parts_are_judged_by_the_path_of_their_table() {
    let shape = crate::normalize::Shape {
        max_depth: 2,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let integers = |values: Vec<i64>| {
        let values: ArrayRef = Arc::new(Int64Array::from(values));
        RecordBatch::try_from_iter([("n", values)]).unwrap()
    };
    let judged = |values: Vec<i64>| {
        let parts = crate::normalize::normalize(&integers(values), &shape).unwrap();
        let rounding = judge(&job(), parts).unwrap();
        let root: Vec<String> = rounding[&Vec::new()]
            .iter()
            .map(ToString::to_string)
            .collect();
        root
    };
    assert_eq!(judged(vec![1_i64 << 60, 1]), ["n"]);
    assert!(judged(vec![1, 2, 3]).is_empty());
}

#[test]
fn a_row_beyond_what_a_request_may_take_is_a_source_error_naming_its_stream() {
    let row = super::pieces::RowTooLarge {
        expanded: 9_000,
        limit: 4_096,
    };
    let error = super::row_too_large(&job(), &row);
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Source, Some("row_exceeds_budget"))
    );
    assert_eq!(error.stream(), Some(&job().stream));
    let said = error.to_string();
    assert!(said.contains("9000") && said.contains("4096"), "{said}");
}

#[test]
fn written_bytes_count_a_slices_own_rows() {
    let whole = ids(1000);
    assert_eq!(written_bytes(&whole), 8_125);
    assert_eq!(written_bytes(&whole.slice(0, 10)), 82);
}

/// A change batch of `rows` inserts of one id each, and `deletes` deletes after them.
fn changes(rows: i64, deletes: i64) -> RecordBatch {
    use rdlt_connector::{ChangeOp, OP_COLUMN, SEQ_COLUMN};
    let all = usize::try_from(rows + deletes).unwrap();
    let ops = (0..rows + deletes).map(|row| {
        if row < rows {
            ChangeOp::Insert.code()
        } else {
            ChangeOp::Delete.code()
        }
    });
    let seqs = arrow_array::FixedSizeBinaryArray::try_from_iter(vec![[7_u8; 16]; all].into_iter());
    let columns: [(&str, ArrayRef); 3] = [
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..rows + deletes)),
        ),
        (
            OP_COLUMN,
            Arc::new(arrow_array::Int8Array::from_iter_values(ops)),
        ),
        (SEQ_COLUMN, Arc::new(seqs.unwrap())),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
fn a_change_stream_s_unit_is_judged_and_cut_where_it_lies() {
    use super::changes::{aligned, data, ignored, split_changes};
    let mode = crate::partition::ChangeMode {
        merge: true,
        deletes: crate::plan::DeleteMode::Ignore,
        truncates: crate::plan::OnTruncate::Ignore,
        partial_updates: false,
    };
    let unit = changes(1_000, 10);
    let held = Held::of(Vec::new(), std::slice::from_ref(&unit));
    // Its data columns are judged as they lie: nothing of the unit is copied for them, and
    // they keep alive only a schema of their own.
    let judged = data(&unit).unwrap();
    assert_eq!(judged.num_columns(), 1);
    let schema = rdlt_connector::cost::schema_bytes(&judged.schema());
    assert_eq!(held.allocations.lock().add(&judged), schema);
    // The rows the stream ignores are counted from their ops, where they lie.
    let counted = ignored(mode, std::slice::from_ref(&unit));
    assert_eq!(
        (counted.deletes, counted.truncates, counted.rows()),
        (10, 0, 10)
    );
    // How the table stores each data column is told by the batch's own columns.
    let stored = rdlt_connector::cost::Stored {
        column: LogicalType::Int64,
        text: false,
        read: false,
    };
    let by_batch = aligned(&unit, &[Some(stored.clone())]);
    assert_eq!(by_batch.len(), 3);
    assert_eq!(by_batch[0].as_ref().map(|stored| stored.text), Some(false));
    assert!(by_batch[1].is_none() && by_batch[2].is_none());
    // A piece is split, and loses the rows its stream ignores, only as it is lowered.
    let stream = StreamName::new("events").unwrap();
    let piece = [unit.slice(1_000, 10), unit.slice(0, 5)];
    let (data, rows) = split_changes(&stream, mode, &piece).unwrap();
    assert_eq!((data.num_rows(), rows.op.len()), (5, 5));
}

#[test]
fn a_unit_whose_every_row_its_stream_ignores_is_counted_and_dropped() {
    let mode = crate::partition::ChangeMode {
        merge: true,
        deletes: crate::plan::DeleteMode::Ignore,
        truncates: crate::plan::OnTruncate::Ignore,
        partial_updates: false,
    };
    let job = PartitionJob {
        changes: Some(mode),
        ..job()
    };
    let mut open = super::super::OpenSegment::new(rdlt_connector::SegmentId(1));
    // What the units before it counted.
    (open.deletes_ignored, open.truncates_ignored) = (2, 3);
    // Seven deletes, the last two of them made truncates.
    let deletes = changes(0, 7);
    let ops = (0..7).map(|row| {
        let op = if row < 5 {
            rdlt_connector::ChangeOp::Delete
        } else {
            rdlt_connector::ChangeOp::Truncate
        };
        op.code()
    });
    let mut columns = deletes.columns().to_vec();
    columns[1] = Arc::new(arrow_array::Int8Array::from_iter_values(ops));
    let unit = RecordBatch::try_new(deletes.schema(), columns).unwrap();
    let held = Held::of(Vec::new(), std::slice::from_ref(&unit));
    let judged = super::judged(&job, &mut open, vec![(vec![unit], held)]).unwrap();
    assert!(judged.is_empty());
    assert_eq!((open.deletes_ignored, open.truncates_ignored), (7, 5));
}

#[test]
fn a_piece_and_a_row_take_their_shares_over_what_is_reserved_for_them() {
    let shares = crate::budget::Shares::of(64 << 20);
    assert_eq!((shares.piece, shares.request), (4 << 20, 16 << 20));
    assert_eq!(super::piece_bounds(shares, 1), (4 << 20, 16 << 20));
    assert_eq!(super::piece_bounds(shares, 2), (2 << 20, 8 << 20));
    // Before its split, a piece's rows as they arrive take half a piece, and a row a quarter of
    // a request: the split makes twice that.
    assert_eq!(super::normalized::split_bounds(shares), (2 << 20, 4 << 20));
}

#[test]
fn eight_pieces_are_lowered_together_at_most() {
    assert!(!super::full(7));
    assert!(super::full(8));
    assert!(super::full(9));
}

/// A piece held in a window: a run that lowers nothing, holding `bytes` of `budget`.
fn windowed(window: &mut super::Window, budget: &MemoryBudget, bytes: u64) {
    let lowering = super::Lowering {
        allocations: Arc::new(parking_lot::Mutex::new(Allocations::default())),
        reserved: budget.try_acquire_working(bytes).unwrap(),
        unit: None,
    };
    let run: super::Run = Box::new(|| Err(crate::error::Error::internal("not lowered")));
    window.push(run, lowering);
}

#[test]
fn a_window_reserves_a_piece_beside_those_it_holds_only_where_the_budget_has_room_at_once() {
    let budget = MemoryBudget::new(64 << 20);
    let mut window = super::Window::default();
    // An empty window reserves nothing: its piece waits for the budget like any request.
    assert!(window.reserve(&budget, 1).is_none());
    windowed(&mut window, &budget, 1 << 20);
    windowed(&mut window, &budget, 1 << 20);
    assert_eq!(window.len(), 2);
    let beside = window.reserve(&budget, 1 << 20).expect("room at once");
    assert_eq!(beside.bytes(), 1 << 20);
    // More than a request may take is never reserved at once.
    assert!(window.reserve(&budget, 17 << 20).is_none());
}

#[test]
fn a_piece_and_a_row_lower_no_more_text_than_a_text_array_s_offsets_reach() {
    // At sixty-four gibibytes a piece's share and a request's are past what 32-bit offsets
    // reach; a piece and a row are held below it whatever the budget.
    let reach = u64::try_from(i32::MAX).unwrap();
    let shares = crate::budget::Shares::of(64 << 30);
    assert!(shares.piece > reach && shares.request > reach);
    for times in [1, 2] {
        let (piece, row) = super::piece_bounds(shares, times);
        assert!(
            piece <= reach && row <= reach,
            "{piece} and {row}, {times} times"
        );
    }
    let cutter = super::allowance::Cutter::new(shares.request, shares.piece, 1);
    assert!(cutter.bounds().0 <= reach && cutter.bounds().1 <= reach);
    // One row of 400 million booleans renders to more text than offsets reach: it is refused
    // before it is lowered, rather than overflowing them.
    let refused = flags_as_text(400_000_000, shares);
    assert!(refused.expanded > reach, "{}", refused.expanded);
    assert_eq!(refused.limit, reach);
    // Under the default budget, forty million take more than a request may.
    let shares = crate::budget::Shares::of(256 << 20);
    let refused = flags_as_text(40_000_000, shares);
    assert!(refused.expanded > shares.request, "{}", refused.expanded);
    assert_eq!(refused.limit, shares.request);
}

/// Cuts one row of `items` booleans, stored as text, by what `shares` lets a piece and a row
/// take: the row's refusal.
fn flags_as_text(items: usize, shares: crate::budget::Shares) -> super::pieces::RowTooLarge {
    let flags = arrow_array::BooleanArray::new(arrow_buffer::BooleanBuffer::new_unset(items), None);
    let item = Arc::new(arrow_schema::Field::new(
        "item",
        arrow_schema::DataType::Boolean,
        true,
    ));
    let list = arrow_array::ListArray::new(
        item,
        arrow_buffer::OffsetBuffer::from_lengths([items]),
        Arc::new(flags),
        None,
    );
    let unit = RecordBatch::try_from_iter([("l", Arc::new(list) as ArrayRef)]).unwrap();
    let logical = LogicalType::List(Box::new(rdlt_connector::Field::new(
        "item",
        LogicalType::Bool,
        true,
    )));
    let stored = rdlt_connector::cost::Stored {
        column: logical,
        text: true,
        read: false,
    };
    let (max, limit) = super::piece_bounds(shares, 1);
    let lowered = super::pieces::Lowered {
        rendering: rdlt_connector::cost::Rendering::text(),
        stored: vec![Some(stored)],
        row: 0,
        item: 0,
        max,
        limit,
    };
    let mut pieces = super::pieces::Pieces::new(vec![unit], lowered);
    pieces.next().unwrap_err()
}
