//! The stage on a flush: the request it holds, what it leaves its write, and that it holds
//! nothing once the flush fails.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray as _;
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{Fields, Schema, SchemaRef};
use rdlt_connector::cost::{arrays, batch_charge};
use rdlt_connector::{ChangeOp, OP_COLUMN, Permit, ReadMode, SEQ_COLUMN, UNCHANGED_COLUMN};

use super::super::held::Held;
use super::super::stage::{fitted, staged};
use super::super::units::Units;
use super::super::{Room, Schemas};
use super::{allocated, buffers, ids, job};
use crate::budget::MemoryBudget;
use crate::compute::Pool;
use crate::env::InlineEnv;
use crate::error::{ErrorKind, codes};
use crate::partition::tests::on_pool;
use crate::partition::{OpenSegment, PartitionJob};
use crate::plan::StreamPlan;
use crate::transform::stage::Stage;
use crate::transform::{Bound, Literal, Op, Transform, TransformError};

/// A filter keeping nothing, or failing, written as an embedder's.
struct Nothing {
    fails: bool,
}

impl Transform for Nothing {
    fn name(&self) -> &'static str {
        "nothing"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        true
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        if self.fails {
            return Err(TransformError::new("refused on purpose"));
        }
        Ok(BooleanArray::from(vec![false; batch.num_rows()]))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }
}

fn budget() -> MemoryBudget {
    MemoryBudget::new(64 << 20, Arc::new(InlineEnv), Duration::from_secs(60))
}

fn staged_job(plan: &StreamPlan) -> PartitionJob {
    let mut job = job();
    job.stage = Stage::new(plan, &[], None).map(Arc::new);
    job
}

/// A unit of `batch`, held with a permit of what its push was charged.
fn unit(budget: &MemoryBudget, batch: RecordBatch) -> Held {
    let pushed: Permit = Box::new(budget.try_acquire_working(batch_charge(&batch)).unwrap());
    Held::of(vec![batch], vec![pushed])
}

/// `units` through `job`'s stage, as its write takes them.
async fn through(
    job: &PartitionJob,
    budget: &MemoryBudget,
    open: &mut OpenSegment,
    units: Units,
) -> Result<Units, crate::error::Error> {
    let context = on_pool(budget.clone(), Pool::inline()).await;
    let stage = job.stage.clone().expect("the job has a stage");
    staged(job, &context, open, &stage, &mut Schemas::default(), units).await
}

#[tokio::test(start_paused = true)]
async fn a_staged_flush_lowers_inside_one_request_holding_what_its_outputs_grew() {
    let budget = budget();
    let text = Literal::Text("x".repeat(100));
    let job = staged_job(&StreamPlan::new(job().stream).transform(Op::constant([("c", text)])));
    let batch = ids(1000);
    let (input, charged) = (batch.clone(), batch_charge(&batch));
    let units = Units {
        units: vec![unit(&budget, batch)],
        room: Room::Budget,
    };
    let staged = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .unwrap();
    let request = budget.shares().request;
    let [output] = &staged.units[0].batches[..] else {
        panic!("one batch fits whole");
    };
    // What the output keeps alive beyond its push's ids, and the array of the column it added.
    let added = output.schema_ref().field_with_name("c").unwrap().clone();
    let added = Fields::from(vec![added]);
    let grown = allocated(output) - buffers(&input) + arrays(&added);
    let Room::Lent(lent) = &staged.room else {
        panic!("a staged flush lowers inside what is lent to it");
    };
    assert!(grown >= 100 * 1000, "{grown}");
    assert_eq!(lent.bytes(), request - grown);
    assert_eq!(budget.reserved(), charged + request);
    drop((staged, input));
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_staged_flush_takes_whole_what_is_lent_to_it() {
    let budget = budget();
    let job = staged_job(&StreamPlan::new(job().stream).transform(Op::rename([("id", "n")])));
    let lent = budget.try_acquire_working(1 << 20).unwrap().lend();
    let units = Units {
        units: vec![unit(&budget, ids(10))],
        room: Room::Lent(lent.clone()),
    };
    let charged = budget.reserved();
    let staged = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .unwrap();
    assert!(lent.try_take(1).is_none(), "the flush's room lends no more");
    let Room::Lent(room) = &staged.room else {
        panic!("a staged flush lowers inside what is lent to it");
    };
    assert!(room.bytes() <= 1 << 20);
    assert_eq!(budget.reserved(), charged);
    drop((staged, lent));
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_rename_holds_no_array_its_push_holds_already() {
    let budget = budget();
    let job = staged_job(&StreamPlan::new(job().stream).transform(Op::rename([("id", "n")])));
    let batch = ids(1000);
    let input = batch.clone();
    let units = Units {
        units: vec![unit(&budget, batch)],
        room: Room::Budget,
    };
    let staged = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .unwrap();
    let Room::Lent(lent) = &staged.room else {
        panic!("a staged flush lowers inside what is lent to it");
    };
    // The output's ids are the push's own array: only its schema is new.
    let output = &staged.units[0].batches[0];
    assert!(Arc::ptr_eq(output.column(0), input.column(0)));
    let grown = allocated(output) - buffers(&input);
    assert_eq!(budget.shares().request - lent.bytes(), grown);
}

/// A change batch of `rows` updates of an id and a name, each leaving its name unchanged where
/// `flagged` says it carries flags.
fn updates(rows: i64, flagged: bool) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let names = (0..rows).map(|row| format!("n{row}"));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(names));
    let ops: ArrayRef = Arc::new(Int8Array::from(vec![
        ChangeOp::Update.code();
        usize::try_from(rows).unwrap()
    ]));
    let positions = (0..rows).map(|row| {
        let mut seq = [0_u8; 16];
        seq[8..].copy_from_slice(&row.to_be_bytes());
        seq
    });
    let seqs: ArrayRef = Arc::new(FixedSizeBinaryArray::try_from_iter(positions).unwrap());
    let mut columns = vec![
        ("id", ids),
        ("name", names),
        (OP_COLUMN, ops),
        (SEQ_COLUMN, seqs),
    ];
    if flagged {
        let bitmap: &[u8] = &[0b0000_0010];
        let flags = BinaryArray::from(vec![Some(bitmap); usize::try_from(rows).unwrap()]);
        columns.push((UNCHANGED_COLUMN, Arc::new(flags)));
    }
    RecordBatch::try_from_iter(columns).unwrap()
}

#[tokio::test(start_paused = true)]
async fn an_included_change_stream_holds_a_request_only_for_flags_it_moves() {
    let plan = StreamPlan::new(job().stream)
        .read(ReadMode::Cdc)
        .include_only(["id"]);
    for flagged in [false, true] {
        let budget = budget();
        let job = staged_job(&plan);
        let batch = updates(10, flagged);
        let charged = batch_charge(&batch);
        let units = Units {
            units: vec![unit(&budget, batch)],
            room: Room::Budget,
        };
        let staged = through(&job, &budget, &mut OpenSegment::default(), units)
            .await
            .unwrap();
        assert_eq!(staged.units[0].batches[0].num_rows(), 10, "{flagged}");
        let held = budget.reserved();
        let request = budget.shares().request;
        if flagged {
            assert!(matches!(staged.room, Room::Lent(_)));
            assert_eq!(held, charged + request);
        } else {
            assert!(matches!(staged.room, Room::Budget));
            assert_eq!(held, charged);
        }
    }
}

/// The stream `events` read as changes, keeping only its names.
fn names_only() -> StreamPlan {
    StreamPlan::new(job().stream)
        .read(ReadMode::Cdc)
        .include_only(["name"])
}

/// Each row's unchanged flags in `batch`, which carries them.
fn flags_of(batch: &RecordBatch) -> Vec<Vec<u8>> {
    let flags = batch
        .column_by_name(UNCHANGED_COLUMN)
        .unwrap()
        .as_binary::<i32>();
    flags
        .iter()
        .map(|bitmap| bitmap.unwrap().to_vec())
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_unit_of_flagged_and_unflagged_batches_is_reserved_whole_and_its_flags_moved() {
    let budget = budget();
    let job = staged_job(&names_only());
    let batches = vec![updates(3, true), updates(2, false)];
    let charged: u64 = batches.iter().map(batch_charge).sum();
    let pushed: Permit = Box::new(budget.try_acquire_working(charged).unwrap());
    let units = Units {
        units: vec![Held::of(batches, vec![pushed])],
        room: Room::Budget,
    };
    let staged = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .unwrap();
    assert!(matches!(staged.room, Room::Lent(_)));
    assert_eq!(budget.reserved(), charged + budget.shares().request);
    let [flagged, unflagged] = &staged.units[0].batches[..] else {
        panic!("both batches are kept, in order");
    };
    // The name was the second column; kept alone, it is the first.
    assert_eq!(flags_of(flagged), vec![vec![0b0000_0001]; 3]);
    assert!(unflagged.column_by_name(UNCHANGED_COLUMN).is_none());
    assert_eq!(unflagged.num_rows(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_flush_reserves_for_the_unit_whose_flags_move_and_projects_the_other() {
    let budget = budget();
    let job = staged_job(&names_only());
    let (flagged, unflagged) = (updates(3, true), updates(2, false));
    let charged = batch_charge(&flagged) + batch_charge(&unflagged);
    let input = unflagged.clone();
    let units = Units {
        units: vec![unit(&budget, flagged), unit(&budget, unflagged)],
        room: Room::Budget,
    };
    let staged = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .unwrap();
    assert!(matches!(staged.room, Room::Lent(_)));
    assert_eq!(budget.reserved(), charged + budget.shares().request);
    let [reserved, projected] = &staged.units[..] else {
        panic!("both units are kept, in order");
    };
    // The moved flags are held with their unit; the other unit holds only its push.
    assert_eq!(reserved.permits.len(), 2);
    assert_eq!(projected.permits.len(), 1);
    let names = projected.batches[0].column_by_name("name").unwrap();
    assert!(Arc::ptr_eq(names, input.column_by_name("name").unwrap()));
}

#[tokio::test(start_paused = true)]
async fn a_filter_dropping_every_row_of_a_flush_leaves_no_unit_and_lets_go_of_its_pushes() {
    let budget = budget();
    let nothing: Arc<dyn Transform> = Arc::new(Nothing { fails: false });
    let job = staged_job(&StreamPlan::new(job().stream).transform(nothing));
    let units = Units {
        units: vec![unit(&budget, ids(100))],
        room: Room::Budget,
    };
    let mut open = OpenSegment::default();
    let staged = through(&job, &budget, &mut open, units).await.unwrap();
    assert!(staged.units.is_empty());
    assert_eq!((open.filtered.dropped, open.filtered.deleted), (100, 0));
    assert_eq!(budget.reserved(), budget.shares().request);
    drop(staged);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_transform_failing_on_a_flush_fails_it_typed_and_leaves_nothing_reserved() {
    let budget = budget();
    let failing: Arc<dyn Transform> = Arc::new(Nothing { fails: true });
    let job = staged_job(&StreamPlan::new(job().stream).transform(failing));
    let units = Units {
        units: vec![unit(&budget, ids(100))],
        room: Room::Budget,
    };
    let failed = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .err()
        .expect("the transform fails");
    assert_eq!(
        (failed.kind(), failed.code()),
        (ErrorKind::Transform, Some(codes::TRANSFORM_FAILED))
    );
    assert!(!failed.is_retryable());
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_schema_its_transforms_refuse_fails_the_flush_and_leaves_nothing_reserved() {
    let budget = budget();
    let constant = Op::constant([("id", Literal::Integer(1))]);
    let job = staged_job(&StreamPlan::new(job().stream).transform(constant));
    let units = Units {
        units: vec![unit(&budget, ids(10))],
        room: Room::Budget,
    };
    let failed = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .err()
        .expect("the transform refuses the schema");
    assert_eq!(
        (failed.kind(), failed.code()),
        (ErrorKind::Config, Some(codes::TRANSFORM_SCHEMA))
    );
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_stage_that_only_includes_reserves_nothing_more() {
    let budget = budget();
    let job = staged_job(&StreamPlan::new(job().stream).include_only(["id"]));
    let batch = ids(10);
    let charged = batch_charge(&batch);
    let units = Units {
        units: vec![unit(&budget, batch)],
        room: Room::Budget,
    };
    let staged = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .unwrap();
    assert!(matches!(staged.room, Room::Budget));
    assert_eq!(staged.units[0].batches[0].num_rows(), 10);
    assert_eq!(budget.reserved(), charged);
}

#[tokio::test(start_paused = true)]
async fn a_unit_that_arrived_with_no_rows_is_kept_in_the_stage_s_schema() {
    let plain = || StreamPlan::new(job().stream);
    let cases = [
        (plain().include_only(["id"]), "id"),
        (plain().transform(Op::rename([("id", "n")])), "n"),
    ];
    for (plan, column) in cases {
        let budget = budget();
        let job = staged_job(&plan);
        let units = Units {
            units: vec![unit(&budget, ids(0))],
            room: Room::Budget,
        };
        let staged = through(&job, &budget, &mut OpenSegment::default(), units)
            .await
            .unwrap();
        let [unit] = &staged.units[..] else {
            panic!("{column}: the unit is kept");
        };
        let [batch] = &unit.batches[..] else {
            panic!("{column}: one batch of no rows");
        };
        assert_eq!(batch.num_rows(), 0, "{column}");
        assert!(batch.column_by_name(column).is_some(), "{column}");
    }
}

/// A transform passing its input on, declaring a bound of `per_row` bytes a row, far above
/// what it allocates.
struct Bounded {
    per_row: u64,
}

impl Transform for Bounded {
    fn name(&self) -> &'static str {
        "bounded"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }

    fn bound(&self, _input: &Schema) -> Option<Bound> {
        Some(Bound::new(0).with_rows(self.per_row))
    }
}

#[tokio::test(start_paused = true)]
async fn a_batch_the_stage_cuts_holds_the_buffers_its_parts_share_once() {
    let budget = MemoryBudget::new(1 << 20, Arc::new(InlineEnv), Duration::from_secs(60));
    let request = budget.shares().request;
    let bounded: Arc<dyn Transform> = Arc::new(Bounded { per_row: 40 });
    let job = staged_job(&StreamPlan::new(job().stream).transform(bounded));
    // The bound of all the rows passes the request many times over, so the batch is cut.
    let batch = ids(30_000);
    let (charged, shared) = (batch_charge(&batch), buffers(&batch));
    let units = Units {
        units: vec![unit(&budget, batch)],
        room: Room::Budget,
    };
    let staged = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .expect("every part fits, and the flush is not refused");
    assert!(staged.units[0].batches.len() > 2);
    let Room::Lent(lent) = &staged.room else {
        panic!("a staged flush lowers inside what is lent to it");
    };
    // Every part shares the input's ids, which its pushes were charged for.
    let grown = request - lent.bytes();
    assert!(grown < shared, "{grown} of {shared}");
    assert_eq!(budget.reserved(), charged + request);
}

#[tokio::test(start_paused = true)]
async fn batches_of_one_schema_share_one_chain() {
    let budget = budget();
    let job = staged_job(&StreamPlan::new(job().stream).transform(Op::rename([("id", "n")])));
    let units = Units {
        units: vec![
            Held::of(vec![ids(1), ids(2)], Vec::new()),
            Held::of(vec![ids(3)], Vec::new()),
        ],
        room: Room::Budget,
    };
    let staged = through(&job, &budget, &mut OpenSegment::default(), units)
        .await
        .unwrap();
    let schemas: Vec<SchemaRef> = staged
        .units
        .iter()
        .flat_map(|unit| unit.batches.iter().map(RecordBatch::schema))
        .collect();
    assert_eq!(schemas.len(), 3);
    assert!(
        schemas
            .iter()
            .all(|schema| Arc::ptr_eq(schema, &schemas[0]))
    );
}

#[test]
fn a_unit_that_grew_beyond_what_is_left_of_its_request_is_refused() {
    let stream = job().stream;
    for (transform, kind) in [(Some("t"), ErrorKind::Transform), (None, ErrorKind::Source)] {
        assert!(fitted(&stream, transform, 10, 10).is_ok());
        let refused = fitted(&stream, transform, 11, 10).expect_err("eleven bytes pass ten");
        assert_eq!(
            (refused.kind(), refused.code()),
            (kind, Some(codes::ROW_EXCEEDS_BUDGET))
        );
    }
}
