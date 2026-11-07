use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{Int8Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int8Array, Int64Array,
    RecordBatch, StringArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rdlt_connector::cost::arrays;
use rdlt_connector::{
    ChangeOp, ColumnPath, OP_COLUMN, ReadMode, SEQ_COLUMN, StreamName, UNCHANGED_COLUMN,
};

use super::{Breach, Ran, Refusal, Stage};
use crate::error::{ErrorKind, codes};
use crate::plan::{StreamPlan, WriteMode};
use crate::transform::op::ARRAY_ROOM;
use crate::transform::{
    Bound, Literal, Mask, Op, Transform, TransformError, allocated_beyond, column_bytes,
};

fn stream() -> StreamPlan {
    StreamPlan::new(StreamName::new("s").unwrap())
}

fn people(ids: &[i64]) -> RecordBatch {
    let names: Vec<String> = ids.iter().map(|id| format!("n{id}")).collect();
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
        (
            "name",
            Arc::new(StringArray::from(names.clone())) as ArrayRef,
        ),
        ("note", Arc::new(StringArray::from(names)) as ArrayRef),
    ])
    .unwrap()
}

/// A change row: its id, its op, and the flags it carries.
type Row<'a> = (Option<i64>, ChangeOp, Option<&'a [u8]>);

/// A change batch of `rows`.
fn changes(rows: &[Row<'_>]) -> RecordBatch {
    let ids: Int64Array = rows.iter().map(|row| row.0).collect();
    let names: StringArray = rows
        .iter()
        .map(|row| row.0.map(|id| format!("n{id}")))
        .collect();
    let ops = Int8Array::from(rows.iter().map(|row| row.1.code()).collect::<Vec<_>>());
    let seqs = FixedSizeBinaryArray::try_from_iter((0..rows.len()).map(|row| {
        let mut seq = [0_u8; 16];
        seq[15] = u8::try_from(row).unwrap();
        seq
    }))
    .unwrap();
    let flags: BinaryArray = rows.iter().map(|row| row.2).collect();
    RecordBatch::try_from_iter([
        ("id", Arc::new(ids) as ArrayRef),
        ("name", Arc::new(names) as ArrayRef),
        (OP_COLUMN, Arc::new(ops) as ArrayRef),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (UNCHANGED_COLUMN, Arc::new(flags) as ArrayRef),
    ])
    .unwrap()
}

fn ran(plan: &StreamPlan, key: &[&str], batch: RecordBatch, free: u64) -> Result<Ran, Refusal> {
    let key: Vec<ColumnPath> = key.iter().copied().map(ColumnPath::from).collect();
    let stage = Stage::new(plan, &key, None).expect("the plan has a stage");
    let chain = stage.chain(batch.schema_ref())?;
    stage.run(&chain, batch, free)
}

fn run(
    plan: &StreamPlan,
    key: &[&str],
    batch: RecordBatch,
    free: u64,
) -> Result<Vec<RecordBatch>, Refusal> {
    Ok(ran(plan, key, batch, free)?.batches)
}

/// The rows the stage left.
fn rows(ran: &Ran) -> usize {
    ran.batches.iter().map(RecordBatch::num_rows).sum()
}

fn names(batch: &RecordBatch) -> Vec<&str> {
    let fields = batch.schema_ref().fields().iter();
    fields.map(|field| field.name().as_str()).collect()
}

fn flags_of(batch: &RecordBatch) -> Vec<u8> {
    let flags = batch.column_by_name(UNCHANGED_COLUMN).unwrap();
    flags.as_binary::<i32>().value(0).to_vec()
}

/// A transform passing its input on and declaring no lineage, written as an embedder's.
struct Unlined;

impl Transform for Unlined {
    fn name(&self) -> &'static str {
        "unlined"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }
}

/// A transform passing its input on and declaring a lineage naming a column it lacks.
struct Overlined;

impl Transform for Overlined {
    fn name(&self) -> &'static str {
        "overlined"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }

    fn lineage(&self, input: &Schema) -> Option<Vec<Vec<usize>>> {
        Some(vec![vec![input.fields().len()]; input.fields().len()])
    }
}

fn ids(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    let ids = batches.iter().flat_map(|batch| {
        let column = batch.column_by_name("id").unwrap();
        column
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>()
    });
    ids.collect()
}

fn ops(batches: &[RecordBatch]) -> Vec<(Option<i64>, i8)> {
    let mut ops = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let codes = batch
            .column_by_name(OP_COLUMN)
            .unwrap()
            .as_primitive::<Int8Type>();
        ops.extend(ids.iter().zip(codes.values().iter().copied()));
    }
    ops
}

/// A filter answering each row as `answers` says, written as an embedder's.
struct Answers(Vec<Option<bool>>);

impl Transform for Answers {
    fn name(&self) -> &'static str {
        "answers"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        true
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        Ok(self.0.iter().copied().take(batch.num_rows()).collect())
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }
}

/// A filter keeping the rows whose id is even, nulls never, saying it reads the columns
/// `reads` names and that each column it outputs reads itself where `lined`.
struct Even {
    reads: Option<Vec<usize>>,
    lined: bool,
}

const EVEN: Even = Even {
    reads: None,
    lined: false,
};

impl Transform for Even {
    fn name(&self) -> &'static str {
        "even"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        true
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        let ids = batch
            .column_by_name("id")
            .ok_or_else(|| TransformError::new("no id"))?;
        let ids = ids.as_primitive::<Int64Type>();
        Ok(ids.iter().map(|id| id.map(|id| id % 2 == 0)).collect())
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }

    fn lineage(&self, input: &Schema) -> Option<Vec<Vec<usize>>> {
        let columns = 0..input.fields().len();
        self.lined
            .then(|| columns.map(|column| vec![column]).collect())
    }

    fn keep_reads(&self, _input: &Schema) -> Option<Vec<usize>> {
        self.reads.clone()
    }
}

/// How a transform breaks its contract.
#[derive(Clone, Copy)]
enum Breaks {
    Rows,
    Schema,
    Keep,
    Allocates,
    Fails,
}

/// A transform that breaks its contract as it says, declaring `bound`.
struct Broken(Breaks, Option<Bound>);

impl Transform for Broken {
    fn name(&self) -> &'static str {
        "broken"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        let mut fields: Vec<_> = input.fields().iter().cloned().collect();
        if matches!(self.0, Breaks::Allocates) {
            fields.push(Arc::new(Field::new("pad", DataType::Int64, false)));
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    fn filters(&self) -> bool {
        matches!(self.0, Breaks::Keep)
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        Ok(BooleanArray::from(vec![true; batch.num_rows() + 1]))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        let rows = batch.num_rows();
        match self.0 {
            Breaks::Rows => Ok(batch.slice(0, rows.saturating_sub(1))),
            Breaks::Schema => Ok(batch.project(&[0]).unwrap()),
            Breaks::Keep => Ok(batch),
            Breaks::Allocates => {
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(Int64Array::from(vec![0_i64; rows])));
                Ok(RecordBatch::try_new(self.output(batch.schema_ref())?, columns).unwrap())
            }
            Breaks::Fails => Err(TransformError::new("refused on purpose").with_column("name")),
        }
    }

    fn bound(&self, _input: &Schema) -> Option<Bound> {
        self.1.clone()
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

fn code(transform: impl Transform) -> Arc<dyn Transform> {
    Arc::new(transform)
}

#[test]
fn a_stream_that_includes_every_column_and_transforms_nothing_has_no_stage() {
    assert!(Stage::new(&stream(), &[], None).is_none());
}

#[test]
fn a_stage_holds_bytes_of_lowering_where_it_transforms_or_moves_flags() {
    let flagged: &[u8] = &[0b0000_0010];
    let with_flags = changes(&[(Some(1), ChangeOp::Update, Some(flagged))]).schema();
    let without_flags = Arc::new(with_flags.project(&[0, 1, 2, 3]).unwrap());
    let reserves = |plan: StreamPlan, input: &Schema| {
        let stage = Stage::new(&plan, &[], None).unwrap();
        stage.reserves(input)
    };
    let logged = || stream().read(ReadMode::Cdc).include_only(["id"]);
    assert!(!reserves(logged(), &without_flags));
    assert!(reserves(logged(), &with_flags));
    assert!(!reserves(stream().include_only(["id"]), &with_flags));
    let renamed = stream().transform(Op::rename([("a", "b")]));
    assert!(reserves(renamed, &without_flags));
}

#[test]
fn a_stage_names_the_transform_whose_output_it_leaves() {
    let plan = stream()
        .transform(Op::rename([("a", "b")]))
        .transform(Op::mask(["b"], Mask::Null));
    let last = |plan: &StreamPlan| {
        let stage = Stage::new(plan, &[], None).unwrap();
        stage.last_transform().map(str::to_owned)
    };
    assert_eq!(last(&plan).as_deref(), Some("mask"));
    assert_eq!(last(&stream().include_only(["id"])), None);
}

#[test]
fn include_keeps_its_columns_and_the_key_as_they_arrive_sharing_their_arrays() {
    let input = people(&[1, 2]);
    let output = run(
        &stream().include_only(["note"]),
        &["id"],
        input.clone(),
        u64::MAX,
    )
    .unwrap();
    assert_eq!(names(&output[0]), ["id", "note"]);
    assert!(Arc::ptr_eq(output[0].column(0), input.column(0)));
    assert!(Arc::ptr_eq(output[0].column(1), input.column(2)));
}

#[test]
fn include_keeps_the_change_time_the_stage_is_timed_by() {
    let plan = stream().include_only(["note"]);
    let time = ColumnPath::from("name");
    let stage = Stage::new(&plan, &[], Some(&time)).unwrap();
    let input = people(&[1]);
    let chain = stage.chain(input.schema_ref()).unwrap();
    let output = stage.run(&chain, input, u64::MAX).unwrap();
    assert_eq!(names(&output.batches[0]), ["name", "note"]);
    assert_eq!(chain.data_output().fields().len(), 2);
    assert_eq!(**chain.input(), *people(&[1]).schema());
}

#[test]
fn a_filter_drops_what_it_does_not_keep_before_the_next_transform_runs() {
    let plan = stream()
        .transform(code(EVEN))
        .transform(Op::constant([("c", Literal::Integer(1))]));
    let output = run(&plan, &[], people(&[1, 2, 3, 4, 5, 6]), u64::MAX).unwrap();
    assert_eq!(ids(&output), [Some(2), Some(4), Some(6)]);
    assert!(
        output
            .iter()
            .all(|batch| batch.column_by_name("c").is_some())
    );
}

#[test]
fn a_null_answer_keeps_nothing() {
    let plan = stream().transform(code(Answers(vec![Some(true), None, Some(false)])));
    let output = run(&plan, &[], people(&[1, 2, 3]), u64::MAX).unwrap();
    assert_eq!(ids(&output), [Some(1)]);
}

#[test]
fn a_filter_keeping_every_row_passes_the_batch_on_whole() {
    let plan = stream().transform(code(Answers(vec![Some(true); 3])));
    let input = people(&[1, 2, 3]);
    let output = ran(&plan, &[], input.clone(), u64::MAX).unwrap();
    assert_eq!((output.dropped, output.deleted), (0, 0));
    assert!(Arc::ptr_eq(output.batches[0].column(1), input.column(1)));
}

#[test]
fn a_filter_dropping_every_row_leaves_no_batch() {
    let plan = stream().transform(code(Answers(vec![Some(false); 3])));
    let output = ran(&plan, &[], people(&[1, 2, 3]), u64::MAX).unwrap();
    assert!(output.batches.is_empty());
    assert_eq!(output.dropped, 3);
}

/// A filter keeping every row by its values, but null, and so keeping nothing, where `valid`
/// says a row is not: its nulls lie over values that are true.
struct NullOverTrue(Vec<bool>);

impl Transform for NullOverTrue {
    fn name(&self) -> &'static str {
        "null_over_true"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        true
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        let rows = batch.num_rows();
        let nulls = NullBuffer::from(self.0[..rows].to_vec());
        Ok(BooleanArray::new(BooleanBuffer::new_set(rows), Some(nulls)))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }
}

#[test]
fn a_null_answer_turns_a_merged_upsert_into_a_delete_whatever_value_lies_under_it() {
    let filter = code(NullOverTrue(vec![true, false, false]));
    let plan = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(filter);
    let batch = changes(&[
        (Some(1), ChangeOp::Insert, None),
        (Some(2), ChangeOp::Update, None),
        (Some(3), ChangeOp::Insert, None),
    ]);
    let output = ran(&plan, &["id"], batch, u64::MAX).unwrap();
    let delete = ChangeOp::Delete.code();
    assert_eq!(
        ops(&output.batches),
        [
            (Some(1), ChangeOp::Insert.code()),
            (Some(2), delete),
            (Some(3), delete)
        ]
    );
    assert_eq!(output.deleted, 2);
}

#[test]
fn a_chain_runs_each_transform_on_the_last_ones_output() {
    let plan = stream()
        .transform(Op::rename([("note", "remark")]))
        .transform(Op::mask(["remark"], Mask::redact("x")));
    let output = run(&plan, &[], people(&[1]), u64::MAX).unwrap();
    let remark = output[0].column_by_name("remark").unwrap();
    assert_eq!(remark.as_string::<i32>().value(0), "x");
}

#[test]
fn a_breach_is_the_transforms_where_it_declares_no_bound_and_the_engines_where_it_does() {
    let cases = [
        (
            Breaks::Rows,
            Breach::Rows {
                input: 3,
                output: 2,
            },
        ),
        (Breaks::Schema, Breach::Schema),
        (
            Breaks::Keep,
            Breach::Keep {
                rows: 3,
                answered: 4,
            },
        ),
    ];
    for (breaks, expected) in cases {
        for bound in [None, Some(Bound::new(1 << 30))] {
            let bounded = bound.is_some();
            let plan = stream().transform(code(Broken(breaks, bound)));
            match run(&plan, &[], people(&[1, 2, 3]), u64::MAX) {
                Err(Refusal::Contract {
                    bounded: seen,
                    breach,
                    ..
                }) => {
                    assert_eq!((seen, breach), (bounded, expected));
                }
                other => panic!("{other:?}"),
            }
        }
    }
}

#[test]
fn output_beyond_the_bound_its_transform_declares_is_a_bound_breach() {
    let plan = stream().transform(code(Broken(Breaks::Allocates, Some(Bound::new(0)))));
    let refused = run(&plan, &[], people(&[1, 2, 3]), u64::MAX).unwrap_err();
    assert!(
        matches!(refused, Refusal::Bound { allocated, bound: 0, .. } if allocated > 0),
        "{refused:?}"
    );
}

#[test]
fn output_within_the_bound_its_transform_declares_passes() {
    let plan = stream().transform(code(Broken(Breaks::Allocates, Some(Bound::new(1 << 20)))));
    let output = run(&plan, &[], people(&[1, 2, 3]), u64::MAX).unwrap();
    assert_eq!(names(&output[0]), ["id", "name", "note", "pad"]);
}

#[test]
fn a_failing_transform_is_refused_with_its_error() {
    let plan = stream().transform(code(Broken(Breaks::Fails, None)));
    match run(&plan, &[], people(&[1]), u64::MAX) {
        Err(Refusal::Failed { transform, error }) => {
            assert_eq!(
                (transform.as_str(), error.column()),
                ("broken", Some("name"))
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_batch_whose_bound_passes_the_room_is_cut_into_rows_that_fit_in_order() {
    let plan = stream().transform(code(Bounded { per_row: 1000 }));
    let all: Vec<i64> = (0..64).collect();
    let output = run(&plan, &[], people(&all), 20_000).unwrap();
    assert!(output.len() > 1, "{}", output.len());
    let expected: Vec<Option<i64>> = all.iter().copied().map(Some).collect();
    assert_eq!(ids(&output), expected);
}

#[test]
fn a_batch_whose_bound_fits_the_room_is_run_whole() {
    let plan = stream().transform(code(Bounded { per_row: 1000 }));
    let all: Vec<i64> = (0..64).collect();
    let output = run(&plan, &[], people(&all), 1 << 20).unwrap();
    assert_eq!(output.len(), 1);
}

/// A filter keeping every row, declaring a bound of nothing beyond what filtering holds.
struct Sieve;

impl Transform for Sieve {
    fn name(&self) -> &'static str {
        "sieve"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        true
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }

    fn bound(&self, _input: &Schema) -> Option<Bound> {
        Some(Bound::new(0))
    }
}

/// A batch of `rows` ids, each with a note of a thousand bytes.
fn noted(rows: i64) -> RecordBatch {
    let notes = (0..rows).map(|_| "x".repeat(1000));
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef,
        ),
        (
            "note",
            Arc::new(StringArray::from_iter_values(notes)) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn a_filter_is_held_to_room_for_the_rows_it_copies() {
    let plan = stream().transform(code(Sieve));
    let all: Vec<i64> = (0..64).collect();
    // Each note holds a thousand bytes, which a filter's copy of its row may take again.
    let output = run(&plan, &[], noted(64), 50_000).unwrap();
    assert!(output.len() > 1, "{}", output.len());
    let expected: Vec<Option<i64>> = all.into_iter().map(Some).collect();
    assert_eq!(ids(&output), expected);
}

#[test]
fn code_is_measured_after_it_runs_not_held_to_room_before() {
    // The filter keeps every row, so it copies none of the notes a room asked for would hold.
    let plan = stream().transform(code(Answers(vec![Some(true); 64])));
    assert_eq!(run(&plan, &[], noted(64), 10_000).unwrap().len(), 1);
}

#[test]
fn a_part_whose_room_is_just_what_is_left_runs_whole() {
    let plan = stream().transform(code(Bounded { per_row: 1000 }));
    let all: Vec<i64> = (0..64).collect();
    let room = 1000 * 64 + arrays(people(&all).schema_ref().fields());
    assert_eq!(run(&plan, &[], people(&all), room).unwrap().len(), 1);
    assert!(run(&plan, &[], people(&all), room - 1).unwrap().len() > 1);
}

#[test]
fn a_row_alone_beyond_the_room_is_refused_for_what_it_asks() {
    let plan = stream().transform(code(Bounded { per_row: 1000 }));
    let input = people(&[1]);
    let room = 1000 + arrays(input.schema_ref().fields());
    let refused = run(&plan, &[], input, room - 1).unwrap_err();
    assert!(
        matches!(refused, Refusal::Row { asked, .. } if asked == room),
        "{refused:?}"
    );
}

#[test]
fn code_whose_output_holds_just_what_is_left_runs_whole() {
    let plan = stream().transform(code(Unlined));
    let held = arrays(people(&[1, 2]).schema_ref().fields());
    assert_eq!(run(&plan, &[], people(&[1, 2]), held).unwrap().len(), 1);
    let refused = run(&plan, &[], people(&[1, 2]), held - 1).unwrap_err();
    assert!(matches!(refused, Refusal::Row { .. }), "{refused:?}");
}

#[test]
fn output_just_at_the_bound_its_transform_declares_passes() {
    let input = people(&[1, 2, 3]);
    let made = Broken(Breaks::Allocates, None)
        .apply(input.clone())
        .unwrap();
    let exact = allocated_beyond(&made, &input);
    let plan = stream().transform(code(Broken(Breaks::Allocates, Some(Bound::new(exact)))));
    assert_eq!(run(&plan, &[], input.clone(), u64::MAX).unwrap().len(), 1);
    let below = stream().transform(code(Broken(Breaks::Allocates, Some(Bound::new(exact - 1)))));
    let refused = run(&below, &[], input, u64::MAX).unwrap_err();
    assert!(matches!(refused, Refusal::Bound { .. }), "{refused:?}");
}

/// A transform swapping its input's first two columns, saying each reads the other.
struct Swap;

impl Transform for Swap {
    fn name(&self) -> &'static str {
        "swap"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        let mut fields: Vec<_> = input.fields().iter().cloned().collect();
        fields.swap(0, 1);
        Ok(Arc::new(Schema::new(fields)))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        let mut columns = batch.columns().to_vec();
        columns.swap(0, 1);
        Ok(RecordBatch::try_new(self.output(batch.schema_ref())?, columns).unwrap())
    }

    fn lineage(&self, input: &Schema) -> Option<Vec<Vec<usize>>> {
        let mut lineage: Vec<Vec<usize>> = (0..input.fields().len()).map(|n| vec![n]).collect();
        lineage.swap(0, 1);
        Some(lineage)
    }
}

#[test]
fn flags_move_with_the_columns_a_lineage_says_they_became() {
    let plan = stream().read(ReadMode::Cdc).transform(code(Swap));
    let flagged: &[u8] = &[0b0000_0010];
    let batch = changes(&[(Some(1), ChangeOp::Update, Some(flagged))]);
    let output = run(&plan, &[], batch, u64::MAX).unwrap();
    assert_eq!(names(&output[0])[..2], ["name", "id"]);
    // The name, flagged as the second column, is the first once swapped.
    assert_eq!(flags_of(&output[0]), [0b0000_0001]);
}

#[test]
fn flags_that_flag_nothing_pass_a_lineage_as_they_are() {
    let plan = stream()
        .read(ReadMode::Cdc)
        .transform(Op::constant([("c", Literal::Integer(1))]));
    let empty: &[u8] = &[];
    let batch = changes(&[
        (Some(1), ChangeOp::Update, Some(empty)),
        (Some(2), ChangeOp::Update, None),
    ]);
    let output = run(&plan, &[], batch, u64::MAX).unwrap();
    let flags = output[0].column_by_name(UNCHANGED_COLUMN).unwrap();
    let flags = flags.as_binary::<i32>();
    assert_eq!((flags.value(0), flags.is_null(1)), (empty, true));
}

#[test]
fn moved_flags_are_new_where_the_other_change_columns_are_shared() {
    let flagged: &[u8] = &[0b0000_0010];
    let batch = changes(&[(Some(1), ChangeOp::Update, Some(flagged))]);
    let unflagged = batch.project(&[0, 1, 2, 3]).unwrap();
    let beyond = |batch: &RecordBatch| {
        let stage = Stage::new(
            &stream().read(ReadMode::Cdc).include_only(["name"]),
            &[],
            None,
        );
        let stage = stage.unwrap();
        let chain = stage.chain(batch.schema_ref()).unwrap();
        let (_, changes) = super::changes::split(batch, &chain).unwrap();
        changes.expect("a change batch").beyond(batch)
    };
    assert!(beyond(&batch) > 1);
    assert_eq!(beyond(&unflagged), 0);
}

#[test]
fn moving_flags_asks_for_room_before_it_runs() {
    let plan = stream().read(ReadMode::Cdc).include_only(["name"]);
    let flagged: &[u8] = &[0b0000_0010];
    let batch = || changes(&[(Some(1), ChangeOp::Update, Some(flagged))]);
    let refused = run(&plan, &[], batch(), 100).unwrap_err();
    assert!(
        matches!(refused, Refusal::Row { transform: None, asked, free: 100 } if asked > 100),
        "{refused:?}"
    );
    assert_eq!(run(&plan, &[], batch(), 1 << 20).unwrap().len(), 1);
    let unflagged = batch().project(&[0, 1, 2, 3]).unwrap();
    assert_eq!(run(&plan, &[], unflagged, 0).unwrap().len(), 1);
}

#[test]
fn the_room_a_step_asks_counts_the_stage_s_own_work_on_its_rows() {
    let flagged: &[u8] = &[0b0000_0010];
    let rows: Vec<Row<'_>> = (0..20)
        .map(|id| (Some(id), ChangeOp::Update, Some(flagged)))
        .collect();
    let batch = changes(&rows);
    let key = [ColumnPath::from("id")];
    let room = |plan: StreamPlan, flags: bool| {
        let stage = Stage::new(&plan, &key, None).unwrap();
        let chain = stage.chain(batch.schema_ref()).unwrap();
        let (data, changes) = super::changes::split(&batch, &chain).unwrap();
        let stepping = (
            stage.steps[0].as_transform(),
            &chain.declared[0],
            stage.rule,
        );
        let changes = changes.filter(|_| flags);
        super::room::step_room(stepping, &data, changes.as_ref(), &chain.schemas[1])
    };
    let even = |lined: bool| {
        code(Even {
            reads: Some(vec![0]),
            lined,
        })
    };
    let merged = || stream().read(ReadMode::Cdc).write(WriteMode::Merge);
    let logged = || stream().read(ReadMode::Cdc);
    let rows = 20;
    // A step that filters nothing asks for its output's arrays alone.
    let renamed = room(merged().transform(Op::rename([("name", "label")])), false);
    // A merge's filter asks for its answer and the ops it rewrites.
    let rewritten = room(merged().transform(even(false)), true);
    assert_eq!(rewritten - renamed, rows + rows + ARRAY_ROOM);
    // A log's filter asks for its answer and a copy of the rows it keeps, change columns too.
    let copied = room(logged().transform(even(false)), true);
    let uncopied = room(logged().transform(even(false)), false);
    let changed = [OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN]
        .map(|name| column_bytes(batch.column_by_name(name).unwrap().as_ref()));
    assert_eq!(copied - uncopied, changed.iter().sum::<u64>());
    assert!(rewritten < copied, "{rewritten} {copied}");
    // A step with a lineage rebuilds the flags it passes on: a byte a row for two columns, an
    // offset a row and a validity bit a row.
    let relined = room(merged().transform(even(true)), true);
    assert_eq!(
        relined - rewritten,
        rows * (1 + 4) + rows.div_ceil(8) + ARRAY_ROOM
    );
}

#[test]
fn a_merge_filter_is_held_to_room_for_the_ops_it_rewrites_not_a_copy_of_its_rows() {
    let plan = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(code(Sieve));
    let all: Vec<i64> = (0..64).collect();
    let notes = all.iter().map(|_| "x".repeat(1000));
    let ops = Int8Array::from(vec![ChangeOp::Update.code(); all.len()]);
    let seqs = FixedSizeBinaryArray::try_from_iter(all.iter().map(|row| {
        let mut seq = [0_u8; 16];
        seq[8..].copy_from_slice(&row.to_be_bytes());
        seq
    }))
    .unwrap();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(all.clone())) as ArrayRef),
        (
            "note",
            Arc::new(StringArray::from_iter_values(notes)) as ArrayRef,
        ),
        (OP_COLUMN, Arc::new(ops) as ArrayRef),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
    ])
    .unwrap();
    // A copy of the notes would take a thousand bytes a row; the ops a merge rewrites take one.
    let output = run(&plan, &["id"], batch, 10_000).unwrap();
    assert_eq!(output.len(), 1);
}

#[test]
fn a_row_whose_bound_passes_the_room_is_refused() {
    let plan = stream().transform(Op::constant([("c", Literal::Text("x".repeat(1000)))]));
    let refused = run(&plan, &[], people(&[1, 2]), 10).unwrap_err();
    assert!(
        matches!(refused, Refusal::Row { asked, free: 10, .. } if asked > 1000),
        "{refused:?}"
    );
}

#[test]
fn code_whose_output_passes_the_room_is_cut_until_a_row_alone_does() {
    let plan = stream().transform(code(Broken(Breaks::Allocates, None)));
    let refused = run(&plan, &[], people(&[1, 2, 3, 4]), 100).unwrap_err();
    assert!(
        matches!(refused, Refusal::Row { free: 100, .. }),
        "{refused:?}"
    );
    let fits = run(&plan, &[], people(&[1, 2, 3, 4]), 1 << 20).unwrap();
    assert_eq!(fits.len(), 1);
}

#[test]
fn transforms_that_drop_or_retype_a_column_the_stream_keeps_are_refused() {
    for plan in [
        stream().transform(Op::rename([("id", "key")])),
        stream().transform(Op::mask(["id"], Mask::Null)),
    ] {
        let key = [ColumnPath::from("id")];
        let stage = Stage::new(&plan, &key, None).unwrap();
        let refused = stage.chain(people(&[1]).schema_ref()).unwrap_err();
        assert!(
            matches!(&refused, Refusal::Kept { column } if column == "id"),
            "{refused:?}"
        );
    }
}

#[test]
fn a_transform_refusing_its_input_is_refused_naming_it() {
    let plan = stream().transform(Op::rename([("note", "id")]));
    let stage = Stage::new(&plan, &[], None).unwrap();
    let refused = stage.chain(people(&[1]).schema_ref()).unwrap_err();
    assert!(
        matches!(&refused, Refusal::Schema { transform, error } if transform == "rename" && error.column() == Some("id")),
        "{refused:?}"
    );
}

#[test]
fn a_merge_filter_turns_failing_upserts_into_deletes_and_passes_every_delete() {
    let plan = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(code(EVEN));
    let batch = changes(&[
        (Some(1), ChangeOp::Insert, None),
        (Some(2), ChangeOp::Insert, None),
        (Some(3), ChangeOp::Update, None),
        (Some(5), ChangeOp::Delete, None),
    ]);
    let delete = ChangeOp::Delete.code();
    let output = run(&plan, &["id"], batch, u64::MAX).unwrap();
    assert_eq!(
        ops(&output),
        [
            (Some(1), delete),
            (Some(2), ChangeOp::Insert.code()),
            (Some(3), delete),
            (Some(5), delete)
        ]
    );
}

#[test]
fn a_log_filter_drops_failing_upserts_and_keeps_deletes_and_truncates() {
    let plan = stream().read(ReadMode::Cdc).transform(code(EVEN));
    let batch = changes(&[
        (Some(1), ChangeOp::Insert, None),
        (Some(2), ChangeOp::Update, None),
        (Some(3), ChangeOp::Delete, None),
        (None, ChangeOp::Truncate, None),
    ]);
    let output = run(&plan, &[], batch, u64::MAX).unwrap();
    let kept: Vec<Option<i64>> = ops(&output).into_iter().map(|(id, _)| id).collect();
    assert_eq!(kept, [Some(2), Some(3), None]);
    let seqs = output[0].column_by_name(SEQ_COLUMN).unwrap();
    let seqs = seqs.as_fixed_size_binary();
    let last: Vec<u8> = (0..seqs.len()).map(|row| seqs.value(row)[15]).collect();
    assert_eq!(last, [1, 2, 3]);
}

#[test]
fn change_columns_follow_the_data_and_flags_move_with_their_columns() {
    let plan = stream().read(ReadMode::Cdc).include_only(["name"]);
    let flagged: &[u8] = &[0b0000_0010];
    let batch = changes(&[(Some(1), ChangeOp::Update, Some(flagged))]);
    let output = run(&plan, &[], batch, u64::MAX).unwrap();
    assert_eq!(
        names(&output[0]),
        ["name", OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN]
    );
    assert_eq!(flags_of(&output[0]), [0b0000_0001]);
}

#[test]
fn flags_follow_lineage_through_a_rename_a_mask_and_a_constant() {
    let plan = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(Op::rename([("name", "label")]))
        .transform(Op::mask(["label"], Mask::redact("***")))
        .transform(Op::constant([("c", Literal::Integer(1))]));
    let flagged: &[u8] = &[0b0000_0010];
    let batch = changes(&[(Some(1), ChangeOp::Update, Some(flagged))]);
    let output = run(&plan, &["id"], batch, u64::MAX).unwrap();
    assert_eq!(
        names(&output[0]),
        ["id", "label", "c", OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN]
    );
    // `label` reads the unchanged `name`, so it stays unchanged; `c` reads nothing, so it is
    // written.
    assert_eq!(flags_of(&output[0]), [0b0000_0010]);
}

#[test]
fn a_null_bitmap_stays_null_through_lineage() {
    let plan = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(Op::constant([("c", Literal::Integer(1))]));
    let flagged: &[u8] = &[0b0000_0010];
    let batch = changes(&[
        (Some(1), ChangeOp::Update, None),
        (Some(2), ChangeOp::Update, Some(flagged)),
    ]);
    let output = run(&plan, &["id"], batch, u64::MAX).unwrap();
    let flags = output[0].column_by_name(UNCHANGED_COLUMN).unwrap();
    let flags = flags.as_binary::<i32>();
    assert!(flags.is_null(0));
    assert_eq!(flags.value(1), [0b0000_0010]);
}

#[test]
fn a_transform_without_lineage_refuses_a_batch_flagging_columns_and_takes_one_flagging_none() {
    let plan = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(code(Unlined));
    let flagged: &[u8] = &[0b0000_0010];
    let batch = changes(&[(Some(1), ChangeOp::Update, Some(flagged))]);
    let refused = ran(&plan, &["id"], batch, u64::MAX).unwrap_err();
    assert!(
        matches!(&refused, Refusal::Partial { transform } if transform == "unlined"),
        "{refused:?}"
    );
    let (empty, zero): (&[u8], &[u8]) = (&[], &[0]);
    let whole = changes(&[
        (Some(1), ChangeOp::Update, None),
        (Some(2), ChangeOp::Update, Some(empty)),
        (Some(3), ChangeOp::Update, Some(zero)),
    ]);
    assert_eq!(rows(&ran(&plan, &["id"], whole, u64::MAX).unwrap()), 3);
}

#[test]
fn a_filter_reading_a_column_an_update_leaves_unchanged_refuses_its_batch() {
    let flagged: &[u8] = &[0b0000_0010];
    for reads in [None, Some(vec![0, 1])] {
        let even = Even {
            reads: reads.clone(),
            lined: true,
        };
        let plan = stream()
            .read(ReadMode::Cdc)
            .write(WriteMode::Merge)
            .transform(code(even));
        let batch = changes(&[
            (Some(2), ChangeOp::Insert, None),
            (Some(3), ChangeOp::Update, Some(flagged)),
        ]);
        let refused = ran(&plan, &["id"], batch, u64::MAX).unwrap_err();
        assert!(
            matches!(&refused, Refusal::Unsent { transform } if transform == "even"),
            "{reads:?}: {refused:?}"
        );
    }
}

#[test]
fn a_filter_reading_only_columns_an_update_sent_judges_its_rows() {
    let even = Even {
        reads: Some(vec![0]),
        lined: true,
    };
    let plan = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(code(even));
    let flagged: &[u8] = &[0b0000_0010];
    let batch = changes(&[
        (Some(2), ChangeOp::Update, Some(flagged)),
        (Some(3), ChangeOp::Update, Some(flagged)),
    ]);
    let output = ran(&plan, &["id"], batch, u64::MAX).unwrap();
    let delete = ChangeOp::Delete.code();
    assert_eq!(
        ops(&output.batches),
        [(Some(2), ChangeOp::Update.code()), (Some(3), delete)]
    );
    assert_eq!(output.deleted, 1);
    assert_eq!(flags_of(&output.batches[0]), [0b0000_0010]);
}

#[test]
fn a_lineage_or_filter_naming_columns_its_input_lacks_breaks_the_contract() {
    let reads_beyond = Even {
        reads: Some(vec![3]),
        lined: true,
    };
    let cases = [
        (code(Overlined), Breach::Lineage),
        (code(reads_beyond), Breach::Reads),
    ];
    for (transform, expected) in cases {
        let plan = stream().transform(transform);
        let refused = run(&plan, &[], people(&[1]), u64::MAX).unwrap_err();
        assert!(
            matches!(refused, Refusal::Contract { breach, .. } if breach == expected),
            "{refused:?}"
        );
    }
}

#[test]
fn a_lineage_of_another_width_than_the_output_breaks_the_contract() {
    struct Narrow;

    impl Transform for Narrow {
        fn name(&self) -> &'static str {
            "narrow"
        }

        fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
            Ok(Arc::new(input.clone()))
        }

        fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
            Ok(batch)
        }

        fn lineage(&self, _input: &Schema) -> Option<Vec<Vec<usize>>> {
            Some(vec![vec![0]])
        }
    }

    let refused = run(
        &stream().transform(code(Narrow)),
        &[],
        people(&[1]),
        u64::MAX,
    )
    .unwrap_err();
    assert!(
        matches!(
            refused,
            Refusal::Contract {
                breach: Breach::Lineage,
                ..
            }
        ),
        "{refused:?}"
    );
}

#[test]
fn the_stage_counts_the_rows_a_filter_drops_and_the_upserts_it_deletes() {
    let dropped = ran(
        &stream().transform(code(EVEN)),
        &[],
        people(&[1, 2, 3, 4, 5]),
        u64::MAX,
    )
    .unwrap();
    assert_eq!(
        (dropped.dropped, dropped.deleted, rows(&dropped)),
        (3, 0, 2)
    );
    let log = stream().read(ReadMode::Cdc).transform(code(EVEN));
    let batch = changes(&[
        (Some(1), ChangeOp::Insert, None),
        (Some(3), ChangeOp::Delete, None),
    ]);
    let logged = ran(&log, &[], batch, u64::MAX).unwrap();
    assert_eq!((logged.dropped, logged.deleted, rows(&logged)), (1, 0, 1));
    let merge = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(code(EVEN));
    let batch = changes(&[
        (Some(1), ChangeOp::Insert, None),
        (Some(2), ChangeOp::Update, None),
        (Some(3), ChangeOp::Delete, None),
    ]);
    let deleted = ran(&merge, &["id"], batch, u64::MAX).unwrap();
    assert_eq!(
        (deleted.dropped, deleted.deleted, rows(&deleted)),
        (0, 1, 3)
    );
}

#[test]
fn an_op_naming_a_column_a_declared_schema_lacks_is_refused_where_a_batch_lacking_it_passes() {
    let plan = stream()
        .transform(Op::rename([("note", "remark")]))
        .transform(Op::mask(["note"], Mask::Null));
    let stage = Stage::new(&plan, &[], None).unwrap();
    let schema = people(&[1]).schema();
    let refused = stage.declared(&schema).unwrap_err();
    assert!(
        matches!(&refused, Refusal::Absent { transform, column } if transform == "mask" && column == "note"),
        "{refused:?}"
    );
    assert!(stage.chain(&schema).is_ok());
    let named = Stage::new(
        &stream().transform(Op::mask(["note"], Mask::Null)),
        &[],
        None,
    );
    let chain = named.unwrap().declared(&schema).unwrap();
    assert_eq!(chain.data_output().field(2).data_type(), &DataType::Null);
}

#[test]
fn a_merge_that_only_includes_keeps_its_flags() {
    let plan = stream()
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .include_only(["name"]);
    let flagged: &[u8] = &[0b0000_0010];
    let batch = changes(&[(Some(1), ChangeOp::Update, Some(flagged))]);
    let output = run(&plan, &["id"], batch, u64::MAX).unwrap();
    assert_eq!(flags_of(&output[0]), [0b0000_0010]);
}

#[test]
fn a_stream_not_read_as_changes_passes_columns_named_as_change_columns_to_its_transforms() {
    let batch = changes(&[(Some(2), ChangeOp::Insert, None)]);
    let plan = stream().transform(Op::rename([("name", "label")]));
    let output = run(&plan, &[], batch, u64::MAX).unwrap();
    assert_eq!(
        names(&output[0]),
        ["id", "label", OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN]
    );
}

/// Asserts `refusal` fails its stream with `kind` and `code`, and is never retried.
fn fails_with(refusal: Refusal, kind: ErrorKind, code: Option<crate::error::Code>) {
    let stream = StreamName::new("s").unwrap();
    let shown = format!("{refusal:?}");
    let failed = super::refused(&stream, refusal);
    assert_eq!(
        (failed.kind(), failed.code(), failed.stream()),
        (kind, code, Some(&stream)),
        "{shown}"
    );
    assert!(!failed.is_retryable(), "{shown}");
}

fn named(name: &str) -> String {
    name.to_owned()
}

#[test]
fn a_refused_schema_fails_its_stream_as_its_configuration() {
    let error = TransformError::new("no");
    let refusals = [
        Refusal::Schema {
            transform: named("t"),
            error,
        },
        Refusal::Kept {
            column: named("id"),
        },
        Refusal::Absent {
            transform: named("mask"),
            column: named("x"),
        },
    ];
    for refusal in refusals {
        fails_with(refusal, ErrorKind::Config, Some(codes::TRANSFORM_SCHEMA));
    }
}

#[test]
fn a_refused_batch_fails_its_stream_as_its_transform_s_doing() {
    let cases = [
        (
            Refusal::Failed {
                transform: named("t"),
                error: TransformError::new("no"),
            },
            codes::TRANSFORM_FAILED,
        ),
        (
            Refusal::Contract {
                transform: named("t"),
                bounded: false,
                breach: Breach::Schema,
            },
            codes::TRANSFORM_CONTRACT,
        ),
        (
            Refusal::Row {
                transform: Some(named("t")),
                asked: 2,
                free: 1,
            },
            codes::ROW_EXCEEDS_BUDGET,
        ),
        (
            Refusal::Flush {
                transform: Some(named("t")),
                asked: 2,
                free: 1,
            },
            codes::ROW_EXCEEDS_BUDGET,
        ),
        (
            Refusal::Partial {
                transform: named("t"),
            },
            codes::TRANSFORM_PARTIAL,
        ),
        (
            Refusal::Unsent {
                transform: named("t"),
            },
            codes::TRANSFORM_PARTIAL,
        ),
    ];
    for (refusal, code) in cases {
        fails_with(refusal, ErrorKind::Transform, Some(code));
    }
}

#[test]
fn rows_too_large_where_no_transform_made_them_fail_their_stream_as_lowering_s_do() {
    let refusals = [
        Refusal::Row {
            transform: None,
            asked: 2,
            free: 1,
        },
        Refusal::Flush {
            transform: None,
            asked: 2,
            free: 1,
        },
    ];
    for refusal in refusals {
        fails_with(refusal, ErrorKind::Source, Some(codes::ROW_EXCEEDS_BUDGET));
    }
}

#[test]
fn a_breach_of_a_declared_bound_fails_its_stream_as_a_bug() {
    let cases = [
        (
            Refusal::Contract {
                transform: named("t"),
                bounded: true,
                breach: Breach::Schema,
            },
            Some(codes::TRANSFORM_CONTRACT),
        ),
        (
            Refusal::Bound {
                transform: named("t"),
                allocated: 2,
                bound: 1,
            },
            Some(codes::TRANSFORM_BOUND),
        ),
        (Refusal::Internal(named("bug")), None),
    ];
    for (refusal, code) in cases {
        fails_with(refusal, ErrorKind::Internal, code);
    }
}

#[test]
fn a_failure_keeps_its_transform_s_error_as_its_cause() {
    let stream = StreamName::new("s").unwrap();
    let error = TransformError::new("no").with_column("c");
    let failed = super::refused(
        &stream,
        Refusal::Failed {
            transform: named("t"),
            error: error.clone(),
        },
    );
    let cause = std::error::Error::source(&failed).and_then(|cause| cause.downcast_ref());
    assert_eq!(cause, Some(&error));
}

/// A refusal of each room the stage asks for: for one row and for a flush, as a transform asks
/// it and as moving flags does.
fn refused_rooms() -> Vec<Refusal> {
    let (asked, free) = (2, 1);
    let by = [Some(named("t")), None];
    let rows = by.iter().map(|transform| Refusal::Row {
        transform: transform.clone(),
        asked,
        free,
    });
    let flushes = by.iter().map(|transform| Refusal::Flush {
        transform: transform.clone(),
        asked,
        free,
    });
    rows.chain(flushes).collect()
}

#[test]
fn each_refusal_names_its_stream_and_reads_as_what_it_refused() {
    let stream = StreamName::new("s").unwrap();
    let mut refusals = vec![
        Refusal::Schema {
            transform: named("t"),
            error: TransformError::new("no"),
        },
        Refusal::Kept {
            column: named("id"),
        },
        Refusal::Absent {
            transform: named("mask"),
            column: named("x"),
        },
        Refusal::Failed {
            transform: named("t"),
            error: TransformError::new("no"),
        },
        Refusal::Contract {
            transform: named("t"),
            bounded: false,
            breach: Breach::Schema,
        },
        Refusal::Bound {
            transform: named("t"),
            allocated: 2,
            bound: 1,
        },
        Refusal::Partial {
            transform: named("t"),
        },
        Refusal::Unsent {
            transform: named("t"),
        },
        Refusal::Internal(named("bug")),
    ];
    refusals.extend(refused_rooms());
    let count = refusals.len();
    let mut shown = std::collections::BTreeSet::new();
    for refusal in refusals {
        let text = super::refused(&stream, refusal).to_string();
        let what = text
            .strip_prefix("stream s: ")
            .expect("the error names its stream");
        assert!(!what.is_empty(), "{text}");
        shown.insert(what.to_owned());
    }
    assert_eq!(shown.len(), count);
}

#[test]
fn a_breach_reads_as_what_broke() {
    let breaches = [
        Breach::Rows {
            input: 3,
            output: 2,
        },
        Breach::Keep {
            rows: 3,
            answered: 4,
        },
        Breach::Schema,
        Breach::Lineage,
        Breach::Reads,
    ];
    let shown: std::collections::BTreeSet<String> =
        breaches.iter().map(ToString::to_string).collect();
    assert_eq!(shown.len(), breaches.len());
    assert!(shown.iter().all(|text| !text.is_empty()));
}
