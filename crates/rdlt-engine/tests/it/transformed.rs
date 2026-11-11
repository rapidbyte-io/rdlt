//! Transforms through the engine: ops on every push form, an include, a declared schema, the
//! filter rule on appends and on merged changes, a flush a filter empties, a read ending in rows
//! a filter drops or a stream ignores, partial updates, and transforms that fail.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int8Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Schema, SchemaRef, UnionFields, UnionMode};
use rdlt_connector::{
    ChangeOp, ColumnPath, Cursor, Field, OP_COLUMN, OpenContext, PartitionState, PipelineId,
    PipelineState, ReadMode, SEQ_COLUMN, Secret, StreamName, UNCHANGED_COLUMN,
};
use rdlt_connector_reference::{published, schema};
use rdlt_engine::{
    DeleteMode, ErrorKind, Literal, Mask, Op, RunOutcome, RunStatus, StreamPlan, StreamReport,
    Transform, TransformError, WriteMode, codes,
};

use crate::support::destinations::limited;
use crate::support::served::{Form, Served, ints, people, people_schema, source, texts};
use crate::support::{commit_every, engine, memory, pipeline, published_ids, retrying, stream};

fn succeeded(outcome: &RunOutcome) {
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

/// Each published row's id and text in `column` of `people` in `store`, sorted.
fn texts_of(store: &str, column: &str) -> Vec<(i64, Option<String>)> {
    let mut rows = Vec::new();
    for batch in published(store, "people") {
        let ids = batch
            .column_by_name("id")
            .expect("the table has its id")
            .as_primitive::<Int64Type>();
        let values = batch
            .column_by_name(column)
            .expect("the table has the column");
        let values = values.as_string::<i32>();
        for row in 0..batch.num_rows() {
            let value = values.is_valid(row).then(|| values.value(row).to_owned());
            rows.push((ids.value(row), value));
        }
    }
    rows.sort();
    rows
}

/// A filter keeping the rows whose `id` is even, saying it reads only `id` where `keyed`, and
/// that each column it outputs reads itself where `lined`.
struct Even {
    keyed: bool,
    lined: bool,
}

const EVEN: Even = Even {
    keyed: false,
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

    fn keep_reads(&self, input: &Schema) -> Option<Vec<usize>> {
        let id = input.index_of("id").ok()?;
        self.keyed.then(|| vec![id])
    }
}

/// A filter keeping rows whose `status` is not `test`, a null status never.
struct NotTest;

impl Transform for NotTest {
    fn name(&self) -> &'static str {
        "not_test"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        true
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        let status = batch
            .column_by_name("status")
            .ok_or_else(|| TransformError::new("no status"))?;
        let statuses = status.as_string::<i32>().iter();
        Ok(statuses
            .map(|status| status.map(|status| status != "test"))
            .collect())
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }
}

/// A transform failing on every batch, or panicking where `panics` says.
struct Failing {
    panics: bool,
}

impl Transform for Failing {
    fn name(&self) -> &'static str {
        "failing"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn apply(&self, _batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        assert!(!self.panics, "this transform panics on purpose");
        Err(TransformError::new("refused on purpose"))
    }
}

fn code(transform: impl Transform) -> Arc<dyn Transform> {
    Arc::new(transform)
}

/// A position as a change batch's sequence holds it.
fn position(at: u64) -> [u8; 16] {
    let mut seq = [0_u8; 16];
    seq[8..].copy_from_slice(&at.to_be_bytes());
    seq
}

/// A change batch of `people`: each row's id, status and op, from position `first`.
fn changed(rows: &[(i64, &str, ChangeOp)], first: u64) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let statuses: Vec<&str> = rows.iter().map(|row| row.1).collect();
    let ops = Int8Array::from(rows.iter().map(|row| row.2.code()).collect::<Vec<_>>());
    let positions = (first..).take(rows.len()).map(position);
    let seqs = FixedSizeBinaryArray::try_from_iter(positions).expect("positions are 16 bytes");
    RecordBatch::try_from_iter([
        ("id", ints(&ids)),
        ("status", texts(&statuses)),
        (OP_COLUMN, Arc::new(ops) as ArrayRef),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
    ])
    .expect("the columns are of one length")
}

#[tokio::test]
async fn ops_reshape_every_push_form_before_its_table_is_fitted() {
    for form in [Form::Arrow, Form::Json] {
        let name = format!("transformed_ops_{form:?}").to_lowercase();
        let served = Served::new(form, vec![people(0, 3), people(3, 2)]);
        let plan = stream("people")
            .transform(Op::rename([("note", "remark")]))
            .transform(Op::mask(
                ["secret"],
                Mask::hmac_sha256(Secret::new("k".to_owned())),
            ))
            .transform(Op::constant([("region", Literal::Text("eu".to_owned()))]));
        let outcome = engine(commit_every(4))
            .run(
                pipeline(&name, [plan]),
                source(&name, served).await,
                memory(&name).await,
            )
            .await;
        succeeded(&outcome);
        let columns = schema(&name, "people").expect("the table exists");
        for column in ["id", "secret", "remark", "region"] {
            assert!(columns.field(column).is_some(), "{name}: {column}");
        }
        assert!(columns.field("note").is_none(), "{name}");
        let hexed = |value: &Option<String>| {
            value.as_ref().is_some_and(|hex| {
                hex.len() == 64
                    && hex
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        };
        let secrets = texts_of(&name, "secret");
        assert_eq!(secrets.len(), 5, "{name}");
        assert!(
            secrets.iter().all(|(_, secret)| hexed(secret)),
            "{name}: {secrets:?}"
        );
        let remarks = texts_of(&name, "remark");
        assert_eq!(remarks[0], (0, Some("w0".to_owned())), "{name}");
        let regions = texts_of(&name, "region");
        assert!(
            regions
                .iter()
                .all(|(_, region)| region.as_deref() == Some("eu"))
        );
    }
}

#[tokio::test]
async fn ops_pass_on_pushes_lacking_the_columns_they_name() {
    let name = "transformed_drift";
    let lacking = people(3, 2)
        .project(&[0, 2])
        .expect("the batch has three columns");
    let served = Served::new(Form::Json, vec![people(0, 3), lacking]);
    let plan = stream("people").transform(Op::mask(["secret"], Mask::redact("***")));
    let outcome = engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await;
    succeeded(&outcome);
    assert_eq!(published_ids(name, "people"), [0, 1, 2, 3, 4]);
    let secrets: Vec<Option<String>> = texts_of(name, "secret")
        .into_iter()
        .map(|(_, s)| s)
        .collect();
    let redacted = Some("***".to_owned());
    assert_eq!(
        secrets,
        [redacted.clone(), redacted.clone(), redacted, None, None]
    );
}

#[tokio::test]
async fn an_included_stream_lands_only_its_listed_columns_and_its_key() {
    for form in [Form::Arrow, Form::Json] {
        let name = format!("transformed_included_{form:?}").to_lowercase();
        let served = Served::new(form, vec![people(0, 3)]);
        let plan = stream("people")
            .write(WriteMode::Merge)
            .key(["id"])
            .include_only(["note"]);
        let outcome = engine(commit_every(4))
            .run(
                pipeline(&name, [plan]),
                source(&name, served).await,
                memory(&name).await,
            )
            .await;
        succeeded(&outcome);
        let columns = schema(&name, "people").expect("the table exists");
        let names: Vec<&str> = columns
            .fields()
            .iter()
            .map(Field::name)
            .filter(|name| !name.starts_with("_rdlt_"))
            .collect();
        assert_eq!(names, ["id", "note"], "{name}");
        assert_eq!(published_ids(&name, "people"), [0, 1, 2], "{name}");
    }
}

#[tokio::test]
async fn a_declared_schema_is_fitted_as_the_transforms_make_it() {
    let name = "transformed_declared";
    let mut served = Served::new(Form::Arrow, Vec::new());
    served.declared = Some(people_schema());
    let plan = stream("people").transform(Op::rename([("note", "remark")]));
    let outcome = engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await;
    succeeded(&outcome);
    let columns = schema(name, "people").expect("the declared table exists before any row");
    assert!(columns.field("remark").is_some() && columns.field("note").is_none());
}

#[tokio::test]
async fn a_filtered_append_lands_only_the_rows_its_filter_keeps() {
    let name = "transformed_even";
    let served = Served::new(Form::Arrow, vec![people(0, 5), people(5, 5)]);
    let plan = stream("people").transform(code(EVEN));
    let outcome = engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await;
    succeeded(&outcome);
    assert_eq!(published_ids(name, "people"), [0, 2, 4, 6, 8]);
    let filtered = outcome.report.streams["people"].filtered;
    assert_eq!((filtered.dropped, filtered.deleted), (5, 0));
}

#[tokio::test]
async fn a_flush_its_filter_empties_commits_its_checkpoint_and_the_next_run_resumes_after_it() {
    let name = "transformed_emptied";
    let run = |pushes: Vec<RecordBatch>| async move {
        let served = Served::new(Form::Arrow, pushes);
        let plan = stream("people")
            .read(ReadMode::Incremental)
            .transform(code(EVEN));
        engine(commit_every(1))
            .run(
                pipeline(name, [plan]),
                source(name, served).await,
                memory(name).await,
            )
            .await
    };
    // The last push's flush is emptied by the filter; its checkpoint still commits.
    let first = run(vec![people(1, 1), people(2, 1), people(3, 1)]).await;
    succeeded(&first);
    assert_eq!(published_ids(name, "people"), [2]);
    assert_eq!(first.report.streams["people"].filtered.dropped, 2);
    // Were the emptied flush's checkpoint not committed, the next run would read its push again
    // and land the even row served there now.
    let second = run(vec![people(1, 1), people(2, 1), people(6, 1), people(4, 1)]).await;
    succeeded(&second);
    assert_eq!(published_ids(name, "people"), [2, 4]);
}

/// Where stored state says the partitions of `people` in store `name` stand.
async fn standing(name: &str) -> Vec<PartitionState> {
    let context = OpenContext {
        pipeline: PipelineId::parse(name).expect("a valid pipeline"),
    };
    let opened = memory(name).await.open(&context).await;
    let opened = opened.expect("the destination opens");
    let state = PipelineState::from_records(&opened.state).expect("the state reads");
    let people = StreamName::new("people").expect("a valid name");
    state.streams.get(&people).map_or_else(Vec::new, |stream| {
        stream.partitions.values().cloned().collect()
    })
}

/// Runs `plan` twice over `served`, whose last push follows its last checkpoint, into store
/// `name`; the two runs' reports of `people`.
async fn twice(name: &str, mut served: Served, plan: StreamPlan) -> [StreamReport; 2] {
    served.tail = true;
    let mut reports = Vec::new();
    for _ in 0..2 {
        let outcome = engine(commit_every(1))
            .run(
                pipeline(name, [plan.clone()]),
                source(name, served.clone()).await,
                memory(name).await,
            )
            .await;
        succeeded(&outcome);
        let report = outcome.report.streams.get("people").cloned();
        reports.push(report.unwrap_or_default());
    }
    reports.try_into().expect("two runs")
}

#[tokio::test]
async fn a_bounded_read_ending_in_rows_its_filter_drops_counts_them_once_and_ends_done() {
    let name = "transformed_dropped_tail";
    // Row 1 is dropped before the last checkpoint and row 3, the last push, after it.
    let served = Served::new(Form::Arrow, vec![people(0, 2), people(3, 1)]);
    let plan = stream("people")
        .read(ReadMode::Incremental)
        .transform(code(EVEN));
    let [first, second] = twice(name, served, plan).await;
    assert_eq!((first.filtered.dropped, second.filtered.dropped), (2, 0));
    assert_eq!(published_ids(name, "people"), [0]);
    assert_eq!(standing(name).await, [PartitionState::Done]);
}

#[tokio::test]
async fn a_bounded_read_ending_in_deletes_its_stream_ignores_counts_them_once_and_ends_done() {
    // Changes the stream ignores, with no transform, end a read as rows a filter drops do.
    let name = "transformed_ignored_tail";
    let pushes = vec![
        changed(
            &[(1, "ok", ChangeOp::Insert), (2, "ok", ChangeOp::Insert)],
            1,
        ),
        changed(&[(1, "ok", ChangeOp::Delete)], 3),
    ];
    let mut served = Served::new(Form::Changes, pushes);
    served.key = Some(ColumnPath::from("id"));
    let plan = stream("people")
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .deletes(DeleteMode::Ignore);
    let [first, second] = twice(name, served, plan).await;
    assert_eq!((first.deletes_ignored, second.deletes_ignored), (1, 0));
    assert_eq!(published_ids(name, "people"), [1, 2]);
    assert_eq!(standing(name).await, [PartitionState::Done]);
}

#[tokio::test]
async fn an_unsplit_change_stream_reads_the_changes_after_its_last_checkpoint_again() {
    let name = "transformed_unsplit_tail";
    let pushes = vec![
        changed(
            &[(1, "ok", ChangeOp::Insert), (2, "ok", ChangeOp::Insert)],
            1,
        ),
        changed(&[(3, "ok", ChangeOp::Insert)], 3),
    ];
    let mut served = Served::new(Form::Changes, pushes);
    served.key = Some(ColumnPath::from("id"));
    served.unsplit = true;
    let plan = stream("people").read(ReadMode::Cdc).write(WriteMode::Merge);
    // Each read ends on the change no checkpoint follows: it lands with neither.
    twice(name, served.clone(), plan.clone()).await;
    assert_eq!(published_ids(name, "people"), [1, 2]);
    let cursor = Cursor::encode(1, &1_usize).expect("a cursor encodes");
    assert_eq!(standing(name).await, [PartitionState::Cursor(cursor)]);
    // Read again from that checkpoint by a read that checkpoints it, it lands once.
    served.tail = false;
    let outcome = engine(commit_every(1))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await;
    succeeded(&outcome);
    assert_eq!(published_ids(name, "people"), [1, 2, 3]);
}

#[tokio::test]
async fn a_merged_change_stream_deletes_a_row_once_it_stops_matching_its_filter() {
    for deletes in [DeleteMode::Hard, DeleteMode::Soft] {
        let name = format!("transformed_merged_{deletes:?}").to_lowercase();
        let pushes = vec![
            changed(
                &[(1, "ok", ChangeOp::Insert), (2, "ok", ChangeOp::Insert)],
                1,
            ),
            changed(&[(1, "test", ChangeOp::Update)], 3),
        ];
        let mut served = Served::new(Form::Changes, pushes);
        served.key = Some(ColumnPath::from("id"));
        let plan = stream("people")
            .read(ReadMode::Cdc)
            .write(WriteMode::Merge)
            .deletes(deletes)
            .transform(code(NotTest));
        let outcome = engine(commit_every(4))
            .run(
                pipeline(&name, [plan]),
                source(&name, served).await,
                memory(&name).await,
            )
            .await;
        succeeded(&outcome);
        let mut live = Vec::new();
        for batch in published(&name, "people") {
            let ids = batch
                .column_by_name("id")
                .expect("the table has its id")
                .as_primitive::<Int64Type>();
            let deleted = batch.column_by_name("_rdlt_deleted_at");
            for row in 0..batch.num_rows() {
                let gone = deleted.is_some_and(|at| at.is_valid(row));
                live.push((ids.value(row), gone));
            }
        }
        live.sort_unstable();
        let expected = match deletes {
            DeleteMode::Soft => vec![(1, true), (2, false)],
            _ => vec![(2, false)],
        };
        assert_eq!(live, expected, "{name}");
        let filtered = outcome.report.streams["people"].filtered;
        assert_eq!((filtered.dropped, filtered.deleted), (0, 1), "{name}");
    }
}

/// A change batch of `people` of one row: its id, status and note, its op, and the flags it
/// carries, at position `at`.
fn noted(
    (id, status, note): (i64, &str, Option<&str>),
    op: ChangeOp,
    flags: Option<&[u8]>,
    at: u64,
) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", ints(&[id])),
        ("status", texts(&[status])),
        ("note", Arc::new(StringArray::from(vec![note])) as ArrayRef),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from(vec![op.code()])) as ArrayRef,
        ),
        (
            SEQ_COLUMN,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([position(at)].into_iter())
                    .expect("a position is 16 bytes"),
            ) as ArrayRef,
        ),
        (
            UNCHANGED_COLUMN,
            Arc::new(BinaryArray::from(vec![flags])) as ArrayRef,
        ),
    ])
    .expect("the columns are of one length")
}

/// Merges, into store `name`, an insert of `people` row 1 and an update leaving its note
/// unchanged, through `plan`'s transforms.
async fn partial(name: &str, plan: StreamPlan) -> RunOutcome {
    partial_written(name, plan, WriteMode::Merge, 1).await
}

/// Writes as `write` says, into store `name`, an insert of `people` row `id` and an update
/// leaving its note unchanged, through `plan`'s transforms.
async fn partial_written(name: &str, plan: StreamPlan, write: WriteMode, id: i64) -> RunOutcome {
    let unchanged: &[u8] = &[0b0000_0100];
    let note = format!("n{id}");
    let pushes = vec![
        noted((id, "ok", Some(&note)), ChangeOp::Insert, None, 1),
        noted((id, "late", None), ChangeOp::Update, Some(unchanged), 2),
    ];
    let mut served = Served::new(Form::Changes, pushes);
    served.key = Some(ColumnPath::from("id"));
    let plan = plan.read(ReadMode::Cdc).write(write);
    engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await
}

#[tokio::test]
async fn an_update_leaving_a_column_unchanged_lands_through_ops_as_it_would_untransformed() {
    succeeded(&partial("partial_plain", stream("people")).await);
    let transformed = stream("people")
        .transform(Op::rename([("note", "remark")]))
        .transform(Op::mask(["status"], Mask::redact("***")))
        .transform(Op::constant([("region", Literal::Text("eu".to_owned()))]));
    succeeded(&partial("partial_transformed", transformed).await);
    let kept = vec![(1, Some("n1".to_owned()))];
    assert_eq!(texts_of("partial_plain", "note"), kept);
    assert_eq!(texts_of("partial_transformed", "remark"), kept);
    assert_eq!(
        texts_of("partial_transformed", "status"),
        [(1, Some("***".to_owned()))]
    );
    assert_eq!(
        texts_of("partial_transformed", "region"),
        [(1, Some("eu".to_owned()))]
    );
}

#[tokio::test]
async fn a_transform_declaring_no_lineage_refuses_an_update_leaving_a_column_unchanged() {
    let outcome = partial("partial_unlined", stream("people").transform(code(EVEN))).await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Transform, Some(codes::TRANSFORM_PARTIAL))
    );
}

#[tokio::test]
async fn a_filter_reading_an_unsent_column_refuses_an_update_and_one_reading_the_key_takes_it() {
    let unsent = Even {
        keyed: false,
        lined: true,
    };
    let outcome = partial("partial_unsent", stream("people").transform(code(unsent))).await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Transform, Some(codes::TRANSFORM_PARTIAL))
    );
    let keyed = Even {
        keyed: true,
        lined: true,
    };
    let outcome = partial("partial_keyed", stream("people").transform(code(keyed))).await;
    succeeded(&outcome);
    // Row 1 is odd: its insert and its update become deletes of its key.
    assert!(published_ids("partial_keyed", "people").is_empty());
    let filtered = outcome.report.streams["people"].filtered;
    assert_eq!((filtered.dropped, filtered.deleted), (0, 2));
}

#[tokio::test]
async fn an_op_naming_a_column_the_declared_schema_lacks_is_refused_before_anything_is_read() {
    let name = "transformed_mistyped";
    let mut served = Served::new(Form::Arrow, vec![people(0, 1)]);
    served.declared = Some(people_schema());
    let plan = stream("people").transform(Op::mask(["ssn"], Mask::redact("***")));
    let outcome = engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await;
    let error = outcome.error.expect("the attempt is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Config, Some(codes::TRANSFORM_SCHEMA))
    );
    assert_eq!(outcome.report.rows, 0);
}

#[tokio::test]
async fn an_op_naming_the_sources_primary_key_is_refused_before_anything_is_read() {
    let name = "transformed_keyed";
    let mut served = Served::new(Form::Arrow, vec![people(0, 1)]);
    served.key = Some(ColumnPath::from("id"));
    let plan = stream("people")
        .write(WriteMode::Merge)
        .transform(Op::mask(["id"], Mask::Null));
    let outcome = engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await;
    let error = outcome.error.expect("the attempt is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Config, Some(codes::TRANSFORM_INVALID))
    );
    assert_eq!(outcome.report.rows, 0);
}

#[tokio::test]
async fn a_failing_transform_fails_the_run_once_and_a_panicking_one_as_a_bug() {
    for (panics, kind, expected) in [
        (false, ErrorKind::Transform, Some(codes::TRANSFORM_FAILED)),
        (true, ErrorKind::Internal, None),
    ] {
        let name = format!("transformed_failing_{panics}");
        let served = Served::new(Form::Arrow, vec![people(0, 2)]);
        let plan = stream("people").transform(code(Failing { panics }));
        let outcome = engine(retrying(3))
            .run(
                pipeline(&name, [plan]),
                source(&name, served).await,
                memory(&name).await,
            )
            .await;
        let error = outcome.error.expect("the run fails");
        assert_eq!(error.kind(), kind, "{error:?}");
        if expected.is_some() {
            assert_eq!(error.code(), expected);
            assert_eq!(
                outcome.report.attempted, 1,
                "a transform's failure is never retried"
            );
        }
    }
}

/// The names of the columns of `people` in `store` that are not the engine's, in order.
fn columns_of(store: &str) -> Vec<String> {
    let Some(columns) = schema(store, "people") else {
        return Vec::new();
    };
    let names = columns.fields().iter().map(Field::name);
    let names = names.filter(|name| !name.starts_with("_rdlt_"));
    names.map(str::to_owned).collect()
}

#[tokio::test]
async fn a_change_log_that_includes_columns_keeps_the_key_its_deletes_carry() {
    let name = "transformed_log_included";
    let pushes = vec![
        noted((1, "ok", Some("n1")), ChangeOp::Insert, None, 1),
        noted((1, "gone", None), ChangeOp::Delete, None, 2),
    ];
    let mut served = Served::new(Form::Changes, pushes);
    served.key = Some(ColumnPath::from("id"));
    let plan = stream("people").read(ReadMode::Cdc).include_only(["note"]);
    let outcome = engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await;
    succeeded(&outcome);
    assert_eq!(columns_of(name), ["id", "note"]);
    // The insert and the delete each land with the key.
    assert_eq!(published_ids(name, "people"), [1, 1]);
}

/// A transform adding a column of a type no table holds, written as an embedder's.
struct Untableable;

impl Transform for Untableable {
    fn name(&self) -> &'static str {
        "untableable"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        let union = DataType::Union(UnionFields::empty(), UnionMode::Sparse);
        let mut fields: Vec<_> = input.fields().iter().cloned().collect();
        fields.push(Arc::new(arrow_schema::Field::new("u", union, true)));
        Ok(Arc::new(Schema::new(fields)))
    }

    fn apply(&self, _batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Err(TransformError::new("never asked to run"))
    }
}

#[tokio::test]
async fn a_declared_schema_its_transforms_make_untableable_is_refused_as_configuration() {
    let name = "transformed_untableable";
    let mut served = Served::new(Form::Arrow, vec![people(0, 1)]);
    served.declared = Some(people_schema());
    let plan = stream("people").transform(code(Untableable));
    let outcome = engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            memory(name).await,
        )
        .await;
    let error = outcome.error.expect("the attempt is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Config, Some(codes::TRANSFORM_SCHEMA))
    );
    assert_eq!(outcome.report.rows, 0);
}

#[tokio::test]
async fn a_push_of_no_rows_lands_no_column_staged_or_not() {
    let run = |name: &'static str, plan: StreamPlan| async move {
        let served = Served::new(Form::Arrow, vec![people(0, 0)]);
        let outcome = engine(commit_every(4))
            .run(
                pipeline(name, [plan]),
                source(name, served).await,
                memory(name).await,
            )
            .await;
        succeeded(&outcome);
        columns_of(name)
    };
    // The write fits a table from the rows it lands, so a stream with no stage lands no column.
    assert!(run("schema_only_plain", stream("people")).await.is_empty());
    let renamed = stream("people").transform(Op::rename([("note", "remark")]));
    assert!(run("schema_only_transformed", renamed).await.is_empty());
    let included = stream("people").include_only(["note"]);
    assert!(run("schema_only_included", included).await.is_empty());
}

/// A filter keeping rows whose `status` is not `test`, saying it reads only `status` and that
/// each column it outputs reads itself.
struct Live;

impl Transform for Live {
    fn name(&self) -> &'static str {
        "live"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Ok(Arc::new(input.clone()))
    }

    fn filters(&self) -> bool {
        true
    }

    fn keep(&self, batch: &RecordBatch) -> Result<BooleanArray, TransformError> {
        NotTest.keep(batch)
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        Ok(batch)
    }

    fn lineage(&self, input: &Schema) -> Option<Vec<Vec<usize>>> {
        Some(
            (0..input.fields().len())
                .map(|column| vec![column])
                .collect(),
        )
    }

    fn keep_reads(&self, input: &Schema) -> Option<Vec<usize>> {
        input.index_of("status").ok().map(|status| vec![status])
    }
}

#[tokio::test]
async fn a_merged_filter_reading_beyond_the_key_refuses_an_update_it_keeps_leaving_a_column_unchanged()
 {
    // The filter keeps the update, whose status is not test, by its status: a row it once turned
    // into a delete would come back without the note the update left unchanged.
    let live = || stream("people").transform(code(Live));
    let outcome = partial("partial_rejoined", live()).await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Transform, Some(codes::TRANSFORM_PARTIAL))
    );
    // A change log keeps the update as it is sent.
    let logged = partial_written("partial_logged", live(), WriteMode::Append, 1).await;
    succeeded(&logged);
    assert_eq!(logged.report.streams["people"].filtered.dropped, 0);
    // A filter reading only the key never flips its answer for a row, and keeps the update.
    let keyed = Even {
        keyed: true,
        lined: true,
    };
    let kept = stream("people").transform(code(keyed));
    succeeded(&partial_written("partial_keyed_kept", kept, WriteMode::Merge, 2).await);
    assert_eq!(
        texts_of("partial_keyed_kept", "note"),
        [(2, Some("n2".to_owned()))]
    );
}

#[tokio::test]
async fn an_update_a_filter_deletes_asks_a_destination_to_keep_no_column() {
    let name = "transformed_deleted_partial";
    // The update leaves its note unchanged, and its status makes the filter delete its row.
    let unchanged: &[u8] = &[0b0000_0100];
    let pushes = vec![
        noted((1, "ok", Some("n1")), ChangeOp::Insert, None, 1),
        noted((1, "test", None), ChangeOp::Update, Some(unchanged), 2),
    ];
    let mut served = Served::new(Form::Changes, pushes);
    served.key = Some(ColumnPath::from("id"));
    let plan = stream("people")
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(code(Live));
    let destination = limited(memory(name).await, |capabilities| {
        capabilities.partial_updates = false;
    });
    let outcome = engine(commit_every(4))
        .run(
            pipeline(name, [plan]),
            source(name, served).await,
            destination,
        )
        .await;
    succeeded(&outcome);
    assert!(published_ids(name, "people").is_empty());
    assert_eq!(outcome.report.streams["people"].filtered.deleted, 1);
}
