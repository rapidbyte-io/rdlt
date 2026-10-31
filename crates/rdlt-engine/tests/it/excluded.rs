//! Columns a stream excludes: never in its table, its name map or its state, whether its source
//! leaves them out or not, a change batch's flags moved past them, and a column the table had
//! kept and left null.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch,
    StringArray,
};
use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{
    ColumnKey, ColumnPath, ConnectContext, ConnectorError, Destination, Emitter, Exclusion, Field,
    LogicalType, OP_COLUMN, OpenContext, Partition, PartitionId, PipelineId, PipelineState, Push,
    ReadMode, ReadRequest, ReadStream, Result, SEQ_COLUMN, Source, SourceConnector, SourceEvent,
    StreamName, StreamSpec, Streams, TableSchema, UNCHANGED_COLUMN, partition_channel,
    remap_unchanged, source_factory,
};
use rdlt_connector_reference::{published, schema};
use rdlt_engine::{ErrorKind, RunOutcome, RunStatus, StreamPlan, WriteMode, codes};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::support::{commit_every, engine, memory, pipeline, spawned_generator, stream};

/// How the test's source pushes its stream, `people`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Form {
    Arrow,
    Json,
    Changes,
}

/// What a test's source serves: its pushes, in one form; whether it leaves out the columns the
/// stream excludes, as a source that honours the projection does; and the schema, key and change
/// time its catalog declares, if any.
#[derive(Clone, Debug)]
struct Served {
    form: Form,
    pushes: Vec<RecordBatch>,
    honours: bool,
    declared: Option<TableSchema>,
    key: Option<ColumnPath>,
    change_time: Option<ColumnPath>,
}

impl Served {
    fn new(form: Form, pushes: Vec<RecordBatch>) -> Self {
        Self {
            form,
            pushes,
            honours: false,
            declared: None,
            key: None,
            change_time: None,
        }
    }
}

static SERVED: LazyLock<Mutex<BTreeMap<String, Served>>> = LazyLock::new(Mutex::default);

/// The source serving `served`, registered as `name`.
async fn source(name: &str, served: Served) -> Arc<dyn Source> {
    SERVED.lock().insert(name.to_owned(), served);
    let source = source_factory::<Excluding>()
        .connect(json!({ "name": name }), ConnectContext::new())
        .await
        .expect("the source is registered");
    Arc::from(source)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ExcludingConfig {
    name: String,
}

struct Excluding {
    served: Served,
}

impl SourceConnector for Excluding {
    const ID: &'static str = "io.test.excluding";
    const VERSION: &'static str = "0.0.0";
    type Config = ExcludingConfig;

    async fn connect(config: ExcludingConfig, _: &ConnectContext) -> Result<Self> {
        let served = SERVED.lock().get(&config.name).cloned();
        let served = served.ok_or_else(|| ConnectorError::config("no such source"))?;
        Ok(Self { served })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(People(self.served.clone()))
    }
}

/// The stream `people`, as the test serves it.
struct People(Served);

impl ReadStream<Excluding> for People {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        let name = StreamName::new("people").expect("a valid name");
        let mut spec = StreamSpec::new(name).with_read_modes([ReadMode::Full, ReadMode::Cdc]);
        if let Some(schema) = &self.0.declared {
            spec = spec.with_schema(schema.clone());
        }
        if let Some(key) = &self.0.key {
            spec = spec.with_primary_key([key.clone()]);
        }
        if let Some(column) = &self.0.change_time {
            spec = spec.with_change_time(column.clone());
        }
        spec
    }

    async fn read(
        &self,
        source: &Excluding,
        _: &Partition,
        next: usize,
        out: &mut Emitter<usize>,
    ) -> Result<()> {
        let served = &source.served;
        for (index, batch) in served.pushes.iter().enumerate().skip(next) {
            let batch = if served.honours {
                left_out(batch, out.excluded())
            } else {
                batch.clone()
            };
            match served.form {
                Form::Arrow => out.batch(batch).await?,
                Form::Changes => out.changes(batch).await?,
                Form::Json => out.json(lines(&batch)).await?,
            }
            out.checkpoint(&(index + 1)).await?;
        }
        Ok(())
    }
}

/// `batch` without the columns `excluded` names, as a source that honours its read's projection
/// leaves them out: a change batch's flags name the fields it keeps.
fn left_out(batch: &RecordBatch, excluded: &Exclusion) -> RecordBatch {
    let schema = batch.schema();
    let mut kept = Vec::new();
    let places: Vec<Option<usize>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(place, field)| {
            (!excluded.contains(field.name())).then(|| {
                kept.push(place);
                kept.len() - 1
            })
        })
        .collect();
    let projected = batch
        .project(&kept)
        .expect("the kept columns are the batch's");
    let Some(flags) = projected.column_by_name(UNCHANGED_COLUMN) else {
        return projected;
    };
    let moved: ArrayRef = Arc::new(remap_unchanged(flags.as_binary::<i32>(), &places));
    let mut columns = projected.columns().to_vec();
    let at = projected
        .schema()
        .index_of(UNCHANGED_COLUMN)
        .expect("it has flags");
    columns[at] = moved;
    RecordBatch::try_new(projected.schema(), columns).expect("the flags keep their type")
}

/// `batch`'s rows as JSON lines.
fn lines(batch: &RecordBatch) -> Bytes {
    let mut writer = arrow_json::LineDelimitedWriter::new(Vec::new());
    writer.write(batch).expect("the batch renders as JSON");
    writer.finish().expect("the lines end");
    Bytes::from(writer.into_inner())
}

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn texts(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// `rows` people, from `first`: an id, a secret and a note.
fn people(first: i64, rows: i64) -> RecordBatch {
    let ids: Vec<i64> = (first..first + rows).collect();
    let words: Vec<String> = ids.iter().map(|id| format!("w{id}")).collect();
    let words: Vec<&str> = words.iter().map(String::as_str).collect();
    RecordBatch::try_from_iter([
        ("id", ints(&ids)),
        ("secret", texts(&words)),
        ("note", texts(&words)),
    ])
    .expect("a valid batch")
}

/// The declared schema of `people`.
fn people_schema() -> TableSchema {
    TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("secret", LogicalType::Utf8, true),
        Field::new("note", LogicalType::Utf8, true),
    ])
    .expect("distinct names")
}

/// The state `destination` holds for pipeline `name`.
async fn state(destination: &dyn Destination, name: &str) -> PipelineState {
    let context = OpenContext {
        pipeline: PipelineId::parse(name).expect("a valid pipeline"),
    };
    let opened = destination.open(&context).await.expect("it opens");
    PipelineState::from_records(&opened.state).expect("the state reads")
}

/// Checks that `column` is in none of `table`'s columns, published rows, name map or recorded
/// schema, in `store` and the state of `pipeline`.
async fn lands_nowhere(store: &str, pipeline: &str, table: &str, column: &str) {
    let columns = schema(store, table).expect("the table exists");
    let names: Vec<&str> = columns.fields().iter().map(Field::name).collect();
    assert!(!names.contains(&column), "{store}: {names:?}");
    for batch in published(store, table) {
        assert!(batch.column_by_name(column).is_none(), "{store}");
    }
    let state = state(memory(store).await.as_ref(), pipeline).await;
    let path = rdlt_connector::TablePath::new([table]).expect("a valid path");
    let table = &state.tables[&path];
    let key = ColumnKey::Source(ColumnPath::from(column));
    assert!(table.names.get(&key).is_none(), "{store}");
    let (_, recorded) = table.schema.as_ref().expect("a schema is recorded");
    let recorded: Vec<&str> = recorded.fields().iter().map(Field::name).collect();
    assert!(!recorded.contains(&column), "{store}: {recorded:?}");
}

fn succeeded(outcome: &RunOutcome) {
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test]
async fn an_excluded_column_lands_nowhere_whether_its_source_leaves_it_out_or_not() {
    for form in [Form::Arrow, Form::Json] {
        for honours in [false, true] {
            for declares in [false, true] {
                let name = format!("excluded_{form:?}_{honours}_{declares}").to_lowercase();
                let mut served = Served::new(form, vec![people(0, 3), people(3, 2)]);
                served.honours = honours;
                served.declared = declares.then(people_schema);
                let source = source(&name, served).await;
                let plan = pipeline(&name, [stream("people").exclude(["secret"])]);
                let outcome = engine(commit_every(4))
                    .run(plan, source, memory(&name).await)
                    .await;
                succeeded(&outcome);
                assert_eq!(outcome.report.rows, 5, "{name}");
                lands_nowhere(&name, &name, "people", "secret").await;
            }
        }
    }
}

#[tokio::test]
async fn a_column_the_table_holds_stays_and_holds_null_once_excluded() {
    let store = "excluded_later";
    let run = |plan: StreamPlan, first: i64| async move {
        let served = Served::new(Form::Arrow, vec![people(first, 2)]);
        let plan = pipeline(store, [plan.write(WriteMode::Append)]);
        let source = source(store, served).await;
        engine(commit_every(4))
            .run(plan, source, memory(store).await)
            .await
    };
    succeeded(&run(stream("people"), 0).await);
    succeeded(&run(stream("people").exclude(["note"]), 10).await);
    let columns = schema(store, "people").expect("the table exists");
    assert!(columns.field("note").is_some());
    let mut notes: Vec<(i64, Option<String>)> = Vec::new();
    for batch in published(store, "people") {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let held = batch.column_by_name("note").unwrap().as_string::<i32>();
        for row in 0..batch.num_rows() {
            notes.push((
                ids.value(row),
                held.is_valid(row).then(|| held.value(row).to_owned()),
            ));
        }
    }
    notes.sort();
    let written = |id: i64| (id, Some(format!("w{id}")));
    assert_eq!(notes, [written(0), written(1), (10, None), (11, None)]);
}

/// A change batch of `people`: each row's id, secret, value, and `n`, an update leaving the fields
/// `unchanged` flags as they were.
fn changes(rows: &[(i64, &str, i64, Option<&[u8]>)], first: u64) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let values: Vec<&str> = rows.iter().map(|row| row.1).collect();
    let ns: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let seqs = FixedSizeBinaryArray::try_from_iter((0..rows.len()).map(|row| {
        let mut seq = [0_u8; 16];
        seq[8..].copy_from_slice(&(first + row as u64).to_be_bytes());
        seq
    }))
    .expect("16 bytes each");
    let flags: BinaryArray = rows.iter().map(|row| row.3).collect();
    RecordBatch::try_from_iter([
        ("id", ints(&ids)),
        ("secret", texts(&values)),
        ("value", texts(&values)),
        ("n", ints(&ns)),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from(vec![1_i8; rows.len()])) as ArrayRef,
        ),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (UNCHANGED_COLUMN, Arc::new(flags) as ArrayRef),
    ])
    .expect("a valid change batch")
}

#[tokio::test]
async fn an_update_s_flags_move_past_an_excluded_column() {
    for honours in [false, true] {
        let store = format!("excluded_changes_{honours}");
        // The second update leaves `n`, field 3 of 7, as it was; once `secret` is excluded, `n`
        // is field 2, and a flag left at 3 would name the op column.
        let pushes = vec![
            changes(&[(1, "a", 1, None), (2, "b", 1, None)], 1),
            changes(&[(1, "c", 9, Some(&[0b1000])), (2, "d", 2, None)], 3),
        ];
        let mut served = Served::new(Form::Changes, pushes);
        served.honours = honours;
        served.key = Some(ColumnPath::from("id"));
        let plan = pipeline(
            &store,
            [stream("people")
                .read(ReadMode::Cdc)
                .write(WriteMode::Merge)
                .exclude(["secret"])],
        );
        let source = source(&store, served).await;
        let outcome = engine(commit_every(1))
            .run(plan, source, memory(&store).await)
            .await;
        succeeded(&outcome);
        let mut rows: Vec<(i64, String, i64)> = Vec::new();
        for batch in published(&store, "people") {
            assert!(batch.column_by_name("secret").is_none());
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>();
            let values = batch.column_by_name("value").unwrap().as_string::<i32>();
            let ns = batch
                .column_by_name("n")
                .unwrap()
                .as_primitive::<Int64Type>();
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), values.value(row).to_owned(), ns.value(row)));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            [(1, "c".to_owned(), 1), (2, "d".to_owned(), 2)],
            "{store}"
        );
    }
}

#[tokio::test]
async fn a_source_s_key_and_change_time_cannot_be_excluded_nor_the_columns_holding_them() {
    let path = |segments: &[&str]| ColumnPath::new(segments.iter().copied()).expect("a segment");
    let cases = [
        ("key", path(&["id"]), path(&["at"]), "id"),
        ("time", path(&["id"]), path(&["at"]), "at"),
        ("nested_key", path(&["a", "b"]), path(&["at"]), "a"),
        ("nested_time", path(&["id"]), path(&["t", "at"]), "t"),
    ];
    for (case, key, change_time, column) in cases {
        let store = format!("excluded_needed_{case}");
        let mut served = Served::new(Form::Arrow, vec![people(0, 1)]);
        served.key = Some(key);
        served.change_time = Some(change_time);
        let plan = pipeline(&store, [stream("people").exclude([column])]);
        let source = source(&store, served).await;
        let outcome = engine(commit_every(4))
            .run(plan, source, memory(&store).await)
            .await;
        assert_eq!(outcome.report.status, RunStatus::Failed, "{case}");
        let error = outcome.error.expect("the run failed");
        assert_eq!(error.kind(), ErrorKind::Config, "{error}");
        assert_eq!(error.code(), Some(codes::PLAN_EXCLUDE_INVALID), "{error}");
        assert!(schema(&store, "people").is_none(), "nothing is read");
    }
}

#[tokio::test]
async fn a_source_served_in_a_process_of_its_own_sends_no_excluded_column() {
    let source = spawned_generator(&[("events", 40, 1, 8)]).await;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("events").expect("a valid name"),
        Partition::new(PartitionId::parse("0").expect("a valid id")),
        None,
    )
    .excluding(Exclusion::new(["name", "value"]));
    let received = async {
        let mut columns = Vec::new();
        while let Some(event) = feed.recv().await {
            if let SourceEvent::Push(Push::Arrow(batch)) = event {
                let schema = batch.schema();
                columns.push(
                    schema
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect(),
                );
            }
        }
        columns
    };
    let (read, columns): (_, Vec<Vec<String>>) = tokio::join!(source.read(request, sink), received);
    read.expect("the read ends");
    assert_eq!(columns.len(), 5);
    for pushed in columns {
        assert_eq!(pushed, ["id"]);
    }
}
