//! A source serving the stream `people` as a test says: Arrow, JSON or change pushes, leaving
//! out the columns a read excludes where it honours them, and declaring a schema, a key and a
//! change time where asked.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{
    ColumnPath, ConnectContext, ConnectorError, Emitter, Exclusion, Field, LogicalType, Partition,
    ReadMode, ReadStream, Result, Source, SourceConnector, StreamName, StreamSpec, Streams,
    TableSchema, UNCHANGED_COLUMN, remap_unchanged, source_factory,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

/// How the test's source pushes its stream, `people`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Form {
    Arrow,
    Json,
    Changes,
}

/// What a test's source serves: its pushes, in one form; whether it leaves out the columns the
/// stream excludes, as a source that honours the projection does; and the schema, key and change
/// time its catalog declares, if any.
#[derive(Clone, Debug)]
pub(crate) struct Served {
    pub(crate) form: Form,
    pub(crate) pushes: Vec<RecordBatch>,
    pub(crate) honours: bool,
    pub(crate) declared: Option<TableSchema>,
    pub(crate) key: Option<ColumnPath>,
    pub(crate) change_time: Option<ColumnPath>,
}

impl Served {
    pub(crate) fn new(form: Form, pushes: Vec<RecordBatch>) -> Self {
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
pub(crate) async fn source(name: &str, served: Served) -> Arc<dyn Source> {
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
        let mut spec = StreamSpec::new(name).with_read_modes([
            ReadMode::Full,
            ReadMode::Incremental,
            ReadMode::Cdc,
        ]);
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

pub(crate) fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

pub(crate) fn texts(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// `rows` people, from `first`: an id, a secret and a note.
pub(crate) fn people(first: i64, rows: i64) -> RecordBatch {
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
pub(crate) fn people_schema() -> TableSchema {
    TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("secret", LogicalType::Utf8, true),
        Field::new("note", LogicalType::Utf8, true),
    ])
    .expect("distinct names")
}
