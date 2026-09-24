//! The simulated source: serves the world's workload, with faults.

use std::sync::Arc;

use arrow_array::builder::{Int64Builder, ListBuilder};
use arrow_array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field as ArrowField};
use bytes::Bytes;
use rdlt_connector::{
    Checkpointing, ConnectContext, ConnectorError, Emitter, Field, LogicalType, Partition,
    PartitionId, Partitioning, ReadMode, ReadStream, Result, SourceConnector, StreamName,
    StreamSpec, StreamState, Streams, TableSchema,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::destination::committed_next;
use crate::workload::{Extra, Row, Shape, SimStream};
use crate::world::{FaultPoint, World};

/// Configuration of [`SimSource`]: the world to serve.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct SimSourceConfig {
    /// The world's registered name.
    pub world: String,
}

/// Serves every stream of a world's workload.
#[derive(Debug)]
pub struct SimSource {
    world: Arc<World>,
}

/// Where a simulated partition resumes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimCursor {
    /// The offset of the next row to read.
    pub next: u64,
}

impl SourceConnector for SimSource {
    const ID: &'static str = "io.rapidbyte.sim";
    const VERSION: &'static str = "0.0.0";
    type Config = SimSourceConfig;

    async fn connect(config: SimSourceConfig, _context: &ConnectContext) -> Result<Self> {
        let world = World::named(&config.world)
            .ok_or_else(|| ConnectorError::config(format!("no world named {}", config.world)))?;
        Ok(Self { world })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        let streams = self.world.workload.streams.iter().enumerate();
        streams.fold(Streams::new(), |streams, (index, stream)| {
            streams.with(SimStreamReader {
                index,
                checkpointing: stream.checkpointing,
                merge: stream.keys > 0,
                plan_key: stream.plan_key,
            })
        })
    }
}

/// The schema a simulated stream declares: its base columns, and the key of a merge stream.
///
/// Drift columns are left out, so they are schema changes the pipeline's policy handles.
#[expect(
    clippy::missing_panics_doc,
    reason = "distinct column names always make a schema"
)]
pub fn schema(merge: bool) -> TableSchema {
    let column = |name| Field::new(name, LogicalType::Int64, false);
    let mut columns = vec![
        column("id"),
        column("partition"),
        column("offset"),
        column("value"),
    ];
    if merge {
        columns.push(column("key"));
    }
    TableSchema::new(columns).expect("the simulated schema has distinct names")
}

struct SimStreamReader {
    index: usize,
    checkpointing: Checkpointing,
    merge: bool,
    plan_key: bool,
}

impl SimStreamReader {
    fn stream<'a>(&self, source: &'a SimSource) -> &'a SimStream {
        &source.world.workload.streams[self.index]
    }
}

impl ReadStream<SimSource> for SimStreamReader {
    type Cursor = SimCursor;

    fn spec(&self) -> StreamSpec {
        let name = format!("s{}", self.index);
        let spec =
            StreamSpec::new(StreamName::new(name).expect("simulated stream names are valid"))
                .with_schema(schema(self.merge))
                .with_read_modes([ReadMode::Full, ReadMode::Incremental])
                .with_partitioning(Partitioning::Planned)
                .with_checkpointing(self.checkpointing);
        if self.merge && !self.plan_key {
            spec.with_primary_key(["key"])
        } else {
            spec
        }
    }

    async fn partitions(&self, source: &SimSource, _state: &StreamState) -> Result<Vec<Partition>> {
        let count = self.stream(source).partitions.len();
        Ok((0..count)
            .map(|index| {
                Partition::new(PartitionId::parse(format!("p{index}")).expect("valid partition id"))
            })
            .collect())
    }

    async fn read(
        &self,
        source: &SimSource,
        partition: &Partition,
        cursor: SimCursor,
        out: &mut Emitter<SimCursor>,
    ) -> Result<()> {
        let world = &source.world;
        let stream = self.stream(source);
        let index = partition_index(partition)?;
        let rows = stream.rows(world.workload.salt, index, world.phase());
        let mut next = usize::try_from(cursor.next).unwrap_or(usize::MAX);
        let mut batches = 0;
        while next < rows.len() {
            world.latency().await;
            if let Some(fault) = world.fault(FaultPoint::Read) {
                return Err(fault);
            }
            let end = (next + usize::try_from(stream.batch_rows).unwrap_or(1)).min(rows.len());
            if stream.json {
                out.json(json_push(stream, &rows[next..end], batches % 2 == 1))
                    .await?;
            } else {
                out.batch(batch(stream, &rows[next..end])).await?;
            }
            next = end;
            batches += 1;
            let due = match stream.checkpointing {
                Checkpointing::OnDemand => out.checkpoint_due(),
                Checkpointing::Natural => batches % stream.checkpoint_every == 0,
            };
            if due {
                out.checkpoint(&SimCursor { next: next as u64 }).await?;
            }
        }
        if stream.final_checkpoint {
            out.checkpoint(&SimCursor { next: next as u64 }).await?;
        }
        Ok(())
    }

    async fn committed(
        &self,
        source: &SimSource,
        cursors: &[(PartitionId, SimCursor)],
    ) -> Result<()> {
        let world = &source.world;
        let name = StreamName::new(&self.stream(source).name).expect("valid stream name");
        for (partition, cursor) in cursors {
            let committed = committed_next(world, &name, partition);
            if committed.is_none_or(|committed| cursor.next > committed) {
                world.violation(format!(
                    "stream {name} partition {partition}: acknowledged offset {} beyond the \
                     committed {committed:?}",
                    cursor.next
                ));
            }
        }
        match world.fault(FaultPoint::Acknowledge) {
            Some(fault) => Err(fault),
            None => Ok(()),
        }
    }
}

fn partition_index(partition: &Partition) -> Result<usize> {
    partition
        .id()
        .as_str()
        .strip_prefix('p')
        .and_then(|index| index.parse().ok())
        .ok_or_else(|| ConnectorError::data(format!("no partition {}", partition.id())))
}

/// `rows` of `stream` as one batch: the base columns, the key of a merge stream, and every drift
/// column present where the rows were delivered.
fn batch(stream: &SimStream, rows: &[Row]) -> RecordBatch {
    let column =
        |value: fn(&Row) -> i64| Arc::new(Int64Array::from_iter_values(rows.iter().map(value)));
    let mut columns: Vec<(String, ArrayRef)> = vec![
        ("id".to_owned(), column(|row| row.id)),
        ("partition".to_owned(), column(|row| row.partition)),
        ("offset".to_owned(), column(|row| row.offset)),
        ("value".to_owned(), column(|row| row.value)),
    ];
    if stream.keys > 0 {
        columns.push(("key".to_owned(), column(|row| row.key.unwrap_or_default())));
    }
    let (partition, delivered) = rows.first().map_or((0, 0), |row| {
        (usize::try_from(row.partition).unwrap_or(0), row.delivered)
    });
    for (index, drift) in stream.drift.iter().enumerate() {
        if let Some(shape) = drift.shapes[partition][delivered] {
            let values: Vec<Option<&Extra>> =
                rows.iter().map(|row| row.extras[index].as_ref()).collect();
            columns.push((drift.name.clone(), array(shape, &values)));
        }
    }
    RecordBatch::try_from_iter(columns).expect("equal-length columns make a batch")
}

/// `rows` of `stream` as a JSON push with the columns a batch of them has: a JSON array when
/// `array`, else JSON lines.
fn json_push(stream: &SimStream, rows: &[Row], array: bool) -> Bytes {
    let (partition, delivered) = rows.first().map_or((0, 0), |row| {
        (usize::try_from(row.partition).unwrap_or(0), row.delivered)
    });
    let objects = rows.iter().map(|row| {
        let mut object = Map::new();
        object.insert("id".to_owned(), json!(row.id));
        object.insert("partition".to_owned(), json!(row.partition));
        object.insert("offset".to_owned(), json!(row.offset));
        object.insert("value".to_owned(), json!(row.value));
        if stream.keys > 0 {
            object.insert("key".to_owned(), json!(row.key.unwrap_or_default()));
        }
        for (index, drift) in stream.drift.iter().enumerate() {
            if let Some(shape) = drift.shapes[partition][delivered] {
                let value = row.extras[index]
                    .as_ref()
                    .map_or(Value::Null, |extra| rendered(shape, extra));
                object.insert(drift.name.clone(), value);
            }
        }
        Value::Object(object).to_string()
    });
    let objects: Vec<String> = objects.collect();
    Bytes::from(if array {
        format!("[{}]", objects.join(","))
    } else {
        objects.join("\n")
    })
}

/// `extra` as JSON, as a column of `shape` holds it.
fn rendered(shape: Shape, extra: &Extra) -> Value {
    match (shape, extra) {
        (Shape::Int32, _) => json!(i32::try_from(integer(extra)).unwrap_or(0)),
        (Shape::Int64, _) => json!(integer(extra)),
        (Shape::Float, _) => json!(f64::from(i32::try_from(integer(extra)).unwrap_or(0)) / 4.0),
        (Shape::Text, Extra::Text(text)) => json!(text),
        (Shape::Object, _) => json!({ "n": integer(extra) }),
        (Shape::List, Extra::List(items)) => json!(items),
        (Shape::Text | Shape::List, _) => Value::Null,
    }
}

/// The integer a numeric column of any shape holds for `extra`.
fn integer(extra: &Extra) -> i64 {
    match extra {
        Extra::Int(value) | Extra::Quarters(value) | Extra::Object(value) => *value,
        Extra::Text(_) | Extra::List(_) => 0,
    }
}

/// `values` as an array of `shape`.
fn array(shape: Shape, values: &[Option<&Extra>]) -> ArrayRef {
    let int = integer;
    match shape {
        Shape::Int32 => {
            Arc::new(Int32Array::from_iter(values.iter().map(|value| {
                value.map(|extra| i32::try_from(int(extra)).unwrap_or(0))
            })))
        }
        Shape::Int64 => Arc::new(Int64Array::from_iter(
            values.iter().map(|value| value.map(int)),
        )),
        Shape::Float => Arc::new(Float64Array::from_iter(values.iter().map(|value| {
            value.map(|extra| {
                let quarters = i32::try_from(int(extra)).unwrap_or(0);
                f64::from(quarters) / 4.0
            })
        }))),
        Shape::Text => Arc::new(StringArray::from_iter(values.iter().map(|value| {
            value.and_then(|extra| match extra {
                Extra::Text(text) => Some(text.as_str()),
                _ => None,
            })
        }))),
        Shape::Object => {
            let n: ArrayRef = Arc::new(Int64Array::from_iter(
                values.iter().map(|value| value.map(int)),
            ));
            let field = Arc::new(ArrowField::new("n", DataType::Int64, true));
            let nulls = values.iter().map(Option::is_some).collect::<Vec<_>>();
            Arc::new(StructArray::new(
                vec![field].into(),
                vec![n],
                Some(nulls.into()),
            ))
        }
        Shape::List => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for value in values {
                match value {
                    Some(Extra::List(items)) => {
                        builder.values().append_slice(items);
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Arc::new(builder.finish())
        }
    }
}
