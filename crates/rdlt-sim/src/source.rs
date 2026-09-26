//! The simulated source: serves the world's workload, with faults.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field as ArrowField, Schema};
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
use rdlt_testkit::drawn::json::rendered;
use rdlt_testkit::drawn::{Scalar, array, field};

use crate::workload::{Row, SimStream};
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
                schema: schema(stream),
                merge: stream.keys > 0,
                plan_key: stream.plan_key,
            })
        })
    }
}

/// The schema a simulated stream declares: its base columns, the key of a merge stream, and the
/// drift columns it declares.
///
/// Other drift columns are left out, so they are schema changes the pipeline's policy handles.
#[expect(
    clippy::missing_panics_doc,
    reason = "distinct column names always make a schema"
)]
pub fn schema(stream: &SimStream) -> TableSchema {
    let column = |name| Field::new(name, LogicalType::Int64, false);
    let mut columns = vec![
        column("id"),
        column("partition"),
        column("offset"),
        column("value"),
    ];
    if stream.keys > 0 {
        columns.push(column("key"));
    }
    for drift in &stream.drift {
        if let Some(declared) = &drift.declared {
            columns.push(Field::new(drift.name.as_str(), declared.clone(), true));
        }
    }
    TableSchema::new(columns).expect("the simulated schema has distinct names")
}

struct SimStreamReader {
    index: usize,
    checkpointing: Checkpointing,
    schema: TableSchema,
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
                .with_schema(self.schema.clone())
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
        let rows = stream.rows(index, world.phase());
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
/// column present where the rows were delivered; for a sliced stream, a slice of a batch holding
/// the rows three times.
fn batch(stream: &SimStream, rows: &[Row]) -> RecordBatch {
    if !stream.sliced {
        return whole(stream, rows);
    }
    let tripled: Vec<Row> = rows.iter().chain(rows).chain(rows).cloned().collect();
    whole(stream, &tripled).slice(rows.len(), rows.len())
}

fn whole(stream: &SimStream, rows: &[Row]) -> RecordBatch {
    let column =
        |value: fn(&Row) -> i64| Arc::new(Int64Array::from_iter_values(rows.iter().map(value)));
    let base = |name: &str| ArrowField::new(name, DataType::Int64, false);
    let mut fields = vec![base("id"), base("partition"), base("offset"), base("value")];
    let mut columns: Vec<ArrayRef> = vec![
        column(|row| row.id),
        column(|row| row.partition),
        column(|row| row.offset),
        column(|row| row.value),
    ];
    if stream.keys > 0 {
        fields.push(base("key"));
        columns.push(column(|row| row.key.unwrap_or_default()));
    }
    let (partition, delivered) = rows.first().map_or((0, 0), |row| {
        (usize::try_from(row.partition).unwrap_or(0), row.delivered)
    });
    for (index, drift) in stream.drift.iter().enumerate() {
        if let Some(shape) = &drift.shapes[partition][delivered] {
            let values: Vec<&Scalar> = rows
                .iter()
                .map(|row| row.extras[index].as_ref().unwrap_or(&Scalar::Null))
                .collect();
            let array = array(shape, &values);
            fields.push(field(&drift.name, shape, &array, true));
            columns.push(array);
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), columns, &options)
        .expect("equal-length columns make a batch")
}

/// `rows` of `stream` as a JSON push with the columns a batch of them has: a JSON array when
/// `array`, else JSON lines.
fn json_push(stream: &SimStream, rows: &[Row], array: bool) -> Bytes {
    let objects = rows.iter().map(|row| {
        let mut object = Map::new();
        object.insert("id".to_owned(), json!(row.id));
        object.insert("partition".to_owned(), json!(row.partition));
        object.insert("offset".to_owned(), json!(row.offset));
        object.insert("value".to_owned(), json!(row.value));
        if stream.keys > 0 {
            object.insert("key".to_owned(), json!(row.key.unwrap_or_default()));
        }
        for (drift, extra) in stream.drift.iter().zip(&row.extras) {
            if let Some(extra) = extra {
                object.insert(drift.name.clone(), rendered(extra));
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
