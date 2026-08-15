//! Canonical fixtures: the single-`id`-column schema, its Arrow batch,
//! the commit envelope, and the ONE logical→Arrow derivation every
//! fixture in this crate routes through — the recovery and exactly-once
//! cells across six crates share these, so a change lands in one place.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector::core::{
    ColumnDef, ColumnType, CommitCounters, CommitMeta, LoadId, LogicalType, PipelineId, Provenance,
    StateDoc, TableName, TableSchema,
};

/// The canonical logical schema: one non-nullable `id: Int64` column.
pub fn schema_for(table: &str) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![ColumnDef {
            name: "id".into(),
            column_type: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    }
}

/// A one-column Arrow batch of the given ids. Derived from [`schema_for`]
/// through the crate's one logical→Arrow derivation, so the logical and
/// physical shapes cannot drift apart.
pub fn batch_of(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema(&schema_for("_"))),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("fixture batch")
}

/// A commit envelope for `(pipeline, load, seq)` with an otherwise-fresh
/// state doc — the shape the recovery/exactly-once cells commit.
pub fn commit_meta_for(pipeline: &PipelineId, load: &LoadId, seq: u64) -> CommitMeta {
    CommitMeta {
        load_id: load.clone(),
        commit_seq: seq,
        state: StateDoc::new(pipeline.clone(), "test"),
        counters: CommitCounters::default(),
    }
}

/// The ONE logical→Arrow schema derivation for fixtures. Only the scalar
/// types fixture schemas use are handled — any other type is a bug in a
/// FIXTURE, not a runtime input, and says so at the panic.
pub(crate) fn arrow_schema(logical: &TableSchema) -> Schema {
    Schema::new(
        logical
            .columns
            .iter()
            .map(arrow_field)
            .collect::<Vec<Field>>(),
    )
}

fn arrow_field(column: &ColumnDef) -> Field {
    let data_type = match &column.column_type {
        ColumnType::Scalar {
            scalar: LogicalType::Utf8,
        } => DataType::Utf8,
        ColumnType::Scalar {
            scalar: LogicalType::Int64,
        } => DataType::Int64,
        other => unreachable!("fixture schemas use only Utf8/Int64 columns, got {other:?}"),
    };
    Field::new(column.name.clone(), data_type, column.nullable)
}

/// The byte-meter comparator fixture (round-5 fix — three near-copies
/// kept three meters' comparators drifting-capable): a multi-buffer
/// batch (`IPC_FIXTURE_INTS` int64 columns plus one fixed-width string
/// column) and the EXACT bytes its rows require — the hard lower bound
/// any honest footprint respects. A capacity-summing meter charges the
/// one IPC message body once per buffer and lands far outside every
/// bound asserted against this fixture.
///
/// The one deliberate non-consumer: rdlt-connector's own channel unit
/// tests carry a private twin — the dependency direction (this crate
/// depends on rdlt-connector) forbids importing from here, and that
/// exception is commented at the twin.
pub mod ipc_fixture {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    /// Rows in the fixture batch.
    pub const IPC_FIXTURE_ROWS: usize = 2048;
    /// Int64 columns (one buffer each).
    pub const IPC_FIXTURE_INTS: usize = 5;
    /// The string column's fixed payload width per row.
    pub const IPC_FIXTURE_STRING_WIDTH: usize = 12;

    /// The multi-buffer batch and its raw row payload in bytes.
    pub fn wide_batch() -> (RecordBatch, usize) {
        let mut fields: Vec<Field> = (0..IPC_FIXTURE_INTS)
            .map(|i| Field::new(format!("n{i}"), DataType::Int64, false))
            .collect();
        fields.push(Field::new("s", DataType::Utf8, false));
        let mut columns: Vec<ArrayRef> = (0..IPC_FIXTURE_INTS)
            .map(|i| {
                Arc::new(Int64Array::from_iter_values(
                    (0..IPC_FIXTURE_ROWS as i64).map(|row| row + i as i64),
                )) as ArrayRef
            })
            .collect();
        columns.push(Arc::new(StringArray::from_iter_values(
            (0..IPC_FIXTURE_ROWS).map(|row| format!("row-{row:07}!")),
        )));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch");
        (
            batch,
            IPC_FIXTURE_ROWS * IPC_FIXTURE_INTS * 8 + IPC_FIXTURE_ROWS * IPC_FIXTURE_STRING_WIDTH,
        )
    }

    /// One IPC round trip of [`wide_batch`]: the stream's byte length
    /// (the honest comparator — it carries the whole message body the
    /// decoded batch's buffers are slices of), the decoded batch, and
    /// the raw row payload.
    ///
    /// The bare `StreamReader` below is the one decode in this crate
    /// OUTSIDE the shared pre-pass discipline (6.12) — deliberately:
    /// it decodes the bytes this same function just encoded, so there
    /// is no adversary between the writer and the reader. It joins the
    /// discipline the day it ever decodes foreign bytes.
    pub fn ipc_round_trip() -> (usize, RecordBatch, usize) {
        let (batch, row_payload) = wide_batch();
        let mut stream = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut stream, &batch.schema())
            .expect("stream writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish stream");
        drop(writer);
        let stream_len = stream.len();
        let decoded = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(stream), None)
            .expect("stream reader")
            .next()
            .expect("one batch in the stream")
            .expect("decodes");
        (stream_len, decoded, row_payload)
    }
}
