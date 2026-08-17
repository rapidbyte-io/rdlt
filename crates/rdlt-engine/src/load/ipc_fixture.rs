//! The byte-meter comparator fixture, shared by the load stage's meters
//! so their comparators cannot drift apart: a multi-buffer batch
//! (`IPC_FIXTURE_INTS` int64 columns plus one fixed-width string column)
//! and the EXACT bytes its rows require — the hard lower bound any honest
//! footprint respects. A capacity-summing meter charges the one IPC
//! message body once per buffer and lands far outside every bound
//! asserted against this fixture.
//!
//! The connector channel's own unit tests carry a private twin: the
//! dependency direction forbids importing from here, and that exception
//! is commented at the twin.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

/// Rows in the fixture batch.
pub(crate) const IPC_FIXTURE_ROWS: usize = 2048;
/// Int64 columns (one buffer each).
pub(crate) const IPC_FIXTURE_INTS: usize = 5;
/// The string column's fixed payload width per row.
pub(crate) const IPC_FIXTURE_STRING_WIDTH: usize = 12;

/// The multi-buffer batch and its raw row payload in bytes.
pub(crate) fn wide_batch() -> (RecordBatch, usize) {
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

/// One IPC round trip of [`wide_batch`]: the stream's byte length (the
/// honest comparator — it carries the whole message body the decoded
/// batch's buffers are slices of), the decoded batch, and the raw row
/// payload.
///
/// The bare `StreamReader` below sits outside the shared IPC pre-pass
/// discipline deliberately: it decodes the bytes this same function just
/// encoded, so there is no adversary between the writer and the reader.
/// It joins the discipline the day it ever decodes foreign bytes.
pub(crate) fn ipc_round_trip() -> (usize, RecordBatch, usize) {
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
