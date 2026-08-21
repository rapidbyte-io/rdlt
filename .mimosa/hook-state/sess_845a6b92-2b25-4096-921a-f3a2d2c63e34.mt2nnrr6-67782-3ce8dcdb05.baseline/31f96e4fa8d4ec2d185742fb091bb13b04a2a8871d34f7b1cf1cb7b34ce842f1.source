//! Test support shared across the crate's unit tests: the ONE `LoadSession`
//! fake, manifest writers for scan fixtures, the byte-meter comparator
//! fixture, and a one-column batch. Compiled under `cfg(test)` only.

use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector::destination::LoadSession;
use rdlt_connector::error::DestinationError;
use rdlt_core::commit::{CommitMeta, CommitReceipt, WriteMode};
use rdlt_core::id::{LoadId, PipelineId, TableName};
use rdlt_core::schema::{IdentRules, TableSchema};
use rdlt_core::state::StateDoc;

use crate::wal::format::{WAL_FORMAT_VERSION, WalRecord, encode_line};

/// The ONE session fake: accepts every call, records commits and counts
/// writes, runs an optional hook on each `ensure_table`, and — as
/// [`Self::unreachable`] — refuses every call for pins asserting no
/// destination call is reachable.
#[derive(Default)]
pub(crate) struct FakeSession {
    pub(crate) commits: Arc<Mutex<Vec<CommitMeta>>>,
    pub(crate) writes: Arc<Mutex<usize>>,
    on_ensure: Option<Box<dyn FnMut() + Send>>,
    unreachable: bool,
}

impl FakeSession {
    /// A session that refuses every call — the pin that a code path never
    /// touches the destination.
    pub(crate) fn unreachable() -> Self {
        Self {
            unreachable: true,
            ..Self::default()
        }
    }

    /// Run `hook` on every `ensure_table` — the seam a pin uses to act
    /// between a caller's phases.
    pub(crate) fn on_ensure(mut self, hook: impl FnMut() + Send + 'static) -> Self {
        self.on_ensure = Some(Box::new(hook));
        self
    }
}

#[async_trait::async_trait]
impl LoadSession for FakeSession {
    async fn ensure_table(
        &mut self,
        _: &TableSchema,
        _: &WriteMode,
    ) -> Result<(), DestinationError> {
        assert!(!self.unreachable, "this session must never be touched");
        if let Some(hook) = &mut self.on_ensure {
            hook();
        }
        Ok(())
    }
    async fn write(&mut self, _: &TableName, _: RecordBatch) -> Result<(), DestinationError> {
        assert!(!self.unreachable, "this session must never be touched");
        *self.writes.lock().expect("writes lock") += 1;
        Ok(())
    }
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        assert!(!self.unreachable, "this session must never be touched");
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        self.commits.lock().expect("commits lock").push(meta);
        Ok(receipt)
    }
    async fn read_state(&mut self, _: &PipelineId) -> Result<Option<StateDoc>, DestinationError> {
        assert!(!self.unreachable, "this session must never be touched");
        Ok(None)
    }
}

/// A current-version `Run` header for `load` under pipeline `p`.
pub(crate) fn run_header(load: &str) -> WalRecord {
    WalRecord::Run {
        format_version: WAL_FORMAT_VERSION,
        load_id: LoadId::new(load),
        pipeline: PipelineId::new("p"),
    }
}

/// Write `records` as the writer's own checksummed lines to `manifest.jsonl`
/// in `dir`, with `rules` recorded in the sidecar beside it — the shape every
/// writer leaves.
pub(crate) fn write_manifest(dir: &Path, records: &[WalRecord], rules: IdentRules) {
    let mut out: Vec<u8> = Vec::new();
    for record in records {
        out.extend_from_slice(&encode_line(record).expect("record json"));
        out.push(b'\n');
    }
    std::fs::write(dir.join("manifest.jsonl"), out).expect("write manifest");
    std::fs::write(
        dir.join(crate::wal::dir::RULES_SIDECAR),
        serde_json::to_vec(&rules).expect("rules json"),
    )
    .expect("write sidecar");
}

/// One `id: Int64` column holding `0..rows`.
pub(crate) fn int_batch(rows: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch")
}

/// The byte-meter comparator fixture, shared by the load stage's meters so
/// their comparators cannot drift apart: a multi-buffer batch (five int64
/// columns plus one fixed-width string column) and the EXACT bytes its rows
/// require — the hard lower bound any honest footprint respects. A
/// capacity-summing meter charges the one IPC message body once per buffer
/// and lands far outside every bound asserted against this fixture. The
/// connector channel's own unit tests carry a private twin: the dependency
/// direction forbids importing from here.
const IPC_FIXTURE_ROWS: usize = 2048;
const IPC_FIXTURE_INTS: usize = 5;
const IPC_FIXTURE_STRING_WIDTH: usize = 12;

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
