//! Internals the benchmarks and fuzz targets drive; not part of the engine's API.

mod connectors;
mod corpora;
mod lowering;
mod measure;
mod mix;
mod passthrough;
mod pools;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use bytes::Bytes;

use crate::compute::{ComputePool, Inline, ready};
use crate::normalize::{self, Shape};
use crate::shred::{self, ShredError};

pub use connectors::{
    Replayed, SinkSession, SinkWriter, Sinking, ipc_sink, null_sink, register, replay,
    replay_config, replay_factory, sink_factory,
};
pub use corpora::{CHUNK_BYTES, CORPUS_BYTES, Corpus, PUSH_BYTES};
pub use lowering::Lowering;
pub use measure::{Allocations, Paired, counted, logical_bytes};
pub use mix::Mix;
pub use passthrough::Passthrough;
pub use pools::pool_cores;

/// Why the shredder refused pushes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused {
    /// The error's machine code.
    pub code: &'static str,
    /// What went wrong.
    pub message: String,
}

impl From<ShredError> for Refused {
    fn from(error: ShredError) -> Self {
        Self {
            code: error.code(),
            message: error.to_string(),
        }
    }
}

/// Shreds the JSON `pushes` on the calling thread, in chunks of `chunk_bytes`.
pub fn shred(pushes: &[Bytes], chunk_bytes: usize) -> Result<Vec<RecordBatch>, Refused> {
    ready(shred_on(&Inline, pushes, chunk_bytes))
}

/// Shreds the JSON `pushes` on `pool`, in chunks of `chunk_bytes`, as a partition does.
pub async fn shred_on(
    pool: &dyn ComputePool,
    pushes: &[Bytes],
    chunk_bytes: usize,
) -> Result<Vec<RecordBatch>, Refused> {
    let limits = shred::ShredLimits::new(rdlt_connector::limits::MAX_COLUMNS);
    Ok(shred::shred(pool, pushes, chunk_bytes, limits).await?)
}

/// `batch` normalized as a stream normalized to `max_depth` whose rows `key` identifies: each
/// table's path below the stream's table, and its rows' data columns.
pub fn normalize(
    batch: &RecordBatch,
    max_depth: u8,
    key: &[&str],
) -> Result<Vec<(Vec<String>, RecordBatch)>, ArrowError> {
    let shape = Shape {
        max_depth,
        whole: std::collections::BTreeSet::new(),
        key: key.iter().map(|column| Arc::from(*column)).collect(),
    };
    Ok(normalize::normalize(batch, &shape)?
        .into_iter()
        .map(|part| {
            let path = part.path.iter().map(ToString::to_string).collect();
            (path, part.batch)
        })
        .collect())
}

/// Reads `bytes` as the only chunk of a write-ahead log, as replay reads one, then each batch it
/// found; the count of commit frames.
///
/// # Errors
///
/// Returns why the log cannot be read where it is not one the engine wrote.
pub fn scan_log(bytes: &[u8]) -> Result<usize, Refused> {
    use crate::wal::memory::MemoryWal;
    use crate::wal::{Chunk, WalStore, frame, scan};
    let store = MemoryWal::default();
    let pipeline = rdlt_connector::PipelineId::parse("fuzzed").expect("a valid pipeline");
    let load = rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 1);
    let chunk = Chunk { load, number: 0 };
    let code = |error: crate::error::Error| Refused {
        code: match error.code() {
            Some("wal_unreadable") => "wal_unreadable",
            Some("wal_foreign") => "wal_foreign",
            _ => "other",
        },
        message: error.to_string(),
    };
    let wal = |error: std::io::Error| code(crate::error::Error::from_wal(error));
    ready(store.open_log(&pipeline, load)).map_err(wal)?;
    let mut staged = ready(store.stage(&pipeline, chunk)).map_err(wal)?;
    ready(staged.append(Bytes::copy_from_slice(bytes))).map_err(wal)?;
    ready(staged.publish()).map_err(wal)?;
    let limits = frame::limits(crate::config::EngineConfig::default().memory().get());
    let scanned = ready(scan::scan(&store, &pipeline, load, limits.frame_bytes)).map_err(code)?;
    for located in scanned.batches.values().flatten() {
        ready(scan::batch(&store, &pipeline, *located, limits))
            .and_then(scan::Read::decode)
            .map_err(code)?;
    }
    Ok(scanned.commits.len())
}

/// A write-ahead log of one chunk holding a frame of each kind a commit's chunk holds, as the log
/// of load 1 of pipeline `fuzzed`, whose batch frame holds `batch`: something for fuzzing to cut
/// and garble.
///
/// # Panics
///
/// Panics where a frame does not encode, which a valid batch always does.
pub fn sample_log(batch: RecordBatch) -> Vec<u8> {
    use rdlt_connector::{
        LoadId, PartitionId, PartitionState, PipelineId, SchemaVersion, SegmentId, StreamName,
        TablePath, TableRef, TableSchema,
    };

    use crate::wal::frame::{self, Committing, End, Frame, Header};
    let load = LoadId::from_parts(std::time::UNIX_EPOCH, 1);
    let table = TableRef {
        path: TablePath::new(["t"]).expect("a valid path"),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let schema = TableSchema::from_arrow(&batch.schema())
        .unwrap_or_else(|_| TableSchema::new(Vec::new()).expect("an empty schema is valid"));
    let rows = u64::try_from(batch.num_rows()).expect("rows fit");
    let frames = [
        Frame::Header(Header {
            pipeline: PipelineId::parse("fuzzed").expect("a valid pipeline"),
            load,
            chunk: 0,
            epoch: rdlt_connector::Epoch(1),
            opened: None,
            origin: load,
        }),
        Frame::Schema(frame::Table {
            index: 0,
            table,
            schema,
        }),
        Frame::Batch(frame::Batch {
            segment: SegmentId(1),
            table: 0,
            ordinal: 0,
            batch,
        }),
        Frame::Seal(frame::Seal {
            segment: SegmentId(1),
            stream: StreamName::new("s").expect("a valid stream"),
            partition: PartitionId::parse("p").expect("a valid partition"),
            replayable: true,
            phase: 0,
            from: None,
            state: PartitionState::Done,
            batches: 1,
            rows,
        }),
        Frame::Commit(Box::new(Committing {
            meta: sample_commit(load),
            seals: 1,
            phases: 0,
        })),
        Frame::End(End::default()),
    ];
    let mut log = frame::preamble().to_vec();
    for frame in &frames {
        log.extend_from_slice(&frame.encode().expect("the frame encodes"));
    }
    log
}

/// The commit of segment 1 that [`sample_log`] holds.
fn sample_commit(load: rdlt_connector::LoadId) -> rdlt_connector::CommitMeta {
    rdlt_connector::CommitMeta {
        load_id: load,
        commit_seq: rdlt_connector::CommitSeq::FIRST,
        epoch: rdlt_connector::Epoch(1),
        segments: [rdlt_connector::SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    }
}
