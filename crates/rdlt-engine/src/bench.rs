//! Internals the benchmarks and fuzz targets drive; not part of the engine's API.

mod connectors;
mod corpora;
mod loading;
mod logged;
mod lowering;
mod measure;
mod mix;
mod normalized;
mod passthrough;
mod pools;
mod resident;
mod runner;
#[cfg(test)]
mod tests;
mod wide;

use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::ArrowError;
use bytes::Bytes;

use crate::compute::{Pool, ready};
use crate::env::Env;
use crate::normalize::{self, Shape};
use crate::shred::{self, ShredError};

use connectors::sink;

pub use connectors::{
    Replayed, SinkSession, SinkWriter, Sinking, ipc_sink, null_sink, register, replay,
    replay_config, replay_factory, sink_factory, split_replay_config,
};
pub use corpora::{CHUNK_BYTES, CORPUS_BYTES, Corpus, PUSH_BYTES};
pub use loading::{Load, Loading};
pub use logged::Logged;
pub use lowering::Lowering;
pub use measure::{Allocations, Paired, counted, logical_bytes};
pub use mix::Mix;
pub use normalized::Normalized;
pub use passthrough::Passthrough;
pub use pools::pool_cores;
pub use resident::{Resident, reset_peak, resident_kib};
pub use wide::{Form, Wide};

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

/// A batch of one column of JSON, `rows` rows of the nested corpus's objects, or of the
/// string-heavy corpus's where `escaped` says.
///
/// # Panics
///
/// Panics where `rows` is zero or the batch does not build, which it always does.
pub fn json_column(rows: usize, escaped: bool) -> RecordBatch {
    let corpus = if escaped {
        Corpus::StringHeavy
    } else {
        Corpus::Nested
    };
    let count = u64::try_from(rows).expect("rows fit");
    let per_push = std::num::NonZeroU64::new(count).expect("at least one row");
    let text = corpus.rows(count, per_push).concat();
    let text = String::from_utf8(text).expect("the corpus is UTF-8");
    let values = arrow_array::StringArray::from_iter_values(text.lines().collect::<Vec<_>>());
    let schema = Arc::new(arrow_schema::Schema::new(vec![crate::json::bench_field(
        "payload",
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("the batch builds")
}

/// Checks every value of `batch`'s columns of JSON, as an Arrow push's are checked.
///
/// # Errors
///
/// The first value that is not JSON, with its code.
pub fn check_json(batch: &RecordBatch) -> Result<(), Refused> {
    crate::json::check_batch(batch).map_err(|refused| Refused {
        code: refused.error.code(),
        message: refused.error.to_string(),
    })
}

/// Shreds the JSON `pushes` on the calling thread, in chunks of `chunk_bytes`.
pub fn shred(pushes: &[Bytes], chunk_bytes: usize) -> Result<Vec<RecordBatch>, Refused> {
    // One pool for every call, so a call allocates nothing for it.
    static INLINE: LazyLock<Pool> = LazyLock::new(Pool::inline);
    ready(shredded(&INLINE, pushes, chunk_bytes))
}

/// What shredding the JSON `pushes` on the calling thread, in chunks of `chunk_bytes`, took in
/// passes over its chunks.
///
/// # Errors
///
/// Why the shredder refused the pushes.
pub fn shred_counted(pushes: &[Bytes], chunk_bytes: usize) -> Result<crate::ShredCounts, Refused> {
    static INLINE: LazyLock<Pool> = LazyLock::new(Pool::inline);
    let limits = shred::ShredLimits::new(rdlt_wire::limits::SCHEMA_COLUMNS);
    let shredding = ready(shred::observe(&INLINE, pushes, chunk_bytes, limits))?;
    Ok(shredding.counts())
}

/// Shreds the JSON `pushes` on `env`'s compute pool, in chunks of `chunk_bytes`, as a partition
/// does.
pub async fn shred_on(
    env: &Arc<dyn Env>,
    pushes: &[Bytes],
    chunk_bytes: usize,
) -> Result<Vec<RecordBatch>, Refused> {
    let pool = Pool::new(Arc::clone(env), Arc::default());
    shredded(&pool, pushes, chunk_bytes).await
}

/// Shreds the JSON `pushes` on `pool`, in chunks of `chunk_bytes`.
async fn shredded(
    pool: &Pool,
    pushes: &[Bytes],
    chunk_bytes: usize,
) -> Result<Vec<RecordBatch>, Refused> {
    let limits = shred::ShredLimits::new(rdlt_wire::limits::SCHEMA_COLUMNS);
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

/// `batch` normalized as a keyless stream to `max_depth`, every `every`th root row dropped, and
/// each child table's rows pruned of those whose parent went: the rows left in all its tables.
///
/// # Errors
///
/// Where a value of a column of JSON identity cannot read.
pub fn pruned(
    batch: &RecordBatch,
    max_depth: u8,
    every: NonZeroUsize,
) -> Result<usize, ArrowError> {
    let shape = Shape {
        max_depth,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let mut parts = normalize::normalize(batch, &shape)?.into_iter();
    let Some(root) = parts.next() else {
        return Ok(0);
    };
    let root = normalize::Pruned::whole(root, normalize::PushKind::Arrow);
    let kept: BooleanArray = (0..root.part.batch.num_rows())
        .map(|row| Some(row % every.get() != 0))
        .collect();
    let mut dropped = normalize::Dropped::new(normalize::PushKind::Arrow);
    dropped.unkept(&root, &kept);
    let mut left = kept.true_count();
    for part in parts {
        left += dropped.prune(part)?.part.batch.num_rows();
    }
    Ok(left)
}

/// A write-ahead log store in memory, for counting what logging takes without a disk.
pub fn memory_wal() -> Arc<dyn crate::WalStore> {
    Arc::new(crate::wal::memory::MemoryWal::default())
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
    ready(store.open_log(&pipeline, load)).map_err(refused_wal)?;
    let mut staged = ready(store.stage(&pipeline, chunk)).map_err(refused_wal)?;
    ready(staged.append(Bytes::copy_from_slice(bytes))).map_err(refused_wal)?;
    ready(staged.publish()).map_err(refused_wal)?;
    let limits = frame::limits(crate::config::EngineConfig::default().memory().get());
    let scanned = ready(scan::scan(&store, &pipeline, load, limits.frame_bytes))
        .map_err(|error| refused(&error))?;
    for located in scanned.batches.values().flatten() {
        ready(scan::batch(&store, &pipeline, *located, limits))
            .and_then(scan::Read::decode)
            .map_err(|error| refused(&error))?;
    }
    Ok(scanned.commits.len())
}

/// `error` as a refusal: a log unreadable or foreign by its code, any other as `other`.
fn refused(error: &crate::error::Error) -> Refused {
    Refused {
        code: match error.code() {
            Some("wal_unreadable") => "wal_unreadable",
            Some("wal_foreign") => "wal_foreign",
            _ => "other",
        },
        message: error.to_string(),
    }
}

/// `error`, a log store's failure, as a refusal.
fn refused_wal(error: std::io::Error) -> Refused {
    refused(&crate::error::Error::from_wal(error))
}

/// What reading a load's log as a replay does took: the batch frames it read, the bytes the log
/// holds, the bytes read and the reads made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayRead {
    /// Batch frames read and decoded.
    pub batches: usize,
    /// Bytes the log's chunks hold.
    pub logged: u64,
    /// Bytes read from the store.
    pub read: u64,
    /// Read requests made of the store.
    pub reads: u64,
}

/// Reads the log of pipeline `pipeline`'s only load under `base` as replay does: scans it, then
/// reads and decodes each batch frame it found.
///
/// # Errors
///
/// Why the log does not read.
pub fn replay_scan(base: &std::path::Path, pipeline: &str) -> Result<ReplayRead, Refused> {
    use crate::report::Tally;
    use crate::wal::CountedStore;
    use crate::wal::{WalStore, frame, scan};
    let tally = Arc::new(Tally::default());
    let local: Arc<dyn WalStore> = Arc::new(crate::LocalWal::new(base));
    let store = CountedStore::new(local, Arc::clone(&tally));
    let pipeline = rdlt_connector::PipelineId::parse(pipeline).expect("a valid pipeline");
    // The local store reads on the blocking pool, so its calls need a runtime.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime starts");
    let loads = runtime
        .block_on(store.loads(&pipeline))
        .map_err(refused_wal)?;
    let [load] = loads.as_slice() else {
        return Err(Refused {
            code: "other",
            message: format!("{} logs where one was expected", loads.len()),
        });
    };
    let logged = runtime
        .block_on(store.chunks(&pipeline, *load))
        .map_err(refused_wal)?
        .iter()
        .map(|(_, len)| len)
        .sum();
    let limits = frame::limits(crate::config::EngineConfig::default().memory().get());
    let scanned = runtime
        .block_on(scan::scan(&store, &pipeline, *load, limits.frame_bytes))
        .map_err(|error| refused(&error))?;
    let mut batches = 0;
    for located in scanned.batches.values().flatten() {
        runtime
            .block_on(scan::batch(&store, &pipeline, *located, limits))
            .and_then(scan::Read::decode)
            .map_err(|error| refused(&error))?;
        batches += 1;
    }
    let log = tally.counters().log;
    Ok(ReplayRead {
        batches,
        logged,
        read: log.read,
        reads: log.requests.read,
    })
}

/// The write-ahead log frame of `batch`, as a load logs it.
///
/// # Panics
///
/// Panics where the frame does not encode, which a valid batch always does.
pub fn batch_frame(batch: RecordBatch) -> Bytes {
    use crate::wal::frame::{Batch, Frame};
    let frame = Frame::Batch(Batch {
        segment: rdlt_connector::SegmentId(1),
        table: 0,
        ordinal: 0,
        batch,
    });
    frame.encode().expect("the frame encodes")
}

/// The write-ahead log frame of a table of `columns` integer columns, as a load logs it.
///
/// # Panics
///
/// Panics where the frame does not encode, which a table of integer columns always does.
pub fn schema_frame(columns: usize) -> Bytes {
    use arrow_schema::{DataType, Field, Schema};
    use rdlt_connector::{SchemaVersion, TablePath, TableRef, TableSchema};

    use crate::wal::frame::{Frame, Table};
    let fields = (0..columns).map(|column| Field::new(format!("c{column}"), DataType::Int64, true));
    let schema = TableSchema::from_arrow(&Schema::new(fields.collect::<Vec<_>>()))
        .expect("integer columns map to a table's");
    let frame = Frame::Schema(Table {
        index: 0,
        table: TableRef {
            path: TablePath::new(["t"]).expect("a valid path"),
            name: "t".into(),
            version: SchemaVersion(1),
            generation: None,
            merge: None,
        },
        schema,
    });
    frame.encode().expect("the frame encodes")
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
        LoadId, PartitionId, PipelineId, SchemaVersion, SegmentId, StreamName, TablePath, TableRef,
        TableSchema,
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
            state: sample_position(),
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
    let mut framing = frame::Framing::default();
    framing.preamble();
    for frame in &frames {
        framing.frame(frame).expect("the frame encodes");
    }
    Vec::from(framing.finish())
}

/// The commit of segment 1 that [`sample_log`] holds, recording where its partition stands.
fn sample_commit(load: rdlt_connector::LoadId) -> rdlt_connector::CommitMeta {
    rdlt_connector::CommitMeta {
        load_id: load,
        commit_seq: rdlt_connector::CommitSeq::FIRST,
        epoch: rdlt_connector::Epoch(1),
        segments: [rdlt_connector::SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: vec![rdlt_connector::StateChange::Put(
            rdlt_connector::StateEntry::Partition {
                stream: rdlt_connector::StreamName::new("s").expect("a valid stream"),
                partition: rdlt_connector::PartitionId::parse("p").expect("a valid partition"),
                state: sample_position(),
                load,
            }
            .to_record(),
        )],
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    }
}

/// Where the partition of [`sample_log`] stands once its segment is sealed and committed.
fn sample_position() -> rdlt_connector::PartitionState {
    rdlt_connector::PartitionState::Cursor(
        rdlt_connector::Cursor::new(1, &[b'c'; 256]).expect("a cursor within the limit"),
    )
}
