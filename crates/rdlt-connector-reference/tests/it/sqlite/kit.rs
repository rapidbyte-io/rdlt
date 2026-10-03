//! Sessions of several pipelines on one SQLite database, and what the database then holds.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, ConnectorError, ConnectorErrorKind, Destination, Field,
    GenerationId, LoadId, LogicalType, MergeKey, OpenContext, OpenedSession, PipelineId, Receipt,
    SchemaVersion, SegmentId, TableChange, TablePath, TableRef, TableSchema, destination_factory,
};
use rdlt_connector_reference::{SqliteDestination, sqlite};
use serde_json::json;

/// One database file and the destination connected to it.
pub(super) struct Shared {
    _directory: tempfile::TempDir,
    pub(super) path: PathBuf,
    pub(super) destination: Box<dyn Destination>,
}

impl Shared {
    pub(super) async fn new() -> Self {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("shared.db");
        let destination = connect(&path).await;
        Self {
            _directory: directory,
            path,
            destination,
        }
    }

    /// A session of `pipeline`, whose commits are load `load`'s.
    pub(super) async fn open(&self, pipeline: &str, load: u128) -> Opened {
        let context = OpenContext {
            pipeline: PipelineId::parse(pipeline).expect("a valid pipeline id"),
            load_id: LoadId::from_parts(UNIX_EPOCH, load),
        };
        let session = self
            .destination
            .open(&context)
            .await
            .expect("the session opens");
        Opened {
            session,
            load_id: context.load_id,
            seq: CommitSeq::FIRST,
        }
    }

    /// The `id`s the table `name` holds, ascending.
    pub(super) fn ids(&self, name: &str) -> Vec<i64> {
        let mut ids: Vec<i64> = sqlite::published(&self.path, name)
            .expect("the table reads")
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name("id").expect("an id column");
                column
                    .as_primitive::<Int64Type>()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// The name of every table and index the database holds, ascending.
    pub(super) fn objects(&self) -> Vec<String> {
        self.texts("SELECT name FROM sqlite_schema ORDER BY name")
    }

    /// The first column of each row `sql` returns, as text.
    pub(super) fn texts(&self, sql: &str) -> Vec<String> {
        let connection = rusqlite::Connection::open(&self.path).expect("the database opens");
        let mut statement = connection.prepare(sql).expect("the query prepares");
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .expect("the query runs")
            .collect::<Result<_, _>>()
            .expect("the rows read")
    }

    /// The number `sql` returns.
    pub(super) fn count(&self, sql: &str) -> i64 {
        let connection = rusqlite::Connection::open(&self.path).expect("the database opens");
        connection
            .query_row(sql, [], |row| row.get(0))
            .expect("the count reads")
    }

    /// Runs `sql` on a connection of its own, as another program sharing the file would.
    pub(super) fn execute(&self, sql: &str) {
        let connection = rusqlite::Connection::open(&self.path).expect("the database opens");
        connection.execute_batch(sql).expect("the statements run");
    }
}

pub(super) async fn connect(path: &Path) -> Box<dyn Destination> {
    destination_factory::<SqliteDestination>()
        .connect(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the sqlite destination connects")
}

/// A session and the commits it has made.
pub(super) struct Opened {
    pub(super) session: OpenedSession,
    load_id: LoadId,
    seq: CommitSeq,
}

impl Opened {
    /// Creates `table` with an `id` and a `seq` column.
    pub(super) async fn create(&mut self, table: &TableRef) -> Result<(), ConnectorError> {
        self.session.session.apply_schema(&create(table)).await
    }

    /// Creates `table` and stages `ids` for it in `segment`, each sequenced by its id.
    pub(super) async fn stage(&mut self, table: &TableRef, segment: u64, ids: &[i64]) {
        self.create(table).await.expect("the table is created");
        let mut writer = self
            .session
            .session
            .writer(table)
            .await
            .expect("a writer opens");
        writer
            .write(SegmentId(segment), rows(ids))
            .await
            .expect("the write buffers");
        writer.flush().await.expect("the flush stages");
    }

    /// The session's next commit, of `segments`.
    pub(super) fn meta(&mut self, segments: &[u64]) -> CommitMeta {
        let meta = CommitMeta {
            load_id: self.load_id,
            commit_seq: self.seq,
            epoch: self.session.epoch,
            segments: segments.iter().copied().map(SegmentId).collect(),
            state_delta: Vec::new(),
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
            drop_tables: Vec::new(),
            horizon: None,
        };
        self.seq = self.seq.next();
        meta
    }

    pub(super) async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt, ConnectorError> {
        self.session.session.commit(meta).await
    }

    /// Stages `ids` for `table` in `segment` and commits them.
    pub(super) async fn load(&mut self, table: &TableRef, segment: u64, ids: &[i64]) {
        self.stage(table, segment, ids).await;
        let meta = self.meta(&[segment]);
        self.commit(&meta).await.expect("the commit lands");
    }
}

/// The table `name` at the path `path`, merging by `id` where `merge`.
pub(super) fn table(name: &str, path: &str, merge: bool) -> TableRef {
    TableRef {
        path: TablePath::new([path]).expect("a valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: merge.then(by_id),
    }
}

/// `table` as generation `generation` of itself.
pub(super) fn generation(table: &TableRef, generation: u64) -> TableRef {
    TableRef {
        generation: Some(GenerationId(generation)),
        ..table.clone()
    }
}

pub(super) fn by_id() -> MergeKey {
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    }
}

pub(super) fn create(table: &TableRef) -> TableChange {
    let fields = vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("seq", LogicalType::Binary, false),
    ];
    TableChange::Create {
        table: table.clone(),
        schema: TableSchema::new(fields).expect("the schema is valid"),
    }
}

/// Rows of `ids`, each sequenced by its id.
pub(super) fn rows(ids: &[i64]) -> RecordBatch {
    let seqs = ids.iter().map(|id| {
        let mut seq = [0_u8; 16];
        seq[8..].copy_from_slice(&id.to_be_bytes());
        seq
    });
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
        ("seq", Arc::new(BinaryArray::from_iter_values(seqs)) as _),
    ])
    .expect("a valid batch")
}

/// The kind and code `outcome` was refused with.
pub(super) fn refusal<T: std::fmt::Debug>(
    outcome: Result<T, ConnectorError>,
) -> (ConnectorErrorKind, Option<String>) {
    let error = outcome.expect_err("the call is refused");
    (error.kind(), error.code().map(str::to_owned))
}

/// A `Config` refusal coded `code`.
pub(super) fn config(code: &str) -> (ConnectorErrorKind, Option<String>) {
    (ConnectorErrorKind::Config, Some(code.to_owned()))
}
