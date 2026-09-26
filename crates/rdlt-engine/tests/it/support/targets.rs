//! The reference destinations the destination-facing tests run against, in process and spawned in
//! a process of their own, each keeping a test's stores apart.

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, Int64Array, RecordBatch};
use rdlt_connector::{ConnectContext, ConnectorId, Destination, destination_factory};
use rdlt_connector_reference::{
    FilesDestination, MemoryDestination, SqliteDestination, files, published, sqlite,
};
use rdlt_host::{ConnectorRef, Provider as _};
use serde_json::{Value, json};

/// A reference destination, and where it runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Memory,
    Sqlite,
    Jsonl,
    Arrow,
    /// The SQLite destination in a process of its own.
    SpawnedSqlite,
    /// The files destination, writing JSON lines, in a process of its own.
    SpawnedJsonl,
    /// The files destination, writing Arrow IPC files, in a process of its own.
    SpawnedArrow,
    /// The SQLite destination, listening on the network, reached over mutual TLS.
    RemoteSqlite,
    /// The files destination, writing JSON lines, reached over mutual TLS.
    RemoteJsonl,
    /// The files destination, writing Arrow IPC files, reached over mutual TLS.
    RemoteArrow,
}

/// Where the file-backed destinations of this test process keep their stores.
static ROOT: LazyLock<tempfile::TempDir> =
    LazyLock::new(|| tempfile::tempdir().expect("a temporary directory is created"));

impl Target {
    /// Every reference destination, in the test's process.
    pub(crate) const IN_PROCESS: [Self; 4] = [Self::Memory, Self::Sqlite, Self::Jsonl, Self::Arrow];

    /// Every reference destination that keeps its stores where the test can read them, spawned
    /// in a process of its own: the memory destination keeps them in its process's memory.
    pub(crate) const SPAWNED: [Self; 3] =
        [Self::SpawnedSqlite, Self::SpawnedJsonl, Self::SpawnedArrow];

    /// The same destinations, listening on the network and reached over mutual TLS.
    pub(crate) const REMOTE: [Self; 3] = [Self::RemoteSqlite, Self::RemoteJsonl, Self::RemoteArrow];

    /// The destination this target places, wherever it runs.
    pub(crate) fn kind(self) -> Self {
        match self {
            Self::SpawnedSqlite | Self::RemoteSqlite => Self::Sqlite,
            Self::SpawnedJsonl | Self::RemoteJsonl => Self::Jsonl,
            Self::SpawnedArrow | Self::RemoteArrow => Self::Arrow,
            kind => kind,
        }
    }

    /// The example that serves this target's destination, when it runs out of process.
    fn served_by(self) -> Option<(&'static str, &'static str)> {
        match self.kind() {
            _ if matches!(
                self,
                Self::Memory | Self::Sqlite | Self::Jsonl | Self::Arrow
            ) =>
            {
                None
            }
            Self::Sqlite => Some(("io.rapidbyte.sqlite", "serve_sqlite")),
            _ => Some(("io.rapidbyte.files", "serve_files")),
        }
    }

    /// `store` for this destination, so sources and stores of one test never mix destinations.
    pub(crate) fn name(self, store: &str) -> String {
        format!("{store}_{self:?}").to_lowercase()
    }

    fn path(self, store: &str) -> PathBuf {
        ROOT.path().join(self.name(store))
    }

    /// The configuration of `store` in this destination.
    fn config(self, store: &str) -> Value {
        let path = self.path(store);
        match self.kind() {
            Self::Sqlite => json!({ "path": path.with_extension("db") }),
            Self::Jsonl => json!({ "root": path, "format": "jsonl" }),
            Self::Arrow => json!({ "root": path, "format": "arrow" }),
            _ => json!({ "store": self.name(store) }),
        }
    }

    /// A connection to `store` in this destination.
    pub(crate) async fn destination(self, store: &str) -> Arc<dyn Destination> {
        let config = self.config(store);
        if let Some((id, example)) = self.served_by()
            && Self::REMOTE.contains(&self)
        {
            return crate::support::listening::listening(id, example, &config).await;
        }
        if let Some((id, example)) = self.served_by() {
            let id = ConnectorId::parse(id).expect("a valid id");
            let reference = ConnectorRef::new(id).path(crate::support::example(example));
            let placed = crate::support::local()
                .destination(&reference, &config)
                .await
                .expect("the destination starts");
            return Arc::from(placed.connector);
        }
        let factory = match self {
            Self::Memory => destination_factory::<MemoryDestination>(),
            Self::Sqlite => destination_factory::<SqliteDestination>(),
            _ => destination_factory::<FilesDestination>(),
        };
        let connected = factory.connect(config, ConnectContext::new()).await;
        Arc::from(connected.expect("the destination connects"))
    }

    /// Every published batch of `table` in `store`.
    pub(crate) fn published(self, store: &str, table: &str) -> Vec<RecordBatch> {
        let path = self.path(store);
        match self.kind() {
            Self::Memory => published(&self.name(store), table),
            Self::Sqlite => sqlite::published(path.with_extension("db"), table)
                .expect("the database reads back"),
            _ => files::published(&path, table).expect("the files read back"),
        }
    }

    /// The sorted ids published to `table` in `store`.
    pub(crate) fn ids(self, store: &str, table: &str) -> Vec<i64> {
        let mut ids: Vec<i64> = self
            .published(store, table)
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("id")
                    .expect("tables have an id column");
                let ids = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("ids read back as Int64");
                (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// The number of rows published to `table` in `store`.
    pub(crate) fn rows(self, store: &str, table: &str) -> usize {
        self.published(store, table)
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    /// Every published row of `table` in `store` as JSON, without the metadata columns, sorted
    /// by their rendering.
    pub(crate) fn json(self, store: &str, table: &str) -> Vec<Value> {
        let mut rows = Vec::new();
        for batch in self.published(store, table) {
            let mut writer = arrow_json::ArrayWriter::new(Vec::new());
            writer
                .write(&batch)
                .expect("published batches render as JSON");
            writer.finish().expect("the JSON array closes");
            let rendered: Vec<Value> =
                serde_json::from_slice(&writer.into_inner()).expect("arrow writes valid JSON");
            for mut row in rendered {
                if let Value::Object(columns) = &mut row {
                    columns.retain(|name, _| !name.starts_with("_rdlt_"));
                }
                rows.push(row);
            }
        }
        rows.sort_by_key(ToString::to_string);
        rows
    }
}
