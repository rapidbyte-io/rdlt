//! The reference destinations the destination-facing tests run against, each keeping a test's
//! stores apart.

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, Int64Array, RecordBatch};
use rdlt_connector::{ConnectContext, Destination, destination_factory};
use rdlt_connector_reference::{
    FilesDestination, MemoryDestination, SqliteDestination, files, published, sqlite,
};
use serde_json::{Value, json};

/// A reference destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Memory,
    Sqlite,
    Jsonl,
    Arrow,
}

/// Where the file-backed destinations of this test process keep their stores.
static ROOT: LazyLock<tempfile::TempDir> =
    LazyLock::new(|| tempfile::tempdir().expect("a temporary directory is created"));

impl Target {
    /// Every reference destination.
    pub(crate) const ALL: [Self; 4] = [Self::Memory, Self::Sqlite, Self::Jsonl, Self::Arrow];

    /// `store` for this destination, so sources and stores of one test never mix destinations.
    pub(crate) fn name(self, store: &str) -> String {
        format!("{store}_{self:?}").to_lowercase()
    }

    fn path(self, store: &str) -> PathBuf {
        ROOT.path().join(self.name(store))
    }

    /// A connection to `store` in this destination.
    pub(crate) async fn destination(self, store: &str) -> Arc<dyn Destination> {
        let path = self.path(store);
        let connected = match self {
            Self::Memory => {
                destination_factory::<MemoryDestination>()
                    .connect(json!({ "store": self.name(store) }), ConnectContext::new())
                    .await
            }
            Self::Sqlite => {
                destination_factory::<SqliteDestination>()
                    .connect(
                        json!({ "path": path.with_extension("db") }),
                        ConnectContext::new(),
                    )
                    .await
            }
            Self::Jsonl | Self::Arrow => {
                let format = if self == Self::Jsonl {
                    "jsonl"
                } else {
                    "arrow"
                };
                destination_factory::<FilesDestination>()
                    .connect(
                        json!({ "root": path, "format": format }),
                        ConnectContext::new(),
                    )
                    .await
            }
        };
        Arc::from(connected.expect("the destination connects"))
    }

    /// Every published batch of `table` in `store`.
    pub(crate) fn published(self, store: &str, table: &str) -> Vec<RecordBatch> {
        let path = self.path(store);
        match self {
            Self::Memory => published(&self.name(store), table),
            Self::Sqlite => sqlite::published(path.with_extension("db"), table)
                .expect("the database reads back"),
            Self::Jsonl | Self::Arrow => {
                files::published(&path, table).expect("the files read back")
            }
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
