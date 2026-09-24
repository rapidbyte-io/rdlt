//! The file formats: JSON lines and Arrow IPC files, written once and read back.

use std::collections::BTreeSet;
use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::{ConnectorError, Result, TypeKind};
use schemars::JsonSchema;
use serde::Deserialize;

use super::io;

/// How a table's files store its rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// One JSON object per line; types JSON has no form for are stored as text.
    #[default]
    Jsonl,
    /// Arrow IPC files, which keep every type.
    Arrow,
}

impl FileFormat {
    /// The files' extension.
    pub(super) fn extension(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Arrow => "arrow",
        }
    }

    /// The format of a file with `extension`, if it is one.
    pub(super) fn of(extension: &str) -> Option<Self> {
        match extension {
            "jsonl" | "ndjson" => Some(Self::Jsonl),
            "arrow" => Some(Self::Arrow),
            _ => None,
        }
    }

    /// The logical types the format keeps.
    pub(super) fn types(self) -> BTreeSet<TypeKind> {
        use TypeKind as K;
        let mut types = BTreeSet::from([
            K::Bool,
            K::Int8,
            K::Int16,
            K::Int32,
            K::Int64,
            K::Float32,
            K::Float64,
            K::Utf8,
            K::Struct,
            K::List,
        ]);
        if self == Self::Arrow {
            types.extend([
                K::Decimal,
                K::Binary,
                K::Date,
                K::Time,
                K::Timestamp,
                K::Duration,
                K::Uuid,
                K::Json,
            ]);
        }
        types
    }

    /// Writes `batch` to a new file at `path`, durably; returns the file's size in bytes.
    pub(super) fn write(self, path: &Path, batch: &RecordBatch) -> Result<u64> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io::failed("creating a directory", parent))?;
        }
        let file = fs::File::create_new(path).map_err(io::failed("creating", path))?;
        let encoded = |error: arrow_schema::ArrowError| {
            ConnectorError::data(format!("writing {}: {error}", path.display()))
        };
        let file = match self {
            Self::Jsonl => {
                let mut writer = arrow_json::LineDelimitedWriter::new(BufWriter::new(file));
                writer.write(batch).map_err(encoded)?;
                writer.finish().map_err(encoded)?;
                writer.into_inner().into_inner()
            }
            Self::Arrow => {
                let mut writer = arrow_ipc::writer::FileWriter::try_new(
                    BufWriter::new(file),
                    batch.schema_ref(),
                )
                .map_err(encoded)?;
                writer.write(batch).map_err(encoded)?;
                writer.finish().map_err(encoded)?;
                writer.into_inner().map_err(encoded)?.into_inner()
            }
        };
        let mut file = file.map_err(|error| io::failed("writing", path)(error.into_error()))?;
        file.flush().map_err(io::failed("writing", path))?;
        file.sync_all().map_err(io::failed("syncing", path))?;
        file.metadata()
            .map(|metadata| metadata.len())
            .map_err(io::failed("reading the size of", path))
    }

    /// The rows of the file at `path`; JSON lines are read as `schema`, Arrow files as written.
    pub(super) fn read(self, path: &Path, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
        let file = fs::File::open(path).map_err(io::failed("opening", path))?;
        let decoded = |error: arrow_schema::ArrowError| {
            ConnectorError::data(format!("reading {}: {error}", path.display()))
        };
        match self {
            Self::Jsonl => arrow_json::ReaderBuilder::new(SchemaRef::clone(schema))
                .build(BufReader::new(file))
                .map_err(decoded)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(decoded),
            Self::Arrow => arrow_ipc::reader::FileReader::try_new(BufReader::new(file), None)
                .map_err(decoded)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(decoded),
        }
    }
}
