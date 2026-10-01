//! The file formats: JSON lines and Arrow IPC files, written once and read back.
//!
//! Nothing is written that the reader would refuse: a line or a batch beyond the reader's limits
//! fails the write, and the file is removed.

mod batches;
pub(super) mod ipc;
mod json;
pub(super) mod lines;
pub(super) mod plain;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use rdlt_connector::{ConnectorError, Result, TypeKind};
use rdlt_wire::Limits;
use schemars::JsonSchema;
use serde::Deserialize;

use super::io;
use crate::limits::LINE_BYTES;
use crate::rooted::Dir;

/// How a table's files store its rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// One JSON object per line; types JSON has no form for are stored as text, and a float
    /// that is not finite as the string naming it: `NaN`, `Infinity` or `-Infinity`.
    #[default]
    Jsonl,
    /// Arrow IPC files, which keep every type.
    Arrow,
}

/// What a written file holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Written {
    pub(super) rows: u64,
    pub(super) bytes: u64,
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

    /// The format of the file `name`, by its extension.
    pub(super) fn named(name: &str) -> Option<Self> {
        Self::of(name.rsplit_once('.')?.1)
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

    /// Writes `batches`, of one schema, to the new file `name` in `dir`, durably with its
    /// directory entry; what the file holds.
    pub(super) fn write(self, dir: &Dir, name: &str, batches: &[RecordBatch]) -> Result<Written> {
        let Some(first) = batches.first() else {
            return Err(ConnectorError::internal("a file of no batch has no schema"));
        };
        let mut writer = Writer::create(self, dir, name, first.schema_ref())?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()
    }

    /// The rows of the file `name` in `dir`; JSON lines are read as `schema`, Arrow files as
    /// written.
    pub(super) fn read(
        self,
        dir: &Dir,
        name: &str,
        schema: &SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        let mut reader = Reader::open(self, dir, name, schema)?;
        let mut batches = Vec::new();
        while let Some(batch) = reader.next()? {
            batches.push(batch);
        }
        Ok(batches)
    }
}

/// The batches of one file, read in order.
#[derive(Debug)]
pub(super) struct Reader {
    path: PathBuf,
    rows: Rows,
}

#[derive(Debug)]
enum Rows {
    Jsonl(json::Rows),
    Arrow(ipc::IpcFile),
}

impl Reader {
    /// A reader of the regular file `name` in `dir`, which a manifest or a session's staging
    /// lists.
    pub(super) fn open(
        format: FileFormat,
        dir: &Dir,
        name: &str,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let path = dir.at(name);
        let file = dir.file(name).map_err(io::listed("opening", &path))?;
        Self::over(format, file, path, schema)
    }

    /// A reader of the open regular `file`, which `path` names in messages.
    pub(super) fn over(
        format: FileFormat,
        file: File,
        path: PathBuf,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let rows = match format {
            FileFormat::Jsonl => json::Rows::new(file, schema)
                .map(Rows::Jsonl)
                .map_err(|error| decoded(&path, error)),
            FileFormat::Arrow => ipc::IpcFile::open(file, Limits::default())
                .map(Rows::Arrow)
                .map_err(|error| located(&path, error)),
        }?;
        Ok(Self { path, rows })
    }

    /// The schema an Arrow file's batches are in; none for JSON lines, which hold none.
    pub(super) fn schema(&self) -> Option<&SchemaRef> {
        match &self.rows {
            Rows::Jsonl(_) => None,
            Rows::Arrow(file) => Some(file.schema()),
        }
    }

    /// Skips the next `batches` batches of an Arrow file; how many it skipped, fewer where the
    /// file holds fewer.
    pub(super) fn skip(&mut self, batches: u64) -> u64 {
        match &mut self.rows {
            Rows::Arrow(file) => file.skip(batches),
            Rows::Jsonl(_) => 0,
        }
    }

    /// The next batch, none once the file is read.
    pub(super) fn next(&mut self) -> Result<Option<RecordBatch>> {
        match &mut self.rows {
            Rows::Jsonl(rows) => rows.next().map_err(|error| decoded(&self.path, error)),
            Rows::Arrow(file) => file.next().map_err(|error| located(&self.path, error)),
        }
    }
}

/// `error` reading the file at `path`, naming the file.
fn located(path: &std::path::Path, error: ConnectorError) -> ConnectorError {
    let message = format!("reading {}: {error}", path.display());
    let located = match error.limit() {
        Some(limit) => ConnectorError::exceeds(limit),
        None => ConnectorError::new(error.kind(), message),
    };
    located.with_source(error)
}

/// An Arrow error reading the file at `path`: a line beyond its limit keeps the limit, anything
/// else is a data error.
fn decoded(path: &std::path::Path, error: ArrowError) -> ConnectorError {
    match error {
        ArrowError::IoError(_, error) => io::failed("reading", path)(error),
        error => {
            ConnectorError::data(format!("reading {}: {error}", path.display())).with_source(error)
        }
    }
}

/// A file being written; one dropped unfinished is removed.
pub(super) struct Writer<'a> {
    dir: &'a Dir,
    name: &'a str,
    path: PathBuf,
    rows: u64,
    /// The file's schema, which an Arrow file's batches are all in.
    schema: SchemaRef,
    sink: Option<Sink>,
    finished: bool,
}

enum Sink {
    Jsonl(lines::Bounded<BufWriter<File>>),
    Arrow(Box<arrow_ipc::writer::FileWriter<Counted<BufWriter<File>>>>),
}

impl std::fmt::Debug for Writer<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl<'a> Writer<'a> {
    /// Creates the file `name` in `dir`, to hold batches of `schema`.
    ///
    /// # Errors
    ///
    /// A name that exists, and for an Arrow file a schema beyond the limits a reader accepts.
    pub(super) fn create(
        format: FileFormat,
        dir: &'a Dir,
        name: &'a str,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let path = dir.at(name);
        if format == FileFormat::Arrow {
            ipc::admitted(schema, Limits::default())?;
        }
        let file = dir.create(name).map_err(io::failed("creating", &path))?;
        let mut writer = Self {
            dir,
            name,
            path,
            rows: 0,
            schema: SchemaRef::clone(schema),
            sink: None,
            finished: false,
        };
        let file = BufWriter::new(file);
        writer.sink = Some(match format {
            FileFormat::Jsonl => Sink::Jsonl(lines::Bounded::new(file, LINE_BYTES)),
            FileFormat::Arrow => {
                let counted = Counted {
                    inner: file,
                    written: 0,
                };
                let started = arrow_ipc::writer::FileWriter::try_new(counted, schema);
                Sink::Arrow(Box::new(started.map_err(|error| writer.encoded(error))?))
            }
        });
        Ok(writer)
    }

    /// Writes `batch`; an Arrow file holds it as batches of about the size a batch aims for.
    pub(super) fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let rows = batch.num_rows();
        let written = match self.sink.as_mut().expect("the writer is unfinished") {
            Sink::Jsonl(lines) => {
                let mut writer = arrow_json::WriterBuilder::new()
                    .with_encoder_factory(Arc::new(json::ExactFloats))
                    .build::<_, arrow_json::writer::LineDelimited>(lines);
                writer.write(batch).and_then(|()| writer.finish())
            }
            Sink::Arrow(_) => return self.write_arrow(batch),
        };
        written.map_err(|error| self.encoded(error))?;
        self.count(rows);
        Ok(())
    }

    /// Writes `batch` to an Arrow file as batches a reader accepts, each checked against every
    /// limit a reader holds a batch to.
    fn write_arrow(&mut self, batch: &RecordBatch) -> Result<()> {
        let limits = Limits::default();
        for chunk in batches::chunks(batch, &limits) {
            batches::admitted(&chunk, &limits)?;
            let Some(Sink::Arrow(writer)) = self.sink.as_mut() else {
                unreachable!("the writer writes an Arrow file");
            };
            let before = writer.get_ref().written;
            let written = writer.write(&chunk);
            let bytes = writer.get_ref().written - before;
            written.map_err(|error| self.encoded(error))?;
            batches::framed(bytes, &limits)?;
        }
        self.count(batch.num_rows());
        Ok(())
    }

    /// Writes the rows of `file`, a regular file of this writer's format which `path` names in
    /// messages: an Arrow file's batches, their dictionaries as the values they stand for, which
    /// must then be in this file's schema, or a JSON lines file's lines as they are.
    pub(super) fn append(&mut self, file: File, path: PathBuf) -> Result<()> {
        let lines = match self.sink.as_mut().expect("the writer is unfinished") {
            Sink::Jsonl(lines) => lines,
            Sink::Arrow(_) => {
                let mut reader = Reader::over(FileFormat::Arrow, file, path, &self.schema)?;
                while let Some(batch) = reader.next()? {
                    let batch = plain::unkeyed(&batch, &self.schema)
                        .map_err(|error| self.encoded(error))?;
                    self.write(&batch)?;
                }
                return Ok(());
            }
        };
        let mut from = lines::Lines::new(std::io::BufReader::new(file), LINE_BYTES);
        let (mut line, mut rows) = (Vec::new(), 0);
        while from.next(&mut line).map_err(io::failed("reading", &path))? {
            if !lines::holds_a_record(&line) {
                continue;
            }
            // A last line without its ending gets one, so the next line starts its own.
            let ending: &[u8] = if line.ends_with(b"\n") { b"" } else { b"\n" };
            let copied = lines
                .write_all(&line)
                .and_then(|()| lines.write_all(ending));
            copied.map_err(io::failed("writing", &self.path))?;
            rows += 1;
        }
        self.count(rows);
        Ok(())
    }

    fn count(&mut self, rows: usize) {
        self.rows = self
            .rows
            .saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
    }

    /// Finishes the file and makes it and its directory entry durable; what the file holds.
    pub(super) fn finish(mut self) -> Result<Written> {
        let sink = self.sink.take().expect("the writer is unfinished");
        let buffered = match sink {
            Sink::Jsonl(lines) => Ok(lines.into_inner()),
            Sink::Arrow(mut writer) => writer
                .finish()
                .and_then(|()| writer.into_inner())
                .map(|counted| counted.inner),
        };
        let buffered = buffered.map_err(|error| self.encoded(error))?;
        let failed = io::failed("writing", &self.path);
        let file = buffered
            .into_inner()
            .map_err(|error| failed(error.into_error()))?;
        crate::rooted::sync_file(&file, &self.path).map_err(&failed)?;
        let bytes = file.metadata().map_err(&failed)?.len();
        self.dir
            .sync()
            .map_err(io::failed("syncing", self.dir.path()))?;
        let rows = self.rows;
        self.finished = true;
        Ok(Written { rows, bytes })
    }

    /// An Arrow error writing the file: a limit a line or a batch went beyond keeps the limit,
    /// a filesystem failure is classified as one, anything else is a data error.
    fn encoded(&self, error: ArrowError) -> ConnectorError {
        match error {
            ArrowError::IoError(_, error) => io::failed("writing", &self.path)(error),
            error => ConnectorError::data(format!("writing {}: {error}", self.path.display()))
                .with_source(error),
        }
    }
}

impl Drop for Writer<'_> {
    fn drop(&mut self) {
        if !self.finished {
            drop(self.dir.remove_file(self.name));
        }
    }
}

/// A writer that counts the bytes written through it.
struct Counted<W> {
    inner: W,
    written: u64,
}

impl<W: Write> Write for Counted<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.written = self
            .written
            .saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
