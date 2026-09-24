//! A source that reads JSON lines and Arrow IPC files under a root directory.

use std::fs;
use std::io::{BufRead, BufReader, Seek};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use rdlt_connector::prelude::*;
use rdlt_connector::{Partitioning, StreamName};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::format::FileFormat;
use super::io;
use crate::blocking::blocking;

/// Configuration of [`FilesSource`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FilesSourceConfig {
    /// The directory holding the streams.
    pub root: PathBuf,
    /// Rows per pushed batch of a JSON lines file; a checkpoint follows each batch.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: NonZeroUsize,
}

fn default_batch_rows() -> NonZeroUsize {
    NonZeroUsize::new(1024).expect("1024 is non-zero")
}

/// Reads the files under a root: `<stream>.jsonl` or `<stream>.arrow` is a stream of one
/// partition, and a directory `<stream>/` is a stream whose files are its partitions.
///
/// Names starting with `.` or `_` are skipped. A JSON lines file's schema is inferred from the
/// whole file; an Arrow file's batches are pushed as written. Every batch is followed by a
/// checkpoint, so a read resumes after the last committed batch.
#[derive(Debug)]
pub struct FilesSource {
    streams: Vec<FileStream>,
    batch_rows: NonZeroUsize,
}

/// One stream: its name and its files, by partition.
#[derive(Clone, Debug)]
struct FileStream {
    name: StreamName,
    files: Arc<Vec<(PartitionId, PathBuf, FileFormat)>>,
}

#[source(id = "io.rapidbyte.files")]
impl SourceConnector for FilesSource {
    type Config = FilesSourceConfig;

    async fn connect(config: FilesSourceConfig, _context: &ConnectContext) -> Result<Self> {
        let root = config.root;
        let streams = blocking(move || discover(&root)).await?;
        Ok(Self {
            streams,
            batch_rows: config.batch_rows,
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        self.streams.iter().fold(Streams::new(), |streams, stream| {
            streams.with(stream.clone())
        })
    }
}

/// The streams under `root`, in name order.
fn discover(root: &Path) -> Result<Vec<FileStream>> {
    let mut streams = Vec::new();
    for (name, path) in entries(root)? {
        let (stream, files) = if path.is_dir() {
            let files: Vec<_> = entries(&path)?
                .into_iter()
                .filter_map(|(file, path)| {
                    let (_, format) = split(&file)?;
                    Some((file, path, format))
                })
                .collect();
            (name, files)
        } else {
            let Some((stem, format)) = split(&name) else {
                continue;
            };
            (stem.to_owned(), vec![(name, path, format)])
        };
        if files.is_empty() {
            continue;
        }
        let name = StreamName::new(&stream).config(format!("stream name {stream:?}"))?;
        let files = files
            .into_iter()
            .map(|(file, path, format)| {
                let partition = PartitionId::parse(&file).config(format!("file name {file:?}"))?;
                Ok((partition, path, format))
            })
            .collect::<Result<Vec<_>>>()?;
        streams.push(FileStream {
            name,
            files: Arc::new(files),
        });
    }
    Ok(streams)
}

/// The entries of `dir` that are not hidden, by name, in name order.
fn entries(dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let listed = fs::read_dir(dir).map_err(|error| {
        let error = io::failed("listing", dir)(error);
        ConnectorError::config(error.to_string())
    })?;
    let mut entries = Vec::new();
    for entry in listed {
        let entry = entry.map_err(io::failed("listing", dir))?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if !name.starts_with(['.', '_']) {
            entries.push((name, entry.path()));
        }
    }
    entries.sort();
    Ok(entries)
}

/// The stem and format of the file `name`, if its extension is a format's.
fn split(name: &str) -> Option<(&str, FileFormat)> {
    let (stem, extension) = name.rsplit_once('.')?;
    Some((stem, FileFormat::of(extension)?))
}

/// How much of a file was read: records for JSON lines, batches for Arrow files.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Position {
    read: u64,
}

impl ReadStream<FilesSource> for FileStream {
    type Cursor = Position;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(self.name.clone())
            .with_partitioning(Partitioning::Planned)
            .with_checkpointing(Checkpointing::OnDemand)
    }

    async fn partitions(
        &self,
        _source: &FilesSource,
        _state: &StreamState,
    ) -> Result<Vec<Partition>> {
        Ok(self
            .files
            .iter()
            .map(|(partition, ..)| Partition::new(partition.clone()))
            .collect())
    }

    async fn read(
        &self,
        source: &FilesSource,
        partition: &Partition,
        cursor: Position,
        out: &mut Emitter<Position>,
    ) -> Result<()> {
        let Some((_, path, format)) = self.files.iter().find(|(id, ..)| id == partition.id())
        else {
            return Err(ConnectorError::data(format!(
                "stream {} has no file {}",
                self.name,
                partition.id()
            )));
        };
        let (path, format, batch_rows) = (path.clone(), *format, source.batch_rows.get());
        let mut reader =
            blocking(move || Batches::open(&path, format, cursor.read, batch_rows)).await?;
        let mut read = cursor.read;
        loop {
            let (next, batch) = blocking(move || {
                let mut reader = reader;
                let batch = reader.next()?;
                Ok((reader, batch))
            })
            .await?;
            reader = next;
            let Some(batch) = batch else {
                return Ok(());
            };
            read += match format {
                FileFormat::Jsonl => batch.num_rows() as u64,
                FileFormat::Arrow => 1,
            };
            out.batch(batch).await?;
            out.checkpoint(&Position { read }).await?;
        }
    }
}

/// The batches of one file, from a position on.
enum Batches {
    Jsonl(arrow_json::Reader<BufReader<fs::File>>),
    Arrow(arrow_ipc::reader::FileReader<BufReader<fs::File>>),
    /// A file read to its end.
    Read,
}

impl std::fmt::Debug for Batches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Batches")
    }
}

impl Batches {
    /// The batches of the file at `path` after the first `read` records or batches.
    fn open(path: &Path, format: FileFormat, read: u64, batch_rows: usize) -> Result<Self> {
        let failed = |error: ArrowError| {
            ConnectorError::data(format!("reading {}: {error}", path.display()))
        };
        let file = fs::File::open(path).map_err(io::failed("opening", path))?;
        let mut file = BufReader::new(file);
        match format {
            FileFormat::Jsonl => {
                let (schema, _) =
                    arrow_json::reader::infer_json_schema_from_seekable(&mut file, None)
                        .map_err(failed)?;
                file.rewind().map_err(io::failed("reading", path))?;
                skip_records(&mut file, read).map_err(io::failed("reading", path))?;
                let reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
                    .with_batch_size(batch_rows)
                    .build(file)
                    .map_err(failed)?;
                Ok(Self::Jsonl(reader))
            }
            FileFormat::Arrow => {
                let mut reader =
                    arrow_ipc::reader::FileReader::try_new(file, None).map_err(failed)?;
                let index = usize::try_from(read).unwrap_or(usize::MAX);
                if index >= reader.num_batches() {
                    return Ok(Self::Read);
                }
                reader.set_index(index).map_err(failed)?;
                Ok(Self::Arrow(reader))
            }
        }
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let batch = match self {
            Self::Jsonl(reader) => reader.next(),
            Self::Arrow(reader) => reader.next(),
            Self::Read => None,
        };
        batch
            .transpose()
            .map_err(|error| ConnectorError::data(format!("reading a file: {error}")))
    }
}

/// Moves `file` past its first `records` lines that hold a record; blank lines hold none.
fn skip_records(file: &mut impl BufRead, records: u64) -> std::io::Result<()> {
    let mut skipped = 0;
    let mut line = String::new();
    while skipped < records {
        line.clear();
        if file.read_line(&mut line)? == 0 {
            break;
        }
        if !line.trim().is_empty() {
            skipped += 1;
        }
    }
    Ok(())
}
