//! A source that reads JSON lines and Arrow IPC files under a root directory.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::limits::MAX_JSON_PUSH_BYTES;
use rdlt_connector::prelude::*;
use rdlt_connector::{Partitioning, StreamName};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::format::lines::{self, Lines};
use super::format::{FileFormat, Reader};
use super::io;
use crate::blocking::blocking;
use crate::limits::{FILE_BYTES, LINE_BYTES};
use crate::rooted::{Dir, Kind, Limit, Limited};

/// Configuration of [`FilesSource`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FilesSourceConfig {
    /// The directory holding the streams.
    pub root: PathBuf,
    /// Records per push of a JSON lines file; a checkpoint follows each push.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: NonZeroUsize,
    /// Bytes: the longest line of a JSON lines file the source reads, its line ending apart; a
    /// longer line fails the read.
    ///
    /// 32 MiB where unset, and at most two bytes less than a JSON push may hold.
    #[serde(default = "default_line_bytes")]
    pub max_line_bytes: NonZeroU64,
    /// Bytes: the largest file the source reads, 16 GiB where unset; a larger file fails the
    /// read.
    #[serde(default = "default_file_bytes")]
    pub max_file_bytes: NonZeroU64,
}

fn default_batch_rows() -> NonZeroUsize {
    NonZeroUsize::new(1024).expect("1024 is non-zero")
}

fn default_line_bytes() -> NonZeroU64 {
    NonZeroU64::new(LINE_BYTES).expect("the limit is not zero")
}

fn default_file_bytes() -> NonZeroU64 {
    NonZeroU64::new(FILE_BYTES).expect("the limit is not zero")
}

/// Bytes: what a line's ending adds to it at most, a carriage return and a line feed.
const LINE_ENDING: u64 = 2;

/// Reads the files under a root: `<stream>.jsonl` or `<stream>.arrow` is a stream of one
/// partition, and a directory `<stream>/` is a stream whose files are its partitions.
///
/// Names starting with `.` or `_`, names that are no stream or partition name, and whatever is
/// no regular file or directory (a link, a pipe, a device) are skipped. A JSON lines file's
/// records are pushed as JSON, as they are written, for the engine to type; an Arrow file's
/// batches are pushed as written. Every push is followed by a checkpoint, so a read resumes
/// after the last committed push.
///
/// The root and every stream's directory must belong to the user the source runs as and be
/// writable by no other: a directory another user owns is refused even where this user may
/// only read it, and two entries that name one stream are refused by both names.
#[derive(Debug)]
pub struct FilesSource {
    root: Arc<Dir>,
    streams: Vec<FileStream>,
    limits: ReadLimits,
}

/// The limits a read keeps.
#[derive(Clone, Copy, Debug)]
struct ReadLimits {
    batch_rows: usize,
    line_bytes: u64,
    file_bytes: u64,
}

/// One stream: its name, the directory its files are in, and its files by partition.
#[derive(Clone, Debug)]
struct FileStream {
    name: StreamName,
    /// The stream's directory under the root, for a stream that is one.
    dir: Option<String>,
    files: Arc<Vec<(PartitionId, String, FileFormat)>>,
}

#[source(id = "io.rapidbyte.files")]
impl SourceConnector for FilesSource {
    type Config = FilesSourceConfig;

    async fn connect(config: FilesSourceConfig, _context: &ConnectContext) -> Result<Self> {
        let limits = ReadLimits {
            batch_rows: config.batch_rows.get(),
            line_bytes: config.max_line_bytes.get(),
            file_bytes: config.max_file_bytes.get(),
        };
        if limits.line_bytes > MAX_JSON_PUSH_BYTES - LINE_ENDING {
            return Err(ConnectorError::config(format!(
                "max_line_bytes is {}; one JSON push holds lines of at most {} bytes",
                limits.line_bytes,
                MAX_JSON_PUSH_BYTES - LINE_ENDING
            )));
        }
        let (root, streams) = blocking(move || {
            let root = Dir::ambient(&config.root).map_err(unusable("opening", &config.root))?;
            let streams = discover(&root)?;
            Ok((root, streams))
        })
        .await?;
        Ok(Self {
            root: Arc::new(root),
            streams,
            limits,
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
fn discover(root: &Dir) -> Result<Vec<FileStream>> {
    let mut streams = Vec::new();
    // The entry each stream was found in: two entries are never read as one stream.
    let mut found = BTreeMap::<String, String>::new();
    for (entry, kind) in entries(root)? {
        let named = entry.clone();
        let stream = match kind {
            Kind::Dir => entry.clone(),
            Kind::File => match split(&entry) {
                Some((stem, _)) => stem.to_owned(),
                None => continue,
            },
            // A link, a pipe, a device or a socket is no stream: none is opened.
            Kind::Other => continue,
        };
        // A name that is no stream's is skipped, as a hidden one is, before it is opened: a
        // directory the source would refuse to enter fails nothing unless it is a stream's.
        let Ok(name) = StreamName::new(&stream) else {
            continue;
        };
        let (dir, files): (_, Vec<_>) = if kind == Kind::Dir {
            let listed = root
                .dir(&entry)
                .map_err(io::failed("listing", &root.at(&entry)))?;
            let files = entries(&listed)?
                .into_iter()
                .filter(|(_, kind)| *kind == Kind::File)
                .filter_map(|(file, _)| partition(file))
                .collect();
            (Some(entry), files)
        } else {
            (None, partition(entry).into_iter().collect())
        };
        if !files.is_empty() {
            if let Some(first) = found.insert(stream.clone(), named.clone()) {
                return Err(duplicate(&stream, &first, &named));
            }
            streams.push(FileStream {
                name,
                dir,
                files: Arc::new(files),
            });
        }
    }
    Ok(streams)
}

/// The code of two entries of a root that name the same stream.
const DUPLICATE_STREAM: &str = "duplicate_stream";

/// The error of the entries `first` and `second` both naming the stream `stream`.
fn duplicate(stream: &str, first: &str, second: &str) -> ConnectorError {
    ConnectorError::config(format!(
        "the entries {first:?} and {second:?} both name the stream {stream:?}: keep one of them"
    ))
    .with_code(DUPLICATE_STREAM)
}

/// The file `name` as a partition, if its extension is a format's and its name a partition's.
fn partition(name: String) -> Option<(PartitionId, String, FileFormat)> {
    let (_, format) = split(&name)?;
    let id = PartitionId::parse(&name).ok()?;
    Some((id, name, format))
}

/// Classifies a filesystem error from `what` on `path`, a directory the configuration names or
/// one beneath it, as a configuration error: one the connector refuses keeps its code.
fn unusable<'a>(what: &'a str, path: &'a Path) -> impl Fn(std::io::Error) -> ConnectorError + 'a {
    move |error| {
        let error = io::failed(what, path)(error);
        if error.kind() == ConnectorErrorKind::Config {
            error
        } else {
            ConnectorError::config(error.to_string()).with_source(error)
        }
    }
}

/// The entries of `dir` that are not hidden and whose names are text, in name order.
fn entries(dir: &Dir) -> Result<Vec<(String, Kind)>> {
    let listed = dir.entries().map_err(unusable("listing", dir.path()))?;
    Ok(listed
        .into_iter()
        .filter_map(|(name, kind)| Some((name.into_string().ok()?, kind)))
        .filter(|(name, _)| !name.starts_with(['.', '_']))
        .collect())
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
        let Some((_, file, format)) = self.files.iter().find(|(id, ..)| id == partition.id())
        else {
            return Err(ConnectorError::data(format!(
                "stream {} has no file {}",
                self.name,
                partition.id()
            )));
        };
        let (root, dir) = (Arc::clone(&source.root), self.dir.clone());
        let (file, format, limits) = (file.clone(), *format, source.limits);
        let mut pushes = blocking(move || {
            Pushes::open(&root, dir.as_deref(), &file, format, cursor.read, limits)
        })
        .await?;
        let mut read = cursor.read;
        loop {
            let (next, push) = blocking(move || {
                let mut pushes = pushes;
                let push = pushes.next()?;
                Ok((pushes, push))
            })
            .await?;
            pushes = next;
            match push {
                None => return Ok(()),
                Some(Pushed::Json(json, records)) => {
                    read += records;
                    out.json(json).await?;
                }
                Some(Pushed::Arrow(batch)) => {
                    read += 1;
                    out.batch(batch).await?;
                }
            }
            out.checkpoint(&Position { read }).await?;
        }
    }
}

/// The code of an error for a cursor that stands beyond what its file holds.
const CURSOR_BEYOND_FILE: &str = "cursor_beyond_file";

/// A cursor that read `read` records or batches of the file at `path`, which holds fewer: the
/// file was cut or replaced since, and the source cannot vouch for what a read from there would
/// skip.
fn beyond(path: &Path, read: u64) -> ConnectorError {
    ConnectorError::data(format!(
        "reading {}: the cursor stands after {read} records or batches, more than the file holds",
        path.display()
    ))
    .with_code(CURSOR_BEYOND_FILE)
}

/// One push of a file.
enum Pushed {
    /// Lines of JSON, and how many records they hold.
    Json(Bytes, u64),
    Arrow(arrow_array::RecordBatch),
}

/// The pushes of one file, from a position on.
enum Pushes {
    Jsonl(JsonLines),
    Arrow(Box<Reader>),
}

impl Pushes {
    /// The pushes of the regular file `file`, in `dir` under `root` or in `root` itself, after
    /// its first `read` records or batches.
    fn open(
        root: &Dir,
        dir: Option<&str>,
        file: &str,
        format: FileFormat,
        read: u64,
        limits: ReadLimits,
    ) -> Result<Self> {
        let listed;
        let dir = match dir {
            Some(name) => {
                let opened = root.dir(name);
                listed = opened.map_err(io::failed("opening", &root.at(name)))?;
                &listed
            }
            None => root,
        };
        let path = dir.at(file);
        let opened = dir.file(file).map_err(io::failed("opening", &path))?;
        let limit = Limit {
            name: "file bytes",
            bytes: limits.file_bytes,
        };
        let measured = opened
            .metadata()
            .and_then(|size| Ok(limit.admit(size.len())?));
        measured.map_err(io::failed("reading", &path))?;
        match format {
            FileFormat::Jsonl => {
                // The file is measured again as it is read: one that grows fails its read.
                let opened = Limited::new(opened, limit);
                JsonLines::open(opened, path, read, limits).map(Self::Jsonl)
            }
            FileFormat::Arrow => {
                let empty = Arc::new(arrow_schema::Schema::empty());
                let mut reader = Reader::over(format, opened, path.clone(), &empty)?;
                if reader.skip(read) < read {
                    return Err(beyond(&path, read));
                }
                Ok(Self::Arrow(Box::new(reader)))
            }
        }
    }

    fn next(&mut self) -> Result<Option<Pushed>> {
        match self {
            Self::Jsonl(lines) => lines.next(),
            Self::Arrow(reader) => Ok(reader.next()?.map(Pushed::Arrow)),
        }
    }
}

/// The records of a JSON lines file, as pushes of bounded size.
struct JsonLines {
    lines: Lines<BufReader<Limited<File>>>,
    path: PathBuf,
    /// The line last read, which the next push starts with where `carried`.
    line: Vec<u8>,
    carried: bool,
    batch_rows: usize,
}

impl JsonLines {
    /// The records of `file` after its first `read`; blank lines hold none.
    fn open(file: Limited<File>, path: PathBuf, read: u64, limits: ReadLimits) -> Result<Self> {
        let mut opened = Self {
            lines: Lines::new(BufReader::new(file), limits.line_bytes),
            path,
            line: Vec::new(),
            carried: false,
            batch_rows: limits.batch_rows,
        };
        let mut skipped = 0;
        while skipped < read && opened.record()? {
            skipped += 1;
        }
        if skipped < read {
            return Err(beyond(&opened.path, read));
        }
        Ok(opened)
    }

    /// Reads the next line that holds a record into `line`; `false` at the end.
    fn record(&mut self) -> Result<bool> {
        loop {
            let read = self.lines.next(&mut self.line);
            if !read.map_err(io::failed("reading", &self.path))? {
                return Ok(false);
            }
            if lines::holds_a_record(&self.line) {
                return Ok(true);
            }
        }
    }

    /// The next records, as they are written: at most the rows of one batch, and the bytes one
    /// JSON push may hold.
    fn next(&mut self) -> Result<Option<Pushed>> {
        let most = usize::try_from(MAX_JSON_PUSH_BYTES).unwrap_or(usize::MAX);
        let (mut push, mut records) = (Vec::new(), 0_u64);
        while usize::try_from(records).unwrap_or(usize::MAX) < self.batch_rows {
            if !std::mem::take(&mut self.carried) && !self.record()? {
                break;
            }
            let ended = self.line.ends_with(b"\n");
            if push.len() + self.line.len() + usize::from(!ended) > most {
                // The line starts the next push.
                self.carried = true;
                break;
            }
            push.extend_from_slice(&self.line);
            if !ended {
                push.push(b'\n');
            }
            records += 1;
        }
        Ok((records > 0).then(|| Pushed::Json(Bytes::from(push), records)))
    }
}
