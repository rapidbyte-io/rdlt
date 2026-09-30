//! Where write-ahead logs are kept: one log per load of a pipeline, in numbered chunks.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

/// One chunk of a load's log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Chunk {
    /// The load whose log it is.
    pub load: LoadId,
    /// Its place in the log, from 0.
    pub number: u64,
}

/// A held claim on a load's log; dropping it lets the log go.
pub type Claim = Box<dyn std::any::Any + Send + Sync>;

/// Keeps each load's write-ahead log as numbered chunks of frames.
///
/// A log is written by one load at a time: appends to one chunk, then that chunk made durable,
/// then the next chunk. Reads are by range, so replaying a log never holds more than a frame.
///
/// A log has one claimant at a time: the load writing it, then whoever replays it once that load
/// is gone. A claim outlives nothing that holds it, so a load whose process died leaves its log
/// free for the next to replay.
pub trait WalStore: std::fmt::Debug + Send + Sync + 'static {
    /// A claim on `load`'s log of `pipeline`, or none where another holds it.
    fn claim<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Option<Claim>>>;

    /// Removes `load`'s log of `pipeline` whole: its chunks and what marks its claim.
    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>>;

    /// The loads of `pipeline` that have a log, or a claim's mark left behind.
    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>>;

    /// The chunks of `load`'s log, in order, each with its length.
    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>>;

    /// `len` bytes of chunk `chunk` of `load`'s log, from `offset`.
    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>>;

    /// Appends `bytes` to chunk `chunk` of `load`'s log, creating it where it is missing.
    fn append<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        bytes: Bytes,
    ) -> BoxFuture<'a, io::Result<()>>;

    /// Makes everything appended to chunk `chunk` of `load`'s log durable; a chunk once synced is
    /// finished, and appended to again only after a failure left it unknown.
    fn sync<'a>(&'a self, pipeline: &'a PipelineId, chunk: Chunk) -> BoxFuture<'a, io::Result<()>>;

    /// Removes chunk `chunk` of `load`'s log; the log goes with its last chunk.
    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>>;
}

/// Write-ahead logs in a local directory: each pipeline's in a directory of its own, each load's
/// in a directory of that, each chunk a file.
///
/// On Unix, the directories are created readable by their owner only, and a pipeline's directory
/// owned by another user is refused.
#[derive(Debug)]
pub struct LocalWal {
    base: PathBuf,
    /// The chunk files open for appending.
    open: Arc<Mutex<BTreeMap<PathBuf, Arc<Mutex<std::fs::File>>>>>,
}

impl LocalWal {
    /// Logs under `base`, as `.rdlt` is by default.
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self {
            base: base.into(),
            open: Arc::default(),
        }
    }

    /// The directory of `pipeline`'s logs: its name made safe for a path, then a hash of the
    /// whole, so distinct pipelines never share one (spec §17).
    pub fn pipeline_dir(&self, pipeline: &PipelineId) -> PathBuf {
        let name = pipeline.as_str();
        let safe: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let hash = xxhash_rust::xxh3::xxh3_64(name.as_bytes()) >> 32;
        self.base.join(format!("{safe}-{hash:08x}"))
    }

    fn lock_path(&self, pipeline: &PipelineId, load: LoadId) -> PathBuf {
        self.pipeline_dir(pipeline).join(format!("{load}.lock"))
    }

    fn chunk_path(&self, pipeline: &PipelineId, chunk: Chunk) -> PathBuf {
        self.pipeline_dir(pipeline)
            .join(chunk.load.to_string())
            .join(format!("{:08}.wal", chunk.number))
    }
}

/// Runs `work` on tokio's blocking pool.
fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> BoxFuture<'static, io::Result<T>> {
    Box::pin(async move {
        #[expect(
            clippy::disallowed_methods,
            reason = "LocalWal is the production store; its file I/O waits off the runtime"
        )]
        let task = tokio::task::spawn_blocking(work);
        task.await.map_err(|error| {
            io::Error::other(format!("the write-ahead log's task failed: {error}"))
        })?
    })
}

/// Creates `dir` and its parents where missing, readable by their owner only on Unix, and refuses
/// one another user owns; the directories it created, deepest first.
fn private_dir(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    // The directories missing now, whose names their parents make durable once created: a
    // commit's frame synced in a directory a power loss forgets is lost with it.
    let missing: Vec<PathBuf> = dir
        .ancestors()
        .take_while(|ancestor| !ancestor.exists())
        .map(Path::to_path_buf)
        .collect();
    builder.create(dir)?;
    for created in missing.iter().rev() {
        if let Some(parent) = created.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let owner = std::fs::metadata(dir)?.uid();
        if owner != nix::unistd::geteuid().as_raw() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{} belongs to another user", dir.display()),
            ));
        }
    }
    Ok(missing)
}

impl WalStore for LocalWal {
    /// Locks the file `<load>.lock` beside the load's directory, which the lock outlives; the
    /// lock goes with the process, as `flock` locks do, and is advisory, so the logs' directory is
    /// a local one.
    fn claim<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Option<Claim>>> {
        let path = self.lock_path(pipeline, load);
        blocking(move || {
            private_dir(path.parent().unwrap_or(Path::new(".")))?;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)?;
            match file.try_lock() {
                Ok(()) => Ok(Some(Box::new(file) as Claim)),
                Err(std::fs::TryLockError::WouldBlock) => Ok(None),
                Err(std::fs::TryLockError::Error(error)) => Err(error),
            }
        })
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let (dir, lock) = (
            self.pipeline_dir(pipeline).join(load.to_string()),
            self.lock_path(pipeline, load),
        );
        self.open.lock().retain(|path, _| !path.starts_with(&dir));
        blocking(move || {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            // The claim's mark goes last, so a log without it has no chunks.
            match std::fs::remove_file(&lock) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        })
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let dir = self.pipeline_dir(pipeline);
        blocking(move || {
            let mut loads = Vec::new();
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(loads),
                Err(error) => return Err(error),
            };
            for entry in entries {
                let name = entry?.file_name();
                // A load's directory, or the mark of its claim alone, where it failed before its
                // first frame.
                let load: Option<LoadId> = name
                    .to_str()
                    .map(|name| name.strip_suffix(".lock").unwrap_or(name))
                    .and_then(|name| name.parse().ok());
                loads.extend(load);
            }
            loads.sort();
            loads.dedup();
            Ok(loads)
        })
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        let dir = self.pipeline_dir(pipeline).join(load.to_string());
        blocking(move || {
            let mut chunks = Vec::new();
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(chunks),
                Err(error) => return Err(error),
            };
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name();
                let number = name
                    .to_str()
                    .and_then(|name| name.strip_suffix(".wal"))
                    .and_then(|number| number.parse().ok());
                if let Some(number) = number {
                    chunks.push((number, entry.metadata()?.len()));
                }
            }
            chunks.sort_unstable();
            Ok(chunks)
        })
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        let path = self.chunk_path(pipeline, chunk);
        blocking(move || {
            let mut file = std::fs::File::open(path)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut bytes = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
            file.take(len).read_to_end(&mut bytes)?;
            Ok(Bytes::from(bytes))
        })
    }

    fn append<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        bytes: Bytes,
    ) -> BoxFuture<'a, io::Result<()>> {
        let path = self.chunk_path(pipeline, chunk);
        let file = self.open.lock().get(&path).cloned();
        let open = Arc::clone(&self.open);
        blocking(move || {
            let file = if let Some(file) = file {
                file
            } else {
                let dir = path.parent().unwrap_or(Path::new("."));
                private_dir(dir)?;
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)?;
                // The new chunk's name is durable before any of its frames is.
                std::fs::File::open(dir)?.sync_all()?;
                let file = Arc::new(Mutex::new(file));
                open.lock().insert(path, Arc::clone(&file));
                file
            };
            file.lock().write_all(&bytes)
        })
    }

    fn sync<'a>(&'a self, pipeline: &'a PipelineId, chunk: Chunk) -> BoxFuture<'a, io::Result<()>> {
        let path = self.chunk_path(pipeline, chunk);
        // A synced chunk is finished: its file goes, and an append after a failure opens it again.
        let file = self.open.lock().remove(&path);
        blocking(move || match file {
            Some(file) => file.lock().sync_data(),
            None => Ok(()),
        })
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        let path = self.chunk_path(pipeline, chunk);
        self.open.lock().remove(&path);
        blocking(move || {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            // The load's directory goes with its last chunk; one still holding chunks stays.
            if let Some(dir) = path.parent() {
                drop(std::fs::remove_dir(dir));
            }
            Ok(())
        })
    }
}
