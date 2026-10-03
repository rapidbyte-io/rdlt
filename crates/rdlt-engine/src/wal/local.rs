//! Write-ahead logs kept in a local directory.

mod dir;
mod names;
mod staged;
#[cfg(test)]
mod tests;

use std::ffi::{OsStr, OsString};
use std::io::{self, Read as _, Seek as _};
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

pub(crate) use self::dir::Refusal;
use self::dir::{Dir, Kind};
use super::store::{Chunk, StagedChunk, WalStore};

/// Write-ahead logs in a local directory: each pipeline's in a directory of its own, each load's
/// in a directory of that, each chunk a file.
///
/// A chunk is staged in a file of its own and published by linking that file in under the
/// chunk's name, which fails where the name exists, once the file is durable; the directory is
/// synced after, so a chunk published is whole and durable, and a crash loses only what was
/// staged.
///
/// The base is resolved as given the first time it is used, created where missing, and must
/// belong to the user the engine runs as and be writable by no other; every directory above it
/// must belong to that user or to root and be writable by no other, unless it is sticky. The
/// store keeps the base open from then on, so a base removed or replaced under it is never made
/// again or followed. Everything below it is reached one name at a time,
/// never through a link, and must be that user's alone: directories 0700, files 0600, on the
/// base's file system. Each is checked whenever it is opened, so a log is never read, written or
/// removed through what another user could reach. A name the store never writes, in a directory
/// it keeps a log's files in, is refused, never read.
#[derive(Debug)]
pub struct LocalWal {
    base: Arc<Base>,
}

/// Where a store's logs are: the base as the embedder named it, and the base open, once it
/// was first used.
#[derive(Debug)]
struct Base {
    path: PathBuf,
    open: Mutex<Option<Arc<Dir>>>,
}

impl Base {
    /// The base open: resolved and checked the first time, the same directory every time after.
    fn dir(&self) -> io::Result<Arc<Dir>> {
        let mut open = self.open.lock();
        if let Some(dir) = &*open {
            dir.base_again()?;
            return Ok(Arc::clone(dir));
        }
        let dir = Arc::new(Dir::base(&self.path)?);
        *open = Some(Arc::clone(&dir));
        Ok(dir)
    }
}

impl LocalWal {
    /// Logs under `base`, as `.rdlt` is by default.
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self {
            base: Arc::new(Base {
                path: base.into(),
                open: Mutex::new(None),
            }),
        }
    }

    /// The directory of `pipeline`'s logs, whose name no other pipeline's takes, a file system
    /// that folds case included: `p.` and the id where it holds no upper-case letter, `x.` and
    /// the id in lower-case base32 otherwise.
    pub fn pipeline_dir(&self, pipeline: &PipelineId) -> PathBuf {
        self.base.path.join(names::pipeline(pipeline))
    }

    fn place(&self, pipeline: &PipelineId) -> Place {
        Place {
            base: Arc::clone(&self.base),
            pipeline: names::pipeline(pipeline),
        }
    }
}

/// Where a pipeline's logs are: the base and the name of the pipeline's directory in it.
#[derive(Clone, Debug)]
struct Place {
    base: Arc<Base>,
    pipeline: String,
}

impl Place {
    /// The pipeline's directory, where it exists.
    fn pipeline(&self) -> io::Result<Option<Dir>> {
        self.base.dir()?.dir(&self.pipeline)
    }

    /// `load`'s directory, where it exists.
    fn load(&self, load: LoadId) -> io::Result<Option<Dir>> {
        match self.pipeline()? {
            Some(pipeline) => pipeline.dir(&names::load(load)),
            None => Ok(None),
        }
    }

    /// `load`'s directory, where its log is open: refused as [`io::ErrorKind::NotFound`]
    /// otherwise.
    fn open_load(&self, load: LoadId) -> io::Result<Dir> {
        let name = names::load(load);
        match self.load(load)? {
            Some(dir) if is_open(&dir)? => Ok(dir),
            _ => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("the log of load {name} is not open"),
            )),
        }
    }
}

/// Whether the log whose directory is `dir` is open.
fn is_open(dir: &Dir) -> io::Result<bool> {
    match dir.status(OsStr::new(names::OPEN))? {
        Some(status) if status.kind == Kind::File => {
            dir.private(OsStr::new(names::OPEN), &status)?;
            Ok(true)
        }
        Some(_) => Err(dir.refused(names::OPEN, "it is not a regular file")),
        None => Ok(false),
    }
}

/// Opens `load`'s log in `place`: its directory created, durable in the pipeline's, then the
/// file that marks it open, durable in it; a directory of the load already there, whatever it
/// holds, is refused with [`io::ErrorKind::AlreadyExists`].
fn open_log(place: &Place, load: LoadId) -> io::Result<()> {
    let pipeline = place.base.dir()?.dir_created(&place.pipeline)?;
    let name = names::load(load);
    let dir = pipeline.dir_new(&name)?;
    pipeline.sync()?;
    drop(dir.create(names::OPEN)?);
    dir.sync()
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

/// The error for `name` in `dir`, which no file or directory of a log takes.
fn stray(dir: &Dir, name: &OsStr) -> io::Error {
    Refusal::Stray { path: dir.at(name) }.into()
}

/// The loads of `place` whose logs are open where `open`, removed and left behind otherwise.
fn loads(place: &Place, open: bool) -> io::Result<Vec<LoadId>> {
    let Some(pipeline) = place.pipeline()? else {
        return Ok(Vec::new());
    };
    let mut loads = Vec::new();
    for name in pipeline.names()? {
        if names::is_made_by_system(&name) {
            continue;
        }
        let Some(status) = pipeline.status(&name)? else {
            continue;
        };
        let load = names::parse_load(&name).ok_or_else(|| stray(&pipeline, &name))?;
        if status.kind != Kind::Dir {
            return Err(pipeline.refused(&name, "it is not a directory"));
        }
        let Some(dir) = pipeline.dir(&names::load(load))? else {
            continue;
        };
        if is_open(&dir)? == open {
            loads.push(load);
        }
    }
    Ok(loads)
}

/// A load's published chunks, by number, each with its length, and the names of its staged
/// files.
type Listed = (Vec<(u64, u64)>, Vec<OsString>);

/// `dir`'s published chunks, by number, each with its length, and the names of its staged
/// files.
fn listed(dir: &Dir) -> io::Result<Listed> {
    let (mut chunks, mut parts) = (Vec::new(), Vec::new());
    for name in dir.names()? {
        let Some(status) = dir.status(&name)? else {
            continue;
        };
        if name == names::OPEN || names::is_made_by_system(&name) {
            continue;
        }
        let number = names::parse_chunk(&name);
        if number.is_none() && !names::is_part(&name) {
            return Err(stray(dir, &name));
        }
        if status.kind != Kind::File {
            return Err(dir.refused(&name, "it is not a regular file"));
        }
        dir.private(&name, &status)?;
        match number {
            Some(number) => chunks.push((number, status.len)),
            None => parts.push(name),
        }
    }
    chunks.sort_unstable();
    Ok((chunks, parts))
}

/// `len` bytes of chunk `chunk` in `place` from `offset`, fewer where the chunk ends first.
fn read(place: &Place, chunk: Chunk, offset: u64, len: u64) -> io::Result<Bytes> {
    let name = names::chunk(chunk.number);
    let mut file = place
        .load(chunk.load)?
        .map(|dir| dir.open(&name))
        .transpose()?
        .flatten()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, name))?;
    // What is read is held: never more than the file holds, whatever was asked.
    let held = file.metadata()?.len().saturating_sub(offset).min(len);
    let mut bytes = Vec::with_capacity(usize::try_from(held).unwrap_or(0));
    file.seek(io::SeekFrom::Start(offset))?;
    file.take(held).read_to_end(&mut bytes)?;
    Ok(Bytes::from(bytes))
}

/// Removes `load`'s log in `place`, durably: the file that marks it open first, then its staged
/// files and chunks, then its directory; a directory a staging still fills is left, removed, for
/// a later removal.
fn remove_log(place: &Place, load: LoadId) -> io::Result<()> {
    let Some(pipeline) = place.pipeline()? else {
        return Ok(());
    };
    let name = names::load(load);
    let Some(dir) = pipeline.dir(&name)? else {
        return Ok(());
    };
    let (chunks, parts) = listed(&dir)?;
    dir.remove_file(OsStr::new(names::OPEN))?;
    dir.sync()?;
    for part in parts {
        dir.remove_file(&part)?;
    }
    for (number, _) in chunks {
        dir.remove_file(OsStr::new(&names::chunk(number)))?;
    }
    // What a file system or a desktop made here goes too, where it lets it; one it keeps leaves
    // the directory for a later removal.
    for made in dir
        .names()?
        .into_iter()
        .filter(|name| names::is_made_by_system(name))
    {
        drop(dir.remove_file(&made));
    }
    dir.sync()?;
    match pipeline.remove_dir(&name) {
        Err(error) if error.raw_os_error() == Some(rustix::io::Errno::NOTEMPTY.raw_os_error()) => {
            return Ok(());
        }
        removed => removed?,
    }
    pipeline.sync()
}

/// Deletes the files `load`'s log in `place` staged, which need no room to go.
fn remove_staged(place: &Place, load: LoadId) -> io::Result<()> {
    let Some(dir) = place.load(load)? else {
        return Ok(());
    };
    for part in listed(&dir)?.1 {
        dir.remove_file(&part)?;
    }
    Ok(())
}

/// Deletes chunk `chunk` in `place`, durably.
fn remove(place: &Place, chunk: Chunk) -> io::Result<()> {
    let Some(dir) = place.load(chunk.load)? else {
        return Ok(());
    };
    let name = names::chunk(chunk.number);
    match dir.status(OsStr::new(&name))? {
        Some(status) if status.kind != Kind::File => {
            Err(dir.refused(&name, "it is not a regular file"))
        }
        Some(_) => {
            dir.remove_file(OsStr::new(&name))?;
            dir.sync()
        }
        None => Ok(()),
    }
}

impl WalStore for LocalWal {
    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let place = self.place(pipeline);
        blocking(move || open_log(&place, load))
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        let place = self.place(pipeline);
        Box::pin(async move {
            let staged = blocking(move || staged::staged(place, chunk)).await?;
            Ok(Box::new(staged) as Box<dyn StagedChunk>)
        })
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let place = self.place(pipeline);
        blocking(move || loads(&place, true))
    }

    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        let place = self.place(pipeline);
        blocking(move || loads(&place, false))
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        let place = self.place(pipeline);
        blocking(move || match place.load(load)? {
            Some(dir) => Ok(listed(&dir)?.0),
            None => Ok(Vec::new()),
        })
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        let place = self.place(pipeline);
        blocking(move || read(&place, chunk, offset, len))
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let place = self.place(pipeline);
        blocking(move || remove_staged(&place, load))
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        let place = self.place(pipeline);
        blocking(move || remove(&place, chunk))
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        let place = self.place(pipeline);
        blocking(move || remove_log(&place, load))
    }
}
