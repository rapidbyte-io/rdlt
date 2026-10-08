//! A chunk a local log stages, in a file of its own beside where it is published.

use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Write as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::BoxFuture;

use super::dir::Dir;
use super::{Place, blocking, is_open, names, remove_emptied};
use crate::wal::store::{Chunk, StagedChunk};

/// How many names a staging tries before it gives up: each is new to the process, and only a
/// file an earlier process of the same id left can take one.
const STAGING_TRIES: usize = 64;

/// Counts the files the process stages, so no two of its stagings take one name.
static STAGINGS: AtomicU32 = AtomicU32::new(0);

/// A step of a publish that failed once the chunk's log was closed.
#[derive(Debug, thiserror::Error)]
#[error("the log was removed as the chunk was published")]
struct Removed(#[source] io::Error);

/// A chunk staged in a file of its own beside where it is published.
pub(super) struct Staged {
    place: Place,
    chunk: Chunk,
    part: String,
    file: Arc<Mutex<File>>,
}

impl StagedChunk for Staged {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        let file = Arc::clone(&self.file);
        blocking(move || file.lock().write_all(&bytes))
    }

    /// Makes the staged file durable, links it in under the chunk's name where that is free, and
    /// makes the directory durable, keeping the chunk only where the log is open still.
    ///
    /// A chunk linked in as the log is removed is unlinked again, refused as
    /// [`io::ErrorKind::NotFound`]. The staged name goes whatever happens, and with it the
    /// directory of a log removed meanwhile where that leaves it empty.
    ///
    /// A step that fails once the log is closed is refused as [`io::ErrorKind::NotFound`] too,
    /// whatever it failed with: a removal running alongside deletes what the publish links and
    /// syncs, and macOS fails some of those steps with `EINVAL` where Linux finds the name gone.
    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        blocking(move || match self.published() {
            Err(error) if error.kind() != io::ErrorKind::NotFound && !self.is_log_open()? => {
                // What cannot be taken back goes with a later removal, as after a crash.
                drop(self.withdrawn());
                Err(io::Error::new(io::ErrorKind::NotFound, Removed(error)))
            }
            published => published,
        })
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        blocking(move || self.discarded())
    }
}

impl Staged {
    /// Publishes the chunk, as [`StagedChunk::publish`] says, but for a step that fails.
    fn published(&self) -> io::Result<()> {
        let dir = self.place.load(self.chunk.load)?;
        let published = dir
            .as_ref()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
            .and_then(|dir| self.linked(dir));
        if let Some(dir) = &dir {
            dir.remove_file(OsStr::new(&self.part))?;
            dir.sync()?;
            if published.is_err() {
                self.emptied()?;
            }
        }
        published
    }

    /// Whether the log is open still: its directory is there and holds the file that marks it.
    fn is_log_open(&self) -> io::Result<bool> {
        match self.place.load(self.chunk.load)? {
            Some(dir) => is_open(&dir),
            None => Ok(false),
        }
    }

    /// Takes back what a publish refused for its log's removal left: the chunk's name where it
    /// is the staged file, and the staged name, as [`Staged::discarded`] does.
    fn withdrawn(&self) -> io::Result<()> {
        let Some(dir) = self.place.load(self.chunk.load)? else {
            return Ok(());
        };
        let name = names::chunk(self.chunk.number);
        if dir.same_file(&name, &self.file.lock())? {
            dir.remove_file(OsStr::new(&name))?;
        }
        self.unstaged(&dir)
    }

    /// Unlinks the staged file, where its log's directory is still there, and the directory of a
    /// log removed meanwhile where that leaves it empty.
    fn discarded(&self) -> io::Result<()> {
        match self.place.load(self.chunk.load)? {
            Some(dir) => self.unstaged(&dir),
            None => Ok(()),
        }
    }

    /// Unlinks the staged file from `dir`, the log's directory, durably, and the directory where
    /// the log was removed and that leaves it empty.
    fn unstaged(&self, dir: &Dir) -> io::Result<()> {
        dir.remove_file(OsStr::new(&self.part))?;
        dir.sync()?;
        self.emptied()
    }

    /// Removes the log's directory where its removal left it to this staging and nothing is left
    /// in it.
    fn emptied(&self) -> io::Result<()> {
        match self.place.pipeline()? {
            Some(pipeline) => remove_emptied(&pipeline, self.chunk.load),
            None => Ok(()),
        }
    }

    /// Links the staged file into `dir` under the chunk's name, as [`Staged::publish`] says.
    fn linked(&self, dir: &Dir) -> io::Result<()> {
        self.file.lock().sync_all()?;
        let name = names::chunk(self.chunk.number);
        // A staged file a removal deleted fails the link as `NotFound`.
        dir.link(&self.part, &name)?;
        // The name linked is the staged file's only where no other took its name meanwhile, as
        // a process of another process namespace may once the staging was deleted.
        if !dir.same_file(&name, &self.file.lock())? {
            dir.remove_file(OsStr::new(&name))?;
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "the staged file was deleted",
            ));
        }
        if !is_open(dir)? {
            dir.remove_file(OsStr::new(&name))?;
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "the log was removed",
            ));
        }
        Ok(())
    }
}

/// Stages chunk `chunk` in `place`, in a file of a name no other staging takes.
pub(super) fn staged(place: Place, chunk: Chunk) -> io::Result<Staged> {
    let dir = place.open_load(chunk.load)?;
    for _ in 0..STAGING_TRIES {
        let token = u64::from(std::process::id()) << 32
            | u64::from(STAGINGS.fetch_add(1, Ordering::Relaxed));
        let part = names::part(chunk.number, token);
        match dir.create(&part) {
            Ok(file) => {
                return Ok(Staged {
                    place,
                    chunk,
                    part,
                    file: Arc::new(Mutex::new(file)),
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("{STAGING_TRIES} names to stage a chunk in were taken"),
    ))
}
