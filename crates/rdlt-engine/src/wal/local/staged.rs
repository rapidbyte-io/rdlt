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
    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        blocking(move || {
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
        })
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        blocking(move || self.discarded())
    }
}

impl Staged {
    /// Unlinks the staged file, where its log's directory is still there, and the directory of a
    /// log removed meanwhile where that leaves it empty.
    fn discarded(&self) -> io::Result<()> {
        let Some(dir) = self.place.load(self.chunk.load)? else {
            return Ok(());
        };
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
        dir.link(&self.part, &name)
            .map_err(|error| match error.raw_os_error() {
                Some(code) if code == rustix::io::Errno::NOENT.raw_os_error() => {
                    io::Error::new(io::ErrorKind::NotFound, "the log was removed")
                }
                _ => error,
            })?;
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
