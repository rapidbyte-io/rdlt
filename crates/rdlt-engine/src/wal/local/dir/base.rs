//! Opening a log's base: one directory at a time from the root, each checked on the descriptor
//! the next is opened from, so no directory is checked by one name and used by another.

use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Component, Path};

use rustix::fs::{CWD, Mode, OFlags};

use super::{BASE, BENEATH, Dir, Refusal, STICKY, owned};

/// What a test does with a base's path once the base is opened.
#[cfg(test)]
type Hook = Box<dyn Fn(&Path) + Send>;

/// What a test does once a base is opened, before anything more of it is checked.
#[cfg(test)]
pub(in crate::wal::local) static OPENED: parking_lot::Mutex<Option<Hook>> =
    parking_lot::Mutex::new(None);

impl Dir {
    /// The base at `path`, resolved as the embedder wrote it, created where missing with the
    /// missing directories above it, each private and durable in its parent.
    ///
    /// Each directory on the way, as written, is opened from the directory before it and checked
    /// on that descriptor: it must belong to this user or to root and be writable by no other
    /// unless it is sticky, so none can move what lies below it or a link on the way. A link is
    /// followed only out of a directory that passed. The base must belong to this user and be
    /// writable by no other, and every directory it lies in, walked up from it, must pass too.
    pub(in crate::wal::local) fn base(path: &Path) -> io::Result<Self> {
        let written = std::path::absolute(path)?;
        let mut dir = Self::at_root()?;
        for component in written.components() {
            match component {
                Component::Normal(name) => {
                    dir.passes()?;
                    dir = dir.step(name)?;
                }
                Component::ParentDir => dir = dir.step(OsStr::new(".."))?,
                Component::RootDir | Component::CurDir | Component::Prefix(_) => {}
            }
        }
        #[cfg(test)]
        if let Some(hook) = &*OPENED.lock() {
            hook(path);
        }
        let metadata = dir.file.metadata()?;
        owned(metadata.uid(), metadata.mode(), BASE, &dir.path)?;
        dir.parents()?;
        Ok(dir)
    }

    /// The root directory, open.
    fn at_root() -> io::Result<Self> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOCTTY;
        let fd = rustix::fs::openat(CWD, "/", flags, Mode::empty())?;
        Ok(Self {
            file: File::from(fd),
            path: "/".into(),
        })
    }

    /// The directory `name` in this one, opened from it: created private and durable here where
    /// missing, and a link followed, out of this directory, which passed.
    fn step(&self, name: &OsStr) -> io::Result<Self> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | BENEATH;
        let opened = match rustix::fs::openat(&self.file, name, flags, Mode::empty()) {
            Err(rustix::io::Errno::NOENT) => {
                match rustix::fs::mkdirat(&self.file, name, Mode::RWXU) {
                    Ok(()) => self.sync()?,
                    Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
                rustix::fs::openat(&self.file, name, flags, Mode::empty())
            }
            Err(rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR) => {
                let following = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
                rustix::fs::openat(&self.file, name, following, Mode::empty())
            }
            opened => opened,
        }?;
        Ok(Self {
            file: File::from(opened),
            path: self.path.join(name),
        })
    }

    /// Checks that the directory is one no other user can change what lies in it: this user's or
    /// root's, and writable by no other unless it is sticky.
    fn passes(&self) -> io::Result<()> {
        let metadata = self.file.metadata()?;
        let me = rustix::process::geteuid().as_raw();
        let why = if metadata.uid() != me && metadata.uid() != 0 {
            "another user owns a directory above it"
        } else if metadata.mode() & BASE != 0 && metadata.mode() & STICKY == 0 {
            "others may write a directory above it"
        } else {
            return Ok(());
        };
        Err(Refusal::NotPrivate {
            path: self.path.clone(),
            why,
        }
        .into())
    }

    /// Checks every directory the base lies in, each opened from the directory below it, up to
    /// the root: as the base is, not as its path was written.
    fn parents(&self) -> io::Result<()> {
        let mut dir = self.step(OsStr::new(".."))?;
        let mut below = self.file.metadata()?;
        loop {
            let metadata = dir.file.metadata()?;
            if (metadata.dev(), metadata.ino()) == (below.dev(), below.ino()) {
                return dir.passes();
            }
            dir.passes()?;
            below = metadata;
            dir = dir.step(OsStr::new(".."))?;
        }
    }
}
