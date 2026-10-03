//! A local log's directories, opened one name at a time beneath an open directory, following no
//! link, each refused unless it is its user's alone.
//!
//! A name is never joined into a path the kernel resolves below the base, so no name, link or
//! rename leads a log out of the directory its pipeline's base was opened at.

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use rustix::fs::{AtFlags, FileType, Mode, OFlags};

mod base;

/// Why something of a local log was refused, carried by the [`io::Error`] that refused it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Refusal {
    /// A directory or file of the log another user owns, or others may reach, or a link, or of
    /// another kind than the log keeps there, or on another file system.
    #[error("{path} is not this user's alone: {why}")]
    NotPrivate {
        /// Where it is.
        path: PathBuf,
        /// What makes it so.
        why: &'static str,
    },
    /// A name the store never writes, in a directory it keeps a log's files in.
    #[error("{path} is no file or directory a log is kept in")]
    Stray {
        /// Where it is.
        path: PathBuf,
    },
}

impl From<Refusal> for io::Error {
    fn from(refusal: Refusal) -> Self {
        let kind = match refusal {
            Refusal::NotPrivate { .. } => io::ErrorKind::PermissionDenied,
            Refusal::Stray { .. } => io::ErrorKind::InvalidData,
        };
        Self::new(kind, refusal)
    }
}

/// The flags every open beneath the base carries: no link is followed, no descriptor outlives
/// an exec, and no terminal opened by mistake becomes the process's own.
const BENEATH: OFlags = OFlags::NOFOLLOW
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOCTTY);

/// Permission bits neither a group nor others may hold on what is beneath the base.
const PRIVATE: u32 = 0o077;

/// Permission bits neither a group nor others may hold on the base: none may write it.
const BASE: u32 = 0o022;

/// The bit that keeps whoever may write a directory from removing what others own in it.
const STICKY: u32 = 0o1000;

/// What a directory entry is, links never followed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Dir,
    File,
    /// A link, a pipe, a device or a socket.
    Other,
}

/// What a directory entry is and where it lives, links never followed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Status {
    pub(super) kind: Kind,
    /// Bytes it holds.
    pub(super) len: u64,
    owner: u32,
    mode: u32,
}

/// An open directory of a local log.
#[derive(Debug)]
pub(super) struct Dir {
    file: File,
    /// Where it was reached, for messages; never resolved again.
    path: PathBuf,
}

impl Dir {
    /// Checks the base, held open, again: it must still be linked where it was reached, which
    /// a base removed is not, refused as [`io::ErrorKind::NotFound`], and still be this user's
    /// and writable by no other.
    pub(super) fn base_again(&self) -> io::Result<()> {
        let metadata = self.file.metadata()?;
        if metadata.nlink() == 0 {
            let removed = format!("{} was removed", self.path.display());
            return Err(io::Error::new(io::ErrorKind::NotFound, removed));
        }
        owned(metadata.uid(), metadata.mode(), BASE, &self.path)
    }

    /// Where `name` in the directory is, for messages.
    pub(super) fn at(&self, name: impl AsRef<OsStr>) -> PathBuf {
        self.path.join(name.as_ref())
    }

    /// The directory `name`, where it exists: it must be a directory of this user's alone, on
    /// this directory's file system.
    pub(super) fn dir(&self, name: &str) -> io::Result<Option<Self>> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | BENEATH;
        let fd = match rustix::fs::openat(&self.file, name, flags, Mode::empty()) {
            Ok(fd) => fd,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(rustix::io::Errno::NOTDIR | rustix::io::Errno::LOOP | rustix::io::Errno::MLINK) => {
                return Err(self.refused(name, "it is not a directory"));
            }
            Err(error) => return Err(error.into()),
        };
        let dir = Self {
            file: File::from(fd),
            path: self.at(name),
        };
        let metadata = dir.file.metadata()?;
        if metadata.dev() != self.file.metadata()?.dev() {
            return Err(self.refused(name, "it is on another file system"));
        }
        owned(metadata.uid(), metadata.mode(), PRIVATE, &dir.path)?;
        Ok(Some(dir))
    }

    /// The directory `name`, created private, which the caller makes durable here.
    ///
    /// One that exists is refused with [`io::ErrorKind::AlreadyExists`].
    pub(super) fn dir_new(&self, name: &str) -> io::Result<Self> {
        rustix::fs::mkdirat(&self.file, name, Mode::RWXU)?;
        self.dir(name)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, self.at(name).display().to_string())
        })
    }

    /// The directory `name`, created where missing, private and durable in this one.
    pub(super) fn dir_created(&self, name: &str) -> io::Result<Self> {
        match rustix::fs::mkdirat(&self.file, name, Mode::RWXU) {
            Ok(()) => self.sync()?,
            Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        self.dir(name)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, self.at(name).display().to_string())
        })
    }

    /// Creates the file `name` to write, this user's alone; a name that exists, a link
    /// included, is refused with [`io::ErrorKind::AlreadyExists`].
    pub(super) fn create(&self, name: &str) -> io::Result<File> {
        let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | BENEATH;
        let mode = Mode::RUSR | Mode::WUSR;
        let file = File::from(rustix::fs::openat(&self.file, name, flags, mode)?);
        self.private_file(name, &file)?;
        Ok(file)
    }

    /// Opens the file `name` to read, where it exists: it must be a regular file of this user's
    /// alone.
    pub(super) fn open(&self, name: &str) -> io::Result<Option<File>> {
        // A pipe opened to read answers at once rather than waiting for its writer.
        let flags = OFlags::RDONLY | OFlags::NONBLOCK | BENEATH;
        let file = match rustix::fs::openat(&self.file, name, flags, Mode::empty()) {
            Ok(fd) => File::from(fd),
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(rustix::io::Errno::LOOP | rustix::io::Errno::MLINK) => {
                return Err(self.refused(name, "it is a link"));
            }
            Err(error) => return Err(error.into()),
        };
        self.private_file(name, &file)?;
        Ok(Some(file))
    }

    /// Renames the entry `name` to `to`, where no entry of that name exists: one that does is
    /// refused with [`io::ErrorKind::AlreadyExists`], and nothing is replaced.
    pub(super) fn rename_new(&self, name: &str, to: &str) -> io::Result<()> {
        let flags = rustix::fs::RenameFlags::NOREPLACE;
        rustix::fs::renameat_with(&self.file, name, &self.file, to, flags)?;
        Ok(())
    }

    /// Links the file `name` in as `to`, where no entry of that name exists: one that does is
    /// refused with [`io::ErrorKind::AlreadyExists`].
    pub(super) fn link(&self, name: &str, to: &str) -> io::Result<()> {
        rustix::fs::linkat(&self.file, name, &self.file, to, AtFlags::empty())?;
        Ok(())
    }

    /// Checks that the open `file`, named `name` here, is a regular file of this user's alone.
    fn private_file(&self, name: &str, file: &File) -> io::Result<()> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(self.refused(name, "it is not a regular file"));
        }
        owned(metadata.uid(), metadata.mode(), PRIVATE, &self.at(name))
    }

    /// Whether the entry `name`, a link never followed, is the open `file`.
    pub(super) fn same_file(&self, name: &str, file: &File) -> io::Result<bool> {
        let Some(linked) = self.open(name)? else {
            return Ok(false);
        };
        let (linked, staged) = (linked.metadata()?, file.metadata()?);
        Ok(linked.dev() == staged.dev() && linked.ino() == staged.ino())
    }

    /// What `name` is, where it exists, asked of the directory and never of a descriptor of the
    /// entry: closing any descriptor of a file releases the locks the process holds on it.
    pub(super) fn status(&self, name: &OsStr) -> io::Result<Option<Status>> {
        #[cfg_attr(
            target_os = "linux",
            expect(
                clippy::useless_conversion,
                reason = "a mode is a narrower number on other platforms"
            )
        )]
        match rustix::fs::statat(&self.file, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => Ok(Some(Status {
                kind: match FileType::from_raw_mode(stat.st_mode) {
                    FileType::Directory => Kind::Dir,
                    FileType::RegularFile => Kind::File,
                    _ => Kind::Other,
                },
                len: u64::try_from(stat.st_size).unwrap_or(0),
                owner: stat.st_uid,
                mode: u32::from(stat.st_mode) & 0o7777,
            })),
            Err(rustix::io::Errno::NOENT) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Checks that `status`, of the entry `name`, is one of this user's alone.
    pub(super) fn private(&self, name: &OsStr, status: &Status) -> io::Result<()> {
        owned(status.owner, status.mode, PRIVATE, &self.at(name))
    }

    /// The names of the directory's entries, in name order.
    pub(super) fn names(&self) -> io::Result<Vec<OsString>> {
        let mut names = Vec::new();
        for entry in rustix::fs::Dir::read_from(&self.file)? {
            let entry = entry?;
            let name = entry.file_name().to_bytes();
            if name != b"." && name != b".." {
                names.push(OsStr::from_bytes(name).to_owned());
            }
        }
        names.sort();
        Ok(names)
    }

    /// Removes the entry `name` that is no directory, a link itself; a name gone is no error.
    pub(super) fn remove_file(&self, name: &OsStr) -> io::Result<()> {
        gone(rustix::fs::unlinkat(&self.file, name, AtFlags::empty()))
    }

    /// Removes the empty directory `name`; a name gone is no error.
    pub(super) fn remove_dir(&self, name: &str) -> io::Result<()> {
        gone(rustix::fs::unlinkat(&self.file, name, AtFlags::REMOVEDIR))
    }

    /// Makes the directory's entries durable: a name created or removed in it survives a crash.
    pub(super) fn sync(&self) -> io::Result<()> {
        self.file.sync_all()?;
        #[cfg(test)]
        SYNCED.lock().push(self.path.clone());
        Ok(())
    }

    /// The error refusing `name` here, as `why` says.
    pub(super) fn refused(&self, name: impl AsRef<OsStr>, why: &'static str) -> io::Error {
        Refusal::NotPrivate {
            path: self.at(name),
            why,
        }
        .into()
    }
}

#[cfg(test)]
pub(super) use base::{OPENED, link_followed};

/// The directories synced, in order, for tests to see which names were made durable.
#[cfg(test)]
pub(super) static SYNCED: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::Mutex::new(Vec::new());

/// Checks that what `owner` owns with `mode`, at `path`, is this user's and grants none of
/// `reach` to its group or others.
pub(super) fn owned(owner: u32, mode: u32, reach: u32, path: &Path) -> io::Result<()> {
    let why = if owner != rustix::process::geteuid().as_raw() {
        "another user owns it"
    } else if mode & reach != 0 {
        "its group or others may reach it"
    } else {
        return Ok(());
    };
    Err(Refusal::NotPrivate {
        path: path.to_owned(),
        why,
    }
    .into())
}

/// `removed`, a name gone counted as removed.
fn gone(removed: rustix::io::Result<()>) -> io::Result<()> {
    match removed {
        Ok(()) | Err(rustix::io::Errno::NOENT) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
