//! Directories opened once, and everything beneath them reached by name: one path component at
//! a time, relative to an open directory, following no symbolic link.
//!
//! A name is never joined into a path the kernel resolves, so no name, link or rename leads out
//! of the directory a [`Dir`] was opened at. Only regular files are read, their sizes bounded
//! before a byte is; what is created is its owner's alone.

mod limited;
mod temporary;
#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) mod trace;

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, ErrorKind, Read as _};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
use std::path::{Path, PathBuf};

use rustix::fs::{AtFlags, CWD, FileType, Mode, OFlags};

use crate::limits::TREE_DEPTH;

pub(crate) use limited::Limited;
pub(crate) use temporary::unique;

/// Bytes: bounds one name, as file systems bound it.
const NAME_BYTES: usize = 255;

/// Why a name or what it names was refused, carried by the [`io::Error`] that refused it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Refusal {
    /// The name is not one normal path component.
    #[error("the name is not one path component")]
    Name,
    /// What the name names is a link, a pipe, a device, a socket or a directory, not a file.
    #[error("it is not a regular file")]
    NotRegular,
    /// The file is larger than its reader accepts.
    #[error("{name} is {actual}, over the limit of {limit}")]
    TooLarge {
        /// What was measured, with its unit.
        name: &'static str,
        /// The limit.
        limit: u64,
        /// The size seen: the file's, or the first beyond the limit where it grew while read.
        actual: u64,
    },
    /// What the name names is a link, a file or anything else that is no directory.
    #[error("it is not a directory")]
    NotDirectory,
    /// The directory or file belongs to another user, or its group or others may write it.
    #[error(
        "it belongs to user {owner} with mode {mode:o}: it must be this user's and writable by no other"
    )]
    Shared {
        /// Its owner's user id.
        owner: u32,
        /// Its permission bits.
        mode: u32,
    },
    /// The directory is on another file system than the directory it was reached from.
    #[error("it is a mount point")]
    Mounted,
    /// The directory lies deeper beneath its root than a tree is entered.
    #[error("it lies more than {limit} directories deep")]
    TooDeep {
        /// The depth to which a tree is entered.
        limit: usize,
    },
}

impl From<Refusal> for io::Error {
    fn from(refusal: Refusal) -> Self {
        let kind = match refusal {
            Refusal::Name | Refusal::TooDeep { .. } => ErrorKind::InvalidInput,
            Refusal::NotRegular | Refusal::TooLarge { .. } => ErrorKind::InvalidData,
            Refusal::NotDirectory | Refusal::Mounted => ErrorKind::NotADirectory,
            Refusal::Shared { .. } => ErrorKind::PermissionDenied,
        };
        Self::new(kind, refusal)
    }
}

/// The refusal `error` carries, if one refused.
pub(crate) fn refusal(error: &io::Error) -> Option<Refusal> {
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<Refusal>())
        .copied()
}

/// A size limit and what it measures, with its unit, as a refusal names it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limit {
    pub(crate) name: &'static str,
    pub(crate) bytes: u64,
}

impl Limit {
    /// Admits a size of `actual` bytes if it is at most the limit.
    pub(crate) fn admit(self, actual: u64) -> Result<(), Refusal> {
        match self.bytes.checked_sub(actual) {
            Some(_) => Ok(()),
            None => Err(Refusal::TooLarge {
                name: self.name,
                limit: self.bytes,
                actual,
            }),
        }
    }
}

/// What a directory entry is, links never followed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Kind {
    Dir,
    File,
    /// A link, a pipe, a device or a socket.
    Other,
}

impl From<FileType> for Kind {
    fn from(kind: FileType) -> Self {
        match kind {
            FileType::Directory => Self::Dir,
            FileType::RegularFile => Self::File,
            _ => Self::Other,
        }
    }
}

/// Checks that `name` is one normal path component: not empty, `.` or `..`, of no separator or
/// NUL byte, and at most 255 bytes.
pub(crate) fn component(name: &OsStr) -> io::Result<&OsStr> {
    let bytes = name.as_bytes();
    let normal = !bytes.is_empty()
        && bytes.len() <= NAME_BYTES
        && bytes != b"."
        && bytes != b".."
        && !bytes.contains(&b'/')
        && !bytes.contains(&0);
    if normal {
        Ok(name)
    } else {
        Err(Refusal::Name.into())
    }
}

/// The components of `path`, a relative path of `/`-separated names, each checked as
/// [`component`] checks one.
pub(crate) fn components(path: &str) -> io::Result<Vec<&str>> {
    path.split('/')
        .map(|name| component(OsStr::new(name)).map(|_| name))
        .collect()
}

/// An open directory.
#[derive(Debug)]
pub(crate) struct Dir {
    file: File,
    /// Where the directory was reached, for messages; never resolved again.
    path: PathBuf,
}

/// The flags every open carries: no link is followed, no descriptor outlives an exec, and no
/// terminal opened by mistake becomes the process's own.
const BENEATH: OFlags = OFlags::NOFOLLOW
    .union(OFlags::CLOEXEC)
    .union(OFlags::NOCTTY);

/// The mode of a created directory and of a created file: their owner's alone.
const PRIVATE_DIR: Mode = Mode::RWXU;
const PRIVATE_FILE: Mode = Mode::RUSR.union(Mode::WUSR);

impl Dir {
    /// Opens the directory at `path`, a root its operator named: the path is resolved as the
    /// operator wrote it, links included, and the directory it leads to must be private.
    ///
    /// # Errors
    ///
    /// A [`Refusal::Shared`] for a directory of another user, or one its group or others may
    /// write: whoever can write a root can plant names in it.
    pub(crate) fn ambient(path: &Path) -> io::Result<Self> {
        let root = Self::resolved(path)?;
        root.private()?;
        Ok(root)
    }

    /// Opens the directory at `path` as the path resolves, whoever it belongs to.
    fn resolved(path: &Path) -> io::Result<Self> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOCTTY;
        let fd = rustix::fs::openat(CWD, path, flags, Mode::empty())?;
        Ok(Self {
            file: File::from(fd),
            path: path.to_owned(),
        })
    }

    /// Opens the directory at `path` whoever it belongs to, for tests of what a root holds.
    #[cfg(test)]
    pub(crate) fn trusted(path: &Path) -> io::Result<Self> {
        Self::resolved(path)
    }

    /// Opens the directory at `path` as [`Dir::ambient`] does, first creating it and whichever of
    /// its ancestors are missing, private to their owner and durable in their parents.
    pub(crate) fn ambient_created(path: &Path) -> io::Result<Self> {
        let missing: Vec<&Path> = path
            .ancestors()
            .take_while(|ancestor| !ancestor.as_os_str().is_empty() && !ancestor.exists())
            .collect();
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)?;
        for created in missing.iter().rev() {
            // A relative directory of one component has the empty path as its parent.
            let parent = created
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            Self::resolved(parent)?.sync()?;
        }
        Self::ambient(path)
    }

    /// Where the directory was reached, for messages.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Where `name` in the directory is, for messages.
    pub(crate) fn at(&self, name: impl AsRef<OsStr>) -> PathBuf {
        self.path.join(name.as_ref())
    }

    /// Opens the directory `name`, which must be private as its root is and on its file system.
    ///
    /// # Errors
    ///
    /// A [`Refusal::NotDirectory`] for a link or anything else that is no directory, whatever
    /// error the platform answers such an open with; a [`Refusal::Shared`] for a directory of
    /// another user or one others may write; a [`Refusal::Mounted`] for a mount point.
    pub(crate) fn dir(&self, name: impl AsRef<OsStr>) -> io::Result<Self> {
        use rustix::io::Errno;
        let name = component(name.as_ref())?;
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | BENEATH;
        let fd = match rustix::fs::openat(&self.file, name, flags, Mode::empty()) {
            Ok(fd) => fd,
            Err(Errno::NOTDIR | Errno::LOOP | Errno::MLINK) => {
                return Err(Refusal::NotDirectory.into());
            }
            Err(error) => return Err(error.into()),
        };
        let dir = Self {
            file: File::from(fd),
            path: self.path.join(name),
        };
        if dir.file.metadata()?.dev() != self.file.metadata()?.dev() {
            return Err(Refusal::Mounted.into());
        }
        dir.private()?;
        Ok(dir)
    }

    /// Opens the directory `name`, creating it where missing, private to its owner and durable
    /// in this one.
    pub(crate) fn dir_created(&self, name: impl AsRef<OsStr>) -> io::Result<Self> {
        let name = component(name.as_ref())?;
        #[cfg(test)]
        let known = self.kind(name)?.is_some();
        #[cfg(test)]
        if !known {
            trace::step(trace::Step::MakeDir(self.at(name)))?;
        }
        match rustix::fs::mkdirat(&self.file, name, PRIVATE_DIR) {
            Ok(()) => self.sync()?,
            Err(rustix::io::Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        self.dir(name)
    }

    /// Opens the directory the `names` lead to, one after another.
    pub(crate) fn walk<N: AsRef<OsStr>>(
        &self,
        names: impl IntoIterator<Item = N>,
    ) -> io::Result<Self> {
        let mut names = names.into_iter();
        let Some(first) = names.next() else {
            return Err(Refusal::Name.into());
        };
        names.try_fold(self.dir(first)?, |dir, name| dir.dir(name))
    }

    /// Opens the directory the `names` lead to, creating each where missing as
    /// [`Dir::dir_created`] does.
    pub(crate) fn walk_created<N: AsRef<OsStr>>(
        &self,
        names: impl IntoIterator<Item = N>,
    ) -> io::Result<Self> {
        let mut names = names.into_iter();
        let Some(first) = names.next() else {
            return Err(Refusal::Name.into());
        };
        names.try_fold(self.dir_created(first)?, |dir, name| dir.dir_created(name))
    }

    /// Opens the regular file `name` to read; anything else of that name is refused, a pipe
    /// without waiting for its writer.
    pub(crate) fn file(&self, name: impl AsRef<OsStr>) -> io::Result<File> {
        let name = component(name.as_ref())?;
        // What is no regular file is refused before it is opened: opening a device may act on it.
        if self.kind(name)?.is_some_and(|kind| kind != Kind::File) {
            return Err(Refusal::NotRegular.into());
        }
        self.opened(name)
    }

    /// Opens `name` to read without following a link or waiting on a pipe, and refuses what the
    /// descriptor shows to be no regular file: the name may have changed since it was inspected.
    fn opened(&self, name: &OsStr) -> io::Result<File> {
        let flags = OFlags::RDONLY | OFlags::NONBLOCK | BENEATH;
        let file = match rustix::fs::openat(&self.file, name, flags, Mode::empty()) {
            Ok(fd) => File::from(fd),
            // The name became a link since it was inspected.
            Err(rustix::io::Errno::LOOP | rustix::io::Errno::MLINK) => {
                return Err(Refusal::NotRegular.into());
            }
            Err(error) => return Err(error.into()),
        };
        if !file.metadata()?.is_file() {
            return Err(Refusal::NotRegular.into());
        }
        Ok(file)
    }

    /// The bytes of the regular file `name`, which holds at most `limit`.
    pub(crate) fn read(&self, name: impl AsRef<OsStr>, limit: Limit) -> io::Result<Vec<u8>> {
        let file = self.file(name)?;
        limit.admit(file.metadata()?.len())?;
        within(file, limit)
    }

    /// The bytes of the regular file `name`, as [`Dir::read`] reads them, which must be this
    /// user's alone to write: what another user wrote or may write is not taken for the file's
    /// owner's.
    pub(crate) fn read_private(
        &self,
        name: impl AsRef<OsStr>,
        limit: Limit,
    ) -> io::Result<Vec<u8>> {
        let file = self.file(name)?;
        private(&file)?;
        limit.admit(file.metadata()?.len())?;
        within(file, limit)
    }

    /// The file system and the file on it that the directory is, which no two directories
    /// share and every path to one directory leads to.
    pub(crate) fn identity(&self) -> io::Result<(u64, u64)> {
        let metadata = self.file.metadata()?;
        Ok((metadata.dev(), metadata.ino()))
    }

    /// Creates the file `name` to write, private to its owner; a name that exists, a link
    /// included, is refused.
    pub(crate) fn create(&self, name: impl AsRef<OsStr>) -> io::Result<File> {
        let name = component(name.as_ref())?;
        #[cfg(test)]
        trace::step(trace::Step::Create(self.at(name)))?;
        let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | BENEATH;
        let fd = rustix::fs::openat(&self.file, name, flags, PRIVATE_FILE)?;
        Ok(File::from(fd))
    }

    /// What `name` is, if it exists.
    pub(crate) fn kind(&self, name: impl AsRef<OsStr>) -> io::Result<Option<Kind>> {
        let name = component(name.as_ref())?;
        match rustix::fs::statat(&self.file, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => Ok(Some(FileType::from_raw_mode(stat.st_mode).into())),
            Err(rustix::io::Errno::NOENT) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// The directory's entries and what each is, in name order.
    pub(crate) fn entries(&self) -> io::Result<Vec<(OsString, Kind)>> {
        let mut entries = Vec::new();
        for entry in rustix::fs::Dir::read_from(&self.file)? {
            let entry = entry?;
            let name = entry.file_name().to_bytes();
            if name == b"." || name == b".." {
                continue;
            }
            let name = OsString::from_vec(name.to_vec());
            let kind = match entry.file_type() {
                // A file system that does not tell an entry's type is asked for it.
                FileType::Unknown => match self.kind(&name)? {
                    Some(kind) => kind,
                    None => continue,
                },
                kind => kind.into(),
            };
            entries.push((name, kind));
        }
        entries.sort();
        Ok(entries)
    }

    /// Removes the entry `name` that is no directory: a link itself, never what it leads to.
    pub(crate) fn remove_file(&self, name: impl AsRef<OsStr>) -> io::Result<()> {
        let name = component(name.as_ref())?;
        #[cfg(test)]
        trace::step(trace::Step::Remove(self.at(name)))?;
        Ok(rustix::fs::unlinkat(&self.file, name, AtFlags::empty())?)
    }

    /// Removes the empty directory `name`.
    pub(crate) fn remove_dir(&self, name: impl AsRef<OsStr>) -> io::Result<()> {
        let name = component(name.as_ref())?;
        #[cfg(test)]
        trace::step(trace::Step::RemoveDir(self.at(name)))?;
        Ok(rustix::fs::unlinkat(&self.file, name, AtFlags::REMOVEDIR)?)
    }

    /// Removes `name` and, where it is a directory, everything beneath it; a link is removed
    /// itself and never followed, and a name already gone is no error.
    ///
    /// # Errors
    ///
    /// A [`Refusal::TooDeep`] for a tree deeper than [`TREE_DEPTH`], which is left as it is.
    pub(crate) fn remove_tree(&self, name: impl AsRef<OsStr>) -> io::Result<()> {
        self.remove_within(component(name.as_ref())?, TREE_DEPTH)
    }

    /// Removes `name` as [`Dir::remove_tree`] does, entering at most `depth` directories.
    fn remove_within(&self, name: &OsStr, depth: usize) -> io::Result<()> {
        let gone = |removed: io::Result<()>| match removed {
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            removed => removed,
        };
        match self.kind(name)? {
            None => Ok(()),
            Some(Kind::Dir) => {
                let Some(depth) = depth.checked_sub(1) else {
                    return Err(Refusal::TooDeep { limit: TREE_DEPTH }.into());
                };
                let dir = match self.dir(name) {
                    Ok(dir) => dir,
                    Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error),
                };
                for (entry, _) in dir.entries()? {
                    dir.remove_within(&entry, depth)?;
                }
                gone(self.remove_dir(name))
            }
            Some(_) => gone(self.remove_file(name)),
        }
    }

    /// Moves `name` to `to` in `into`, replacing what is there.
    pub(crate) fn rename(
        &self,
        name: impl AsRef<OsStr>,
        into: &Self,
        to: impl AsRef<OsStr>,
    ) -> io::Result<()> {
        let (name, to) = (component(name.as_ref())?, component(to.as_ref())?);
        #[cfg(test)]
        trace::step(trace::Step::Rename(into.at(to)))?;
        Ok(rustix::fs::renameat(&self.file, name, &into.file, to)?)
    }

    /// Makes the directory's entries durable, so a file created, linked or renamed in it
    /// survives a crash.
    pub(crate) fn sync(&self) -> io::Result<()> {
        #[cfg(test)]
        trace::step(trace::Step::SyncDir(self.path.clone()))?;
        self.file.sync_all()
    }

    /// Checks that the directory is its user's alone to write: it belongs to the user the
    /// process runs as, and neither its group nor others may write it.
    pub(crate) fn private(&self) -> io::Result<()> {
        private(&self.file)
    }
}

/// Every byte of `reader`, which holds at most `limit`: one byte beyond the limit refuses a
/// reader that holds more than it was measured to.
fn within(reader: impl io::Read, limit: Limit) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(limit.bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    limit.admit(u64::try_from(bytes.len()).unwrap_or(u64::MAX))?;
    Ok(bytes)
}

/// Checks that the open `file` belongs to the user the process runs as, and that neither its
/// group nor others may write it.
pub(crate) fn private(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    let ours = metadata.uid() == rustix::process::geteuid().as_raw();
    if ours && metadata.mode() & 0o022 == 0 {
        Ok(())
    } else {
        Err(Refusal::Shared {
            owner: metadata.uid(),
            mode: metadata.mode() & 0o7777,
        }
        .into())
    }
}
