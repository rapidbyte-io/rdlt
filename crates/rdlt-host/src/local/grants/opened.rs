//! Paths opened once, and known from then on by what was opened: what is checked of a grant
//! and what a sandbox binds are the same object, whatever its path names later.

use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use rustix::fs::{Mode, OFlags};

/// An object's identity: its device and its inode.
type Identity = (u64, u64);

/// Directories a chain climbs at most: a deeper one is refused rather than followed.
const DEPTH: usize = 4096;

/// How a path is opened to be known: on Linux as a location alone, which reads nothing of it.
#[cfg(target_os = "linux")]
const LOOKED_AT: OFlags = OFlags::PATH.union(OFlags::CLOEXEC);

/// How a path is opened to be known: to be read, which this platform needs to open it at all.
#[cfg(not(target_os = "linux"))]
const LOOKED_AT: OFlags = OFlags::RDONLY
    .union(OFlags::CLOEXEC)
    .union(OFlags::NONBLOCK)
    .union(OFlags::NOCTTY);

/// What an opened object is, and every directory above it up to the root, by identity: where
/// it lies, whichever names lead to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Chain(Vec<Identity>);

impl Chain {
    /// The chain of `file`, open, which was reached at `path`.
    pub(crate) fn of(file: &File, path: &Path) -> io::Result<Self> {
        let metadata = file.metadata()?;
        let mut chain = vec![identity(&metadata)];
        let mut directory = if metadata.is_dir() {
            up(file)?
        } else {
            holding(file, &metadata, path)?
        };
        for _ in 0..DEPTH {
            let at = identity(&directory.metadata()?);
            // The root is its own parent.
            if chain.last() == Some(&at) {
                return Ok(Self(chain));
            }
            chain.push(at);
            directory = up(&directory)?;
        }
        Err(io::Error::other(
            "the path lies deeper than any is followed",
        ))
    }

    /// Whether this object is `other`, or lies beneath it.
    pub(crate) fn within(&self, other: &Self) -> bool {
        other.0.first().is_some_and(|other| self.0.contains(other))
    }

    /// Whether this object lies within `other`, or `other` within this.
    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        self.within(other) || other.within(self)
    }

    /// The chain of the directory this object lies in; none for the root.
    pub(crate) fn parent(&self) -> Option<Self> {
        (self.0.len() > 1).then(|| Self(self.0[1..].to_vec()))
    }
}

/// A path opened once, its links followed then and never again.
#[derive(Debug)]
pub(crate) struct Opened {
    /// What was opened; on Linux, open as a location alone.
    pub(crate) file: File,
    /// Where it lies.
    pub(crate) chain: Chain,
}

impl Opened {
    /// Opens `path`, an absolute path.
    pub(crate) fn at(path: &Path) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the path is not absolute",
            ));
        }
        let file = File::from(rustix::fs::open(path, LOOKED_AT, Mode::empty())?);
        let chain = Chain::of(&file, path)?;
        Ok(Self { file, chain })
    }

    /// Opens `path`, an absolute path, or where it is not there the nearest directory above it
    /// that is; and whether that is `path` itself.
    pub(crate) fn nearest(path: &Path) -> io::Result<(Self, bool)> {
        for (above, ancestor) in path.ancestors().enumerate() {
            match Self::at(ancestor) {
                Ok(opened) => return Ok((opened, above == 0)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the path is not absolute",
        ))
    }
}

fn identity(metadata: &std::fs::Metadata) -> Identity {
    (metadata.dev(), metadata.ino())
}

/// The directory `directory` lies in.
fn up(directory: &File) -> io::Result<File> {
    let flags = LOOKED_AT | OFlags::DIRECTORY;
    Ok(File::from(rustix::fs::openat(
        directory,
        "..",
        flags,
        Mode::empty(),
    )?))
}

/// The directory that holds `file`, which is no directory and was reached at `path`, seen to
/// hold an entry that is `file` itself, as it is now.
fn holding(file: &File, metadata: &std::fs::Metadata, path: &Path) -> io::Result<File> {
    let real = located(file, path)?;
    let moved = || io::Error::other("the path changed while it was opened");
    let (Some(parent), Some(name)) = (real.parent(), real.file_name()) else {
        return Err(moved());
    };
    let parent = File::from(rustix::fs::open(
        parent,
        LOOKED_AT | OFlags::DIRECTORY,
        Mode::empty(),
    )?);
    let entry = rustix::fs::openat(&parent, name, LOOKED_AT | OFlags::NOFOLLOW, Mode::empty())?;
    if identity(&File::from(entry).metadata()?) != identity(metadata) {
        return Err(moved());
    }
    Ok(parent)
}

/// Where `file`, reached at `path`, is now, every link resolved: on Linux as the kernel names
/// what is open; elsewhere `path` is resolved again, where no sandbox runs whose grants the
/// answer could mislead.
fn located(file: &File, path: &Path) -> io::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    let located = {
        use std::os::fd::AsRawFd as _;
        let _ = path;
        std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
    };
    #[cfg(not(target_os = "linux"))]
    let located = {
        let _ = file;
        std::fs::canonicalize(path)
    };
    located
}
