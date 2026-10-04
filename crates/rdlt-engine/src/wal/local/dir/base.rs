//! Opening a log's base: one directory at a time from the root, each checked on the descriptor
//! the next is opened from, so no directory is checked by one name and used by another.

use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
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

/// The most links a walk to a base follows, as many as the kernel follows resolving a path.
const LINKS: usize = 40;

/// The error a step answers a link with, which a walk then reads.
const LINKED: i32 = rustix::io::Errno::LOOP.raw_os_error();

/// Whether a link `owner` owns, in a directory of `mode`, may be followed by user `me`: in a
/// directory others may write, only one of `me`'s or root's.
pub(in crate::wal::local) fn link_followed(mode: u32, owner: u32, me: u32) -> bool {
    mode & BASE == 0 || owner == me || owner == 0
}

/// Whether a walk to a base makes the directories missing on the way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Making {
    Missing,
    Nothing,
}

/// The names of `path`'s components, `..` among them, in order.
fn names(path: &Path) -> VecDeque<OsString> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(name) => Some(name.to_owned()),
            Component::ParentDir => Some(OsString::from("..")),
            Component::RootDir | Component::CurDir | Component::Prefix(_) => None,
        })
        .collect()
}

impl Dir {
    /// The base at `path`, resolved as the embedder wrote it, created where missing with the
    /// missing directories above it, each private and durable in its parent.
    ///
    /// Each directory on the way is opened from the directory before it, never following a
    /// link, and checked on that descriptor: it must belong to this user or to root and be
    /// writable by no other unless it is sticky, so none can move what lies below it or a link on
    /// the way. A link is read out of a directory that passed and its target walked the same way,
    /// at most [`LINKS`] of them. The base must belong to this user and be writable by no other,
    /// and every directory it lies in, walked up from it, must pass too.
    pub(in crate::wal::local) fn base(path: &Path) -> io::Result<Self> {
        let dir = Self::walked(path, Making::Missing)?;
        #[cfg(test)]
        if let Some(hook) = &*OPENED.lock() {
            hook(path);
        }
        let metadata = dir.file.metadata()?;
        owned(metadata.uid(), metadata.mode(), BASE, &dir.path)?;
        dir.parents()?;
        Ok(dir)
    }

    /// Checks the base, held open, again: the walk to `path`, the same and as checked, making
    /// nothing, must reach this very directory, still this user's and writable by no other.
    ///
    /// A base gone is refused as [`io::ErrorKind::NotFound`], one another directory took the
    /// place of as not this user's alone.
    pub(in crate::wal::local) fn base_again(&self, path: &Path) -> io::Result<()> {
        let reached = Self::walked(path, Making::Nothing)?;
        let (held, there) = (self.file.metadata()?, reached.file.metadata()?);
        if (held.dev(), held.ino()) != (there.dev(), there.ino()) {
            return Err(Refusal::NotPrivate {
                path: path.to_owned(),
                why: "another directory took the place of the base",
            }
            .into());
        }
        owned(held.uid(), held.mode(), BASE, &self.path)
    }

    /// The directory at `path`, walked to from the root as [`Dir::base`] says, making the
    /// directories missing on the way where `making` says so.
    fn walked(path: &Path, making: Making) -> io::Result<Self> {
        let mut ahead: VecDeque<OsString> = names(&std::path::absolute(path)?);
        let mut dir = Self::at_root()?;
        let mut links = 0;
        while let Some(name) = ahead.pop_front() {
            if name == ".." {
                dir = dir.step(&name, making)?;
                continue;
            }
            dir.passes()?;
            match dir.step(&name, making) {
                Err(error) if error.raw_os_error() == Some(LINKED) => {
                    links += 1;
                    dir.link_trusted(&name)?;
                    let target = dir.link_target(&name, links)?;
                    if target.is_absolute() {
                        dir = Self::at_root()?;
                    }
                    for name in names(&target).into_iter().rev() {
                        ahead.push_front(name);
                    }
                }
                stepped => dir = stepped?,
            }
        }
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

    /// The directory `name` in this one, opened from it without following a link: created
    /// private and durable here where missing and `making` says so; a link, or anything but a
    /// directory, is answered with the error [`LINKED`] for the caller to look at.
    fn step(&self, name: &OsStr, making: Making) -> io::Result<Self> {
        let flags = OFlags::RDONLY | OFlags::DIRECTORY | BENEATH;
        let opened = match rustix::fs::openat(&self.file, name, flags, Mode::empty()) {
            Err(rustix::io::Errno::NOENT) if making == Making::Missing => {
                match rustix::fs::mkdirat(&self.file, name, Mode::RWXU) {
                    Ok(()) => self.sync()?,
                    Err(rustix::io::Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
                rustix::fs::openat(&self.file, name, flags, Mode::empty())
            }
            Err(errno) if super::is_not_a_directory(errno) => {
                return Err(io::Error::from_raw_os_error(LINKED));
            }
            opened => opened,
        }?;
        Ok(Self {
            file: File::from(opened),
            path: self.path.join(name),
        })
    }

    /// Checks that the link `name` here may be followed: in a directory others may write, which
    /// passed only for being sticky, it must be this user's or root's, as the kernel's own rule
    /// for links in such directories has it.
    fn link_trusted(&self, name: &OsStr) -> io::Result<()> {
        let mode = self.file.metadata()?.mode();
        let link = rustix::fs::statat(&self.file, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
        let me = rustix::process::geteuid().as_raw();
        if link_followed(mode, link.st_uid, me) {
            return Ok(());
        }
        Err(self.refused(name, "another user's link in a directory others may write"))
    }

    /// The target of the link `name` here, the `links`th the walk met: anything but a link, and a
    /// walk through more than [`LINKS`] links, is refused.
    fn link_target(&self, name: &OsStr, links: usize) -> io::Result<std::path::PathBuf> {
        if links > LINKS {
            return Err(self.refused(name, "the way to it passes through too many links"));
        }
        let target = rustix::fs::readlinkat(&self.file, name, Vec::new())
            .map_err(|_| self.refused(name, "it is not a directory"))?;
        Ok(OsStr::from_bytes(target.as_bytes()).into())
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
        let mut dir = self.step(OsStr::new(".."), Making::Nothing)?;
        let mut below = self.file.metadata()?;
        loop {
            let metadata = dir.file.metadata()?;
            if (metadata.dev(), metadata.ino()) == (below.dev(), below.ino()) {
                return dir.passes();
            }
            dir.passes()?;
            below = metadata;
            dir = dir.step(OsStr::new(".."), Making::Nothing)?;
        }
    }
}
