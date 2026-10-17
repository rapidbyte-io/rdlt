//! A connector's binary: found only where its operator said, opened once, and from then on
//! known by that open file, which is what is hashed and what is executed.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileExt as _, MetadataExt as _};
use std::path::{Component, Path, PathBuf};

use rustix::fs::{CWD, Mode, OFlags};
use sha2::Digest as _;

use crate::provider::Digest;

#[cfg(test)]
mod tests;

/// Why a file is no binary a connector is spawned from.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Unfit {
    /// Nothing executable is there, or a directory named to search is no absolute path.
    #[error("no executable file is there")]
    Absent(#[source] Option<io::Error>),
    /// Another user wrote the file or may write it, or the directory it was looked up in.
    #[error("{} belongs to user {owner} with mode {mode:o}: another user may change it", path.display())]
    Shared {
        /// The file or directory.
        path: PathBuf,
        /// The user it belongs to.
        owner: u32,
        /// Its permission bits.
        mode: u32,
    },
}

/// A connector's binary, open: no name leads to it again.
#[derive(Debug)]
pub(crate) struct Binary {
    file: File,
    /// Where it was found, for messages and reports; never resolved again.
    path: PathBuf,
}

/// The flags of every open: nothing outlives an exec, no pipe is waited on, no terminal taken.
const OPEN: OFlags = OFlags::RDONLY
    .union(OFlags::CLOEXEC)
    .union(OFlags::NONBLOCK)
    .union(OFlags::NOCTTY);

/// The most links a path is followed through, as many as the kernel follows.
const LINKS: usize = 40;

impl Binary {
    /// Opens the binary at `path`, as its operator wrote the path, links included: every
    /// directory a name on the way is looked up in must be no other user's to change.
    pub(crate) fn at(path: &Path) -> Result<Self, Unfit> {
        let absolute = std::path::absolute(path).map_err(|error| Unfit::Absent(Some(error)))?;
        let fd = rustix::fs::open(&absolute, OPEN, Mode::empty())
            .map_err(|error| Unfit::Absent(Some(error.into())))?;
        let binary = Self::checked(File::from(fd), absolute)?;
        reached_private(&binary.path)?;
        Ok(binary)
    }

    /// Whether a name still leads to the open file: one renamed over, or removed, has none.
    pub(crate) fn linked(&self) -> io::Result<bool> {
        Ok(self.file.metadata()?.nlink() > 0)
    }

    /// Opens the binary `name` in the first of `dirs` that holds it: each an absolute path to
    /// a directory no other user may write, nor any a name on the way to it is looked up in,
    /// searched by that name alone, following no link.
    pub(crate) fn named(dirs: &[PathBuf], name: &str) -> Result<Self, Unfit> {
        for dir in dirs {
            if !dir.is_absolute() {
                let relative = io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("the connector directory {} is not absolute", dir.display()),
                );
                return Err(Unfit::Absent(Some(relative)));
            }
            let listing = OFlags::RDONLY
                .union(OFlags::DIRECTORY)
                .union(OFlags::CLOEXEC);
            let Ok(opened) = rustix::fs::openat(CWD, dir, listing, Mode::empty()) else {
                continue;
            };
            let opened = File::from(opened);
            private(&opened, dir)?;
            reached_private(dir)?;
            // What is absent is looked for in the next directory, and so is a link, which is
            // not followed.
            let unfollowed = OPEN.union(OFlags::NOFOLLOW);
            let named = rustix::fs::openat(&opened, name, unfollowed, Mode::empty());
            if let Ok(fd) = named {
                match Self::checked(File::from(fd), dir.join(name)) {
                    Err(Unfit::Absent(_)) => {}
                    found => return found,
                }
            }
        }
        Err(Unfit::Absent(None))
    }

    /// `file`, found at `path`, once it is seen to be an executable regular file no other
    /// user may write.
    fn checked(file: File, path: PathBuf) -> Result<Self, Unfit> {
        let metadata = file
            .metadata()
            .map_err(|error| Unfit::Absent(Some(error)))?;
        if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
            let error = io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} is not an executable file", path.display()),
            );
            return Err(Unfit::Absent(Some(error)));
        }
        private(&file, &path)?;
        Ok(Self { file, path })
    }

    /// Where the binary was found.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The open file.
    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    /// Whether the file starts as a script does: the kernel then runs its interpreter, which
    /// opens the script by the name it was executed by.
    #[cfg(target_os = "linux")]
    pub(crate) fn is_script(&self) -> io::Result<bool> {
        let mut start = [0; 2];
        let read = self.file.read_at(&mut start, 0)?;
        Ok(read == 2 && start == *b"#!")
    }

    /// The interpreter a script names on its first line, where the file is a script.
    pub(crate) fn interpreter(&self) -> io::Result<Option<PathBuf>> {
        let mut start = [0; 256];
        let read = self.file.read_at(&mut start, 0)?;
        let Some(line) = start[..read].strip_prefix(b"#!") else {
            return Ok(None);
        };
        let line = line.split(|byte| *byte == b'\n').next().unwrap_or_default();
        let named = line
            .split(u8::is_ascii_whitespace)
            .find(|word| !word.is_empty());
        Ok(named.map(|named| PathBuf::from(std::ffi::OsStr::from_bytes(named))))
    }

    /// The SHA-256 digest of the open file's bytes.
    pub(crate) fn digest(&self) -> io::Result<Digest> {
        let mut hasher = sha2::Sha256::new();
        let (mut buffer, mut offset) = (vec![0; 64 * 1024], 0_u64);
        loop {
            let read = self.file.read_at(&mut buffer, offset)?;
            if read == 0 {
                return Ok(Digest(hasher.finalize().into()));
            }
            hasher.update(&buffer[..read]);
            offset = offset.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        }
    }
}

/// Checks every directory a name is looked up in as the absolute `path` is resolved, links
/// followed as the kernel follows them: each the path passes through as it is reached, and each
/// a link on the way is read from.
fn reached_private(path: &Path) -> Result<(), Unfit> {
    let absent = |error: io::Error| Unfit::Absent(Some(error));
    let mut ahead = names(path);
    let (mut at, mut links) = (PathBuf::from("/"), 0);
    while let Some(name) = ahead.pop_front() {
        // `at` holds no link, so its parent is the directory `..` names.
        if name == ".." {
            at.pop();
            continue;
        }
        let entry = at.join(&name);
        entry_private(&at, &entry)?;
        if !std::fs::symlink_metadata(&entry)
            .map_err(absent)?
            .is_symlink()
        {
            at = entry;
            continue;
        }
        links += 1;
        if links > LINKS {
            let looped = rustix::io::Errno::LOOP.raw_os_error();
            return Err(absent(io::Error::from_raw_os_error(looped)));
        }
        let target = std::fs::read_link(&entry).map_err(absent)?;
        if target.is_absolute() {
            at = PathBuf::from("/");
        }
        for name in names(&target).into_iter().rev() {
            ahead.push_front(name);
        }
    }
    Ok(())
}

/// Checks that `directory` belongs to this process's user or to the superuser, and that another
/// user may write it only where it is sticky, as `/tmp` is, and `entry` in it, a link included,
/// is this user's or the superuser's: in a sticky directory another user can neither remove nor
/// rename an entry they do not own.
fn entry_private(directory: &Path, entry: &Path) -> Result<(), Unfit> {
    let ours = rustix::process::geteuid().as_raw();
    let trusted = |owner: u32| owner == 0 || owner == ours;
    let metadata = std::fs::metadata(directory).map_err(|error| Unfit::Absent(Some(error)))?;
    let (owner, mode) = (metadata.uid(), metadata.mode() & 0o7777);
    let held = || std::fs::symlink_metadata(entry).is_ok_and(|held| trusted(held.uid()));
    if trusted(owner) && (mode & 0o022 == 0 || (mode & 0o1000 != 0 && held())) {
        return Ok(());
    }
    Err(Unfit::Shared {
        path: directory.to_owned(),
        owner,
        mode,
    })
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

/// Checks that the open `file`, found at `path`, belongs to this process's user or to the
/// superuser, and that neither its group nor others may write it.
fn private(file: &File, path: &Path) -> Result<(), Unfit> {
    let metadata = file
        .metadata()
        .map_err(|error| Unfit::Absent(Some(error)))?;
    let (owner, mode) = (metadata.uid(), metadata.mode() & 0o7777);
    let trusted = owner == 0 || owner == rustix::process::geteuid().as_raw();
    if trusted && mode & 0o022 == 0 {
        return Ok(());
    }
    Err(Unfit::Shared {
        path: path.to_owned(),
        owner,
        mode,
    })
}
