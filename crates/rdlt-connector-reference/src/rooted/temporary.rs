//! Temporary files: created exclusively under names nobody can guess, and gone on every path
//! that does not publish them.

use std::ffi::{OsStr, OsString};
use std::fmt::Write as _;
use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::time::{Duration, SystemTime};

use rustix::fs::{AtFlags, FileType};

use super::{Dir, Kind, component};

/// What every temporary's name starts with; no published name starts with a dot.
const PREFIX: &str = ".tmp-";

/// A name starting with `prefix` and ending in 128 random bits, which no other process can
/// predict.
pub(crate) fn unique(prefix: &str) -> io::Result<OsString> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(io::Error::other)?;
    let mut name = String::with_capacity(prefix.len() + 2 * random.len());
    name.push_str(prefix);
    for byte in random {
        write!(name, "{byte:02x}").expect("writing to a string never fails");
    }
    Ok(name.into())
}

/// A file being written beside the name it will be published under, removed when dropped
/// unpublished.
#[derive(Debug)]
pub(crate) struct Temporary<'a> {
    dir: &'a Dir,
    name: OsString,
    file: File,
    /// Whether the temporary's name is gone: it was renamed over the name it replaces.
    renamed: bool,
}

impl Dir {
    /// Creates a temporary file in the directory.
    pub(crate) fn temporary(&self) -> io::Result<Temporary<'_>> {
        self.temporary_of("")
    }

    /// Creates a temporary file in the directory for the file `owner`, whose temporaries
    /// [`Dir::sweep_of`] tells from every other file's.
    pub(crate) fn temporary_of(&self, owner: &str) -> io::Result<Temporary<'_>> {
        let name = unique(&prefix(owner))?;
        let file = self.create(&name)?;
        Ok(Temporary {
            dir: self,
            name,
            file,
            renamed: false,
        })
    }

    /// Removes the temporaries in the directory last written at least `age` ago, which the
    /// writers that died writing them left behind.
    pub(crate) fn sweep(&self, age: Duration) -> io::Result<()> {
        self.sweep_of("", age)
    }

    /// Removes the temporaries of the file `owner` in the directory, as [`Dir::sweep`] does.
    ///
    /// A temporary is a regular file of this user's: anything else under a temporary's name is
    /// left where it is, never opened, followed or entered, and an entry that cannot be
    /// inspected or removed is left too, so no entry keeps the directory from being used.
    pub(crate) fn sweep_of(&self, owner: &str, age: Duration) -> io::Result<()> {
        let (now, prefix) = (SystemTime::now(), prefix(owner));
        for (name, kind) in self.entries()? {
            let named = name.as_bytes().starts_with(prefix.as_bytes());
            if named && kind == Kind::File && self.stale(&name, now, age) {
                drop(self.remove_file(&name));
            }
        }
        Ok(())
    }

    /// Whether `name` is a regular file of this user's last written at least `age` before `now`;
    /// one written after `now` is not.
    fn stale(&self, name: &OsStr, now: SystemTime, age: Duration) -> bool {
        let Ok(stat) = rustix::fs::statat(&self.file, name, AtFlags::SYMLINK_NOFOLLOW) else {
            return false;
        };
        let regular = FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile;
        let ours = stat.st_uid == rustix::process::geteuid().as_raw();
        let written = u64::try_from(i128::from(stat.st_mtime))
            .ok()
            .and_then(|seconds| SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(seconds)));
        let old = written
            .and_then(|written| now.duration_since(written).ok())
            .is_some_and(|since| since.checked_sub(age).is_some());
        regular && ours && old
    }
}

/// What the names of the temporaries of the file `owner` start with; of any file's, for no
/// owner.
fn prefix(owner: &str) -> String {
    if owner.is_empty() {
        PREFIX.to_owned()
    } else {
        format!(".{owner}{PREFIX}")
    }
}

impl Temporary<'_> {
    /// Makes the file's bytes durable.
    fn sync(&self) -> io::Result<()> {
        super::sync_file(&self.file, &self.dir.at(&self.name))
    }

    /// The file, to write.
    pub(crate) fn file(&mut self) -> &mut File {
        &mut self.file
    }

    /// Makes the file durable and links it as `name`, unless that name exists; returns whether
    /// it linked, the directory's entry durable when it did.
    pub(crate) fn publish(self, name: impl AsRef<OsStr>) -> io::Result<bool> {
        let name = component(name.as_ref())?;
        self.sync()?;
        if !self.dir.link(&self.name, name)? {
            return Ok(false);
        }
        self.dir.sync()?;
        Ok(true)
    }

    /// Makes the file durable and renames it over `name`, the rename durable too: a crash
    /// leaves what `name` held or what the file holds, never a torn file.
    pub(crate) fn replace(mut self, name: impl AsRef<OsStr>) -> io::Result<()> {
        self.sync()?;
        self.dir.rename(&self.name, self.dir, name)?;
        self.renamed = true;
        self.dir.sync()
    }
}

impl Drop for Temporary<'_> {
    fn drop(&mut self) {
        if !self.renamed {
            drop(self.dir.remove_file(&self.name));
        }
    }
}
