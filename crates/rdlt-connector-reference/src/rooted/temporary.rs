//! Temporary files: created exclusively under names nobody can guess, and gone on every path
//! that does not publish them.

use std::ffi::{OsStr, OsString};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, ErrorKind};
use std::os::unix::ffi::OsStrExt as _;
use std::time::{Duration, SystemTime};

use rustix::fs::AtFlags;

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
    pub(crate) fn sweep_of(&self, owner: &str, age: Duration) -> io::Result<()> {
        let (now, prefix) = (SystemTime::now(), prefix(owner));
        for (name, kind) in self.entries()? {
            if !name.as_bytes().starts_with(prefix.as_bytes()) {
                continue;
            }
            let stale = match kind {
                Kind::File => match self
                    .file(&name)
                    .and_then(|file| file.metadata()?.modified())
                {
                    // Written at least `age` ago; one written in the future is not stale.
                    Ok(written) => now
                        .duration_since(written)
                        .is_ok_and(|since| since.checked_sub(age).is_some()),
                    Err(error) if error.kind() == ErrorKind::NotFound => false,
                    Err(error) => return Err(error),
                },
                // Nothing this connector makes: no writer is waited for.
                Kind::Dir | Kind::Other => true,
            };
            if stale {
                self.remove_tree(&name)?;
            }
        }
        Ok(())
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
    /// The file, to write.
    pub(crate) fn file(&mut self) -> &mut File {
        &mut self.file
    }

    /// Makes the file durable and links it as `name`, unless that name exists; returns whether
    /// it linked, the directory's entry durable when it did.
    pub(crate) fn publish(self, name: impl AsRef<OsStr>) -> io::Result<bool> {
        let name = component(name.as_ref())?;
        self.file.sync_all()?;
        let (from, into) = (&self.dir.file, &self.dir.file);
        match rustix::fs::linkat(from, self.name.as_os_str(), into, name, AtFlags::empty()) {
            Ok(()) => {}
            Err(rustix::io::Errno::EXIST) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        self.dir.sync()?;
        Ok(true)
    }

    /// Makes the file durable and renames it over `name`, the rename durable too: a crash
    /// leaves what `name` held or what the file holds, never a torn file.
    pub(crate) fn replace(mut self, name: impl AsRef<OsStr>) -> io::Result<()> {
        self.file.sync_all()?;
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
