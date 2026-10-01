//! Lock files: regular files of their user's alone, there to be locked and never read.

use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, ErrorKind};

use super::{Dir, private};

impl Dir {
    /// Opens the lock file `name`, creating it where missing: a regular file of this user's
    /// alone, never a link and never another user's.
    pub(crate) fn lock_file(&self, name: impl AsRef<OsStr>) -> io::Result<File> {
        let name = name.as_ref();
        let mut lost = None;
        // A file created between the open that missed it and the create that lost to it is
        // opened.
        for _ in 0..2 {
            match self.file(name) {
                Ok(file) => {
                    private(&file)?;
                    return Ok(file);
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            match self.create(name) {
                Ok(file) => {
                    self.sync()?;
                    return Ok(file);
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => lost = Some(error),
                Err(error) => return Err(error),
            }
        }
        Err(lost.unwrap_or_else(|| ErrorKind::AlreadyExists.into()))
    }
}
