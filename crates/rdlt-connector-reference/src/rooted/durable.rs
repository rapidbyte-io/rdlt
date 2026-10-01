//! The steps a directory handle takes that change what the disk holds: each made durable, or
//! undone, through one place a test can stop and record.

use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::Path;

use rustix::fs::AtFlags;

#[cfg(test)]
use super::trace;
use super::{Dir, PRIVATE_DIR, component};

impl Dir {
    /// Makes the directory `name`, private to its owner; whether it was missing.
    pub(super) fn made(&self, name: &OsStr) -> io::Result<bool> {
        #[cfg(test)]
        let step = trace::Step::MakeDir(self.at(name));
        match rustix::fs::mkdirat(&self.file, name, PRIVATE_DIR) {
            Ok(()) => {}
            Err(rustix::io::Errno::EXIST) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        // A test stops here, the directory made and not yet durable in this one.
        #[cfg(test)]
        trace::attempt(&step)?;
        #[cfg(test)]
        trace::done(step);
        Ok(true)
    }

    /// Removes the entry `name` that is no directory: a link itself, never what it leads to.
    pub(crate) fn remove_file(&self, name: impl AsRef<OsStr>) -> io::Result<()> {
        let name = component(name.as_ref())?;
        durable!(
            trace::Step::Remove(self.at(name)),
            rustix::fs::unlinkat(&self.file, name, AtFlags::empty())
        );
        Ok(())
    }

    /// Removes the empty directory `name`.
    pub(crate) fn remove_dir(&self, name: impl AsRef<OsStr>) -> io::Result<()> {
        let name = component(name.as_ref())?;
        durable!(
            trace::Step::RemoveDir(self.at(name)),
            rustix::fs::unlinkat(&self.file, name, AtFlags::REMOVEDIR)
        );
        Ok(())
    }

    /// Moves `name` to `to` in `into`, replacing what is there.
    pub(crate) fn rename(
        &self,
        name: impl AsRef<OsStr>,
        into: &Self,
        to: impl AsRef<OsStr>,
    ) -> io::Result<()> {
        let (name, to) = (component(name.as_ref())?, component(to.as_ref())?);
        durable!(
            trace::Step::Rename(into.at(to)),
            rustix::fs::renameat(&self.file, name, &into.file, to)
        );
        Ok(())
    }

    /// Makes the directory's entries durable, so a file created, linked or renamed in it
    /// survives a crash.
    pub(crate) fn sync(&self) -> io::Result<()> {
        durable!(trace::Step::SyncDir(self.path.clone()), synced(&self.file));
        Ok(())
    }

    /// Links the file `name` of this directory as `to` in it, unless that name exists; whether
    /// it linked.
    pub(super) fn link(&self, name: &OsStr, to: &OsStr) -> io::Result<bool> {
        #[cfg(test)]
        let step = trace::Step::Link(self.at(to));
        #[cfg(test)]
        trace::attempt(&step)?;
        match rustix::fs::linkat(&self.file, name, &self.file, to, AtFlags::empty()) {
            Ok(()) => {}
            Err(rustix::io::Errno::EXIST) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        #[cfg(test)]
        trace::done(step);
        Ok(true)
    }
}

/// Makes `file`, a file's bytes or a directory's entries, durable.
fn synced(file: &File) -> io::Result<()> {
    // A test that takes thousands of steps records its syncs without making them.
    #[cfg(test)]
    if trace::unsynced() {
        return Ok(());
    }
    file.sync_all()
}

/// Makes the bytes of `file`, which `path` names in a test's record, durable.
pub(crate) fn sync_file(file: &File, path: &Path) -> io::Result<()> {
    #[cfg(not(test))]
    let _ = path;
    durable!(trace::Step::SyncFile(path.to_owned()), synced(file));
    Ok(())
}
