//! Workdir lock: one process per pipeline workdir (embedder-api.md R5).
//!
//! OS advisory locks release automatically on process death — a crashed run never
//! blocks its own recovery (a lock *file* existence check would).

use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use fs4::fs_std::FileExt;
use rdlt_core::RdltError;

#[derive(Debug)]
pub(crate) struct WorkdirLock {
    _file: File,
}

impl WorkdirLock {
    pub(crate) fn acquire(workdir: &Path) -> Result<Self, RdltError> {
        // Failures here are about the configured workdir being usable and
        // ownable by this run (path, permissions, contention) — not WAL damage,
        // so they classify as configuration, like the "already held" case below.
        std::fs::create_dir_all(workdir).map_err(|e| {
            RdltError::config(format!("creating workdir {}: {e}", workdir.display()))
        })?;
        let path = workdir.join(".lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|e| RdltError::config(format!("opening lock {}: {e}", path.display())))?;
        if !file
            .try_lock_exclusive()
            .map_err(|e| RdltError::config(format!("locking {}: {e}", path.display())))?
        {
            return Err(RdltError::config(format!(
                "another run holds the workdir lock at {} — one process per pipeline",
                path.display()
            )));
        }
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_lock_in_same_process_fails_and_releases_on_drop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = WorkdirLock::acquire(dir.path()).expect("first lock");
        // fs4 advisory locks are per-file-handle; a second handle must be refused.
        assert!(
            WorkdirLock::acquire(dir.path()).is_err(),
            "second lock must fail"
        );
        drop(first);
        WorkdirLock::acquire(dir.path()).expect("lock reacquirable after drop");
    }
}
