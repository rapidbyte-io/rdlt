//! Workdir lock: one process per pipeline workdir.
//!
//! OS advisory locks release automatically on process death — a crashed run never
//! blocks its own recovery (a lock *file* existence check would).

use std::{
    fs::{File, OpenOptions},
    os::unix::fs::OpenOptionsExt as _,
    path::Path,
};

use fs4::fs_std::FileExt;
use rdlt_core::error::Error;

#[derive(Debug)]
pub(crate) struct WorkdirLock {
    _file: File,
}

impl WorkdirLock {
    pub(crate) fn acquire(workdir: &Path) -> Result<Self, Error> {
        // Failures here are about the configured workdir being usable and
        // ownable by this run (path, permissions, contention) — not WAL damage,
        // so they classify as configuration, like the "already held" case below.
        // Created PRIVATE (see `create_private_dir`): the WAL and lock
        // under it carry in-flight data no other local user should read.
        crate::wal::dir::create_private_dir(workdir)
            .map_err(|e| Error::config(format!("creating workdir {}: {e}", workdir.display())))?;
        let path = workdir.join(".lock");
        // The lock file legitimately persists between runs, so this open
        // must accept an existing regular file (`create`, not
        // `create_new`) — which is exactly why it needs O_NOFOLLOW, the
        // WAL manifest's own discipline: a plain create-or-open follows
        // a pre-planted symlink at the final component and would open
        // (and lock) whatever file it points at outside the workdir.
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|e| {
                if e.raw_os_error() == Some(libc::ELOOP) {
                    return Error::config(format!(
                        "opening lock {}: the path is a symlink — refusing to follow it out \
                         of the workdir",
                        path.display()
                    ));
                }
                Error::config(format!("opening lock {}: {e}", path.display()))
            })?;
        if !file
            .try_lock_exclusive()
            .map_err(|e| Error::config(format!("locking {}: {e}", path.display())))?
        {
            return Err(Error::config(format!(
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

    /// A pre-planted symlink at the lock path refuses typed and the
    /// victim it points at is never opened or locked — the WAL
    /// manifest's O_NOFOLLOW discipline, applied to the one workdir
    /// file that legitimately persists (and is therefore opened
    /// create-or-existing) across runs.
    #[test]
    fn a_symlink_at_the_lock_path_refuses_and_never_touches_the_victim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let victim = dir.path().join("victim");
        std::fs::write(&victim, b"innocent bytes").expect("victim file");
        std::os::unix::fs::symlink(&victim, dir.path().join(".lock")).expect("planted symlink");

        let refused = WorkdirLock::acquire(dir.path())
            .expect_err("a symlinked lock path must refuse, not follow");
        assert!(
            refused
                .to_string()
                .contains("the path is a symlink — refusing to follow it out of the workdir"),
            "expected the symlink refusal, got: {refused}"
        );
        assert_eq!(
            std::fs::read(&victim).expect("victim survives"),
            b"innocent bytes"
        );
    }
}
