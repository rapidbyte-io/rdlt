//! Workdir lock: one process per pipeline workdir.
//!
//! OS advisory locks release automatically on process death — a crashed run never
//! blocks its own recovery (a lock *file* existence check would).

use std::{
    fs::{File, TryLockError},
    path::Path,
};

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
        // Born PRIVATE, or verified so when it already exists: the
        // WAL and lock under it carry in-flight data no other local
        // user should read, and a directory another user can write is
        // one whose lock, manifest and segments that user plants — a
        // pre-existing workdir is adopted only when it is this user's
        // and nobody else's, BEFORE any entry under it is opened.
        rdlt_core::fs::create_or_verify_private_dir(workdir)
            .map_err(|e| Error::config(format!("adopting workdir {}: {e}", workdir.display())))?;
        let path = workdir.join(".lock");
        // The lock file legitimately persists between runs, so this
        // open must accept an existing regular file — which is exactly
        // why it refuses a symlink (a plain create-or-open would lock
        // whatever a planted link points at outside the workdir) and
        // opens non-blocking with its kind judged on the handle (a
        // planted FIFO would otherwise park the run here, before the
        // lock it came to take).
        let file = rdlt_core::fs::open_or_create_private(&path)
            .map_err(|e| Error::config(format!("opening lock {}: {e}", path.display())))?;
        file.try_lock().map_err(|e| match e {
            TryLockError::WouldBlock => Error::config(format!(
                "another run holds the workdir lock at {} — one process per pipeline",
                path.display()
            )),
            TryLockError::Error(e) => Error::config(format!("locking {}: {e}", path.display())),
        })?;
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
        // OS advisory locks are per-file-handle; a second handle must be
        // refused, and refused AS CONTENTION — an I/O failure on the lock
        // file reaches the operator through a different sentence.
        let second = WorkdirLock::acquire(dir.path()).expect_err("second lock must fail");
        assert!(
            second
                .to_string()
                .contains("another run holds the workdir lock"),
            "contention must name itself, said: {second}"
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
                .contains("a symlink — refusing to follow it"),
            "expected the symlink refusal, got: {refused}"
        );
        assert_eq!(
            std::fs::read(&victim).expect("victim survives"),
            b"innocent bytes"
        );
    }

    /// A FIFO planted at the lock path refuses at once: the open is
    /// non-blocking and the handle's kind is judged, so the run is
    /// never parked before the lock it came to take.
    #[test]
    fn a_fifo_at_the_lock_path_refuses_without_blocking() {
        let dir = tempfile::tempdir().expect("tempdir");
        let status = std::process::Command::new("mkfifo")
            .arg(dir.path().join(".lock"))
            .status()
            .expect("mkfifo runs");
        assert!(status.success());
        let refused = WorkdirLock::acquire(dir.path()).expect_err("a FIFO is not a lock file");
        assert!(
            refused.to_string().contains("not a regular file"),
            "{refused}"
        );
    }

    /// A pre-existing workdir is adopted only when it is this user's
    /// and nobody else's to write: one other users can write is refused
    /// before the lock under it is opened, naming the mode and the
    /// remedy.
    #[test]
    fn a_shared_existing_workdir_is_refused_before_its_lock_is_opened() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path().join("wd");
        std::fs::create_dir(&workdir).expect("mkdir");
        std::fs::set_permissions(&workdir, std::fs::Permissions::from_mode(0o775)).expect("chmod");
        let refused = WorkdirLock::acquire(&workdir).expect_err("a shared workdir is refused");
        assert!(
            refused.to_string().contains("0775") && refused.to_string().contains("chmod"),
            "{refused}"
        );
        assert!(
            !workdir.join(".lock").exists(),
            "nothing was opened under the refused directory"
        );
        std::fs::set_permissions(&workdir, std::fs::Permissions::from_mode(0o700)).expect("chmod");
        WorkdirLock::acquire(&workdir).expect("private, so adopted");
    }
}
