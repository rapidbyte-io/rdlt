//! The WAL directory: where it lives inside a workdir, the private-mode
//! creation of directories and files, the ownership marker that licenses
//! `clear`, and the gated read-open every seat uses — the WAL's file-type
//! boundary, both sides.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt as _,
    path::{Path, PathBuf},
};

/// The WAL's directory inside a pipeline workdir — the one piece of the
/// on-disk layout, alongside the manifest and segment names below, that the
/// WAL owns rather than its callers.
pub(crate) fn dir_in(workdir: &Path) -> PathBuf {
    workdir.join("wal")
}

/// Create `dir` (and any missing parents) PRIVATE to the operating
/// user (0o700 on every component this call creates): the WAL holds
/// cleartext batch data and cursors, and a default-umask directory in
/// a shared location would let any local user read in-flight rows. An
/// already-existing directory keeps its mode but must be this user's
/// and writable by nobody else — the shared adoption rule, so the whole
/// workdir tree is born private, not just the WAL under it.
pub(crate) fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    rdlt_core::fs::create_or_verify_private_dir(dir)
}

/// Open a WAL-path file for reading, refusing anything that is not a plain
/// regular file — the READ side of the WAL's file-type boundary, shared by
/// every seat that opens a manifest, sidecar, segment or marker.
///
/// Two flags close two different plants at the same boundary, race-free
/// because the verdict comes from the opened HANDLE, never a stat beside it:
///
/// - `O_NONBLOCK`: a plain read-open of a writerless FIFO blocks until a
///   writer appears — which for a planted FIFO is never — so one `mkfifo`
///   at a WAL path would hang recovery forever (nothing up that stack has a
///   timeout). With the flag the open returns immediately and the handle
///   check below refuses the FIFO. On a regular file the flag is a no-op:
///   regular-file reads never report "would block", so the descriptor
///   behaves exactly as an ordinary one and needs no flag-clearing (which
///   would take an `fcntl` this workspace's unsafe ban rules out anyway).
/// - `O_NOFOLLOW`: a symlink at a WAL path reads FOREIGN content into scan
///   verdicts — the write side already refuses symlinks everywhere, and the
///   read side must match or the weaker side decides. The resulting ELOOP
///   is re-worded so refusal logs name the plant, not a loop.
///
/// The handle check catches what the flags do not (a planted socket or
/// device node). Callers classify the error as damage/refusal, never crash.
pub(crate) fn open_wal_read(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "refusing WAL file `{}` because it is a symlink",
                    path.display()
                ),
            ));
        }
        Err(error) => return Err(error),
    };
    if !file.metadata()?.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing WAL file `{}` because it is not a regular file",
                path.display()
            ),
        ));
    }
    Ok(file)
}

/// [`OpenOptions`] for a WAL-owned file, born owner-only (0o600) —
/// the file-level half of the directory hardening above: the mode
/// applies only at creation, so a file that already exists keeps
/// whatever the operator gave it. Every file the WAL creates —
/// manifest, rules sidecar, segments — goes through this, so none can
/// silently fall back to the umask default.
pub(super) fn private_file() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.mode(0o600);
    options
}

/// The rules sidecar beside the manifest: the writer's `IdentRules`,
/// serialized verbatim. Recovery's stream↔segment join normalizes under
/// the resuming run's rules, and that join is only sound when they are
/// the WRITING run's rules — the sidecar is what proves it. A workdir
/// file beside the manifest, not a record in the manifest stream;
/// reclaimed with the directory by [`clear`].
pub(crate) const RULES_SIDECAR: &str = "rules.json";

/// Proof that the `wal` leaf was created or explicitly adopted by rdlt. The
/// marker is intentionally inside the leaf that cleanup removes; an explicit
/// workdir pointing at a non-empty foreign `wal` directory can no longer turn
/// that spelling into authority for `remove_dir_all`.
pub(crate) const OWNERSHIP_MARKER: &str = ".rdlt-wal";
const OWNERSHIP_MARKER_BYTES: &[u8] = b"rdlt-wal-v1\n";

pub(super) fn ensure_owned_dir(dir: &Path) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(dir)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing WAL path `{}` because it is not a real directory",
                dir.display()
            ),
        ));
    }

    let marker = dir.join(OWNERSHIP_MARKER);
    match std::fs::symlink_metadata(&marker) {
        Ok(meta) if meta.file_type().is_file() && !meta.file_type().is_symlink() => {
            // The read goes through the gated open, not `fs::read`: the stat
            // above and the read are two syscalls, and a FIFO swapped into
            // the gap would block a plain open forever. The handle-side
            // re-check closes that window.
            let bytes = read_via_gated_open(&marker)?;
            if bytes == OWNERSHIP_MARKER_BYTES {
                return Ok(());
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "WAL ownership marker `{}` has unrecognized contents",
                    marker.display()
                ),
            ));
        }
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "WAL ownership marker `{}` is not a regular file",
                    marker.display()
                ),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    if std::fs::read_dir(dir)?.next().is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing to adopt non-empty directory `{}` as a WAL without `{OWNERSHIP_MARKER}`",
                dir.display()
            ),
        ));
    }
    private_file()
        .create_new(true)
        .write(true)
        .open(marker)?
        .write_all(OWNERSHIP_MARKER_BYTES)
}

/// Read a small WAL file through [`open_wal_read`]'s gate — the marker
/// reads' replacement for `fs::read`, which opens ungated. BOUNDED at one
/// byte past the expected marker: the type gate refuses FIFOs and symlinks,
/// but a sparse giant REGULAR file planted at the marker path passes it —
/// only this bound keeps the read small, and anything longer than the
/// marker fails the content comparison either way.
fn read_via_gated_open(path: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    let mut bytes = Vec::new();
    open_wal_read(path)?
        .take((OWNERSHIP_MARKER_BYTES.len() + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn verify_owned_dir(dir: &Path) -> std::io::Result<()> {
    let dir_meta = std::fs::symlink_metadata(dir)?;
    if !dir_meta.file_type().is_dir() || dir_meta.file_type().is_symlink() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing to clear non-directory WAL path `{}`",
                dir.display()
            ),
        ));
    }
    let marker = dir.join(OWNERSHIP_MARKER);
    let meta = std::fs::symlink_metadata(&marker)?;
    // Gated open for the same stat-to-read race as `ensure_owned_dir`'s
    // marker read: a FIFO swapped in after the stat must refuse, not block.
    if !meta.file_type().is_file()
        || meta.file_type().is_symlink()
        || read_via_gated_open(&marker)? != OWNERSHIP_MARKER_BYTES
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing to clear `{}` without a valid rdlt WAL ownership marker",
                dir.display()
            ),
        ));
    }
    Ok(())
}

/// Remove the whole WAL directory (clean finish, or fresh start after full
/// recovery). An already-absent directory is a clean result; any other
/// failure surfaces — a silent best-effort remove would let recovery
/// "resolve" a span it never actually cleared and proceed over the residue:
/// RECOVERY propagates it and refuses the run, while the clean-finish caller
/// stays best-effort — failing a fully committed run over cleanup would
/// trade a real success for an error, and the residue resolves as an
/// ordinary committed-manifest Discard on the next run's scan.
pub(crate) fn clear(dir: &Path) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    verify_owned_dir(dir)?;
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            // A partial removal may have taken the ownership marker
            // while leaving residue it could not delete (a
            // write-protected subdirectory, say). Ownership was
            // verified at entry, so the surviving directory is still
            // ours — re-stamp it, or the tolerated-residue arm
            // (Discard's failed clear WARNS and the run proceeds)
            // would be refused adoption of its own WAL directory by
            // `ensure_owned_dir`'s non-empty-without-marker gate.
            if dir.exists() {
                let _ = private_file()
                    .create_new(true)
                    .write(true)
                    .open(dir.join(OWNERSHIP_MARKER))
                    .and_then(|mut f| {
                        use std::io::Write as _;
                        f.write_all(OWNERSHIP_MARKER_BYTES)
                    });
            }
            Err(e)
        }
        _ => Ok(()),
    }
}
