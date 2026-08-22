//! The filesystem discipline every seat that touches an operator-named
//! path shares: a document a pipeline file points at, a lock in a
//! workdir, a probe a diagnostic drops, an output a run writes, a
//! staging file a destination publishes. Stated once because every one
//! of those paths is chosen by a document or a directory the process
//! does not own outright, and the same three plants answer to every
//! one of them.
//!
//! - A **FIFO** at the path parks a plain open until a writer appears —
//!   for a planted one, never. Opens here carry `O_NONBLOCK`, which on
//!   a regular file is a no-op (regular reads never report "would
//!   block") and on a FIFO returns at once so the handle check can
//!   refuse it.
//! - A **symlink** at the path redirects the open — and a truncating
//!   or appending open then damages whatever it points at. Opens that
//!   write carry `O_NOFOLLOW`; opens that read a document the operator
//!   named follow it on purpose (a config behind `/etc` is routinely a
//!   link) and judge what they reached.
//! - A **device, directory or socket** at the path reads without end,
//!   or fails every read. Every verdict comes from the opened HANDLE's
//!   metadata, never a `stat` beside it, so a swap between the check
//!   and the use has nothing to race.
//!
//! Files this module creates are born owner-only (`0o600`); directories
//! it verifies must be owner-only too. That is the process's own
//! authority boundary: a directory another user can write is one
//! whose entries that user chooses.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::Path;

/// Whether an open may follow a symlink at the final component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Symlinks {
    /// Follow it and judge what it reaches — for documents the operator
    /// named, where a link is an ordinary way to name a file.
    Follow,
    /// Refuse it — for every path the process writes, creates, or
    /// locks, where a link is a redirect.
    Refuse,
}

/// Open `path` for reading as a regular file: non-blocking, the
/// handle's kind judged before a byte is read.
pub fn open_regular(path: &Path, symlinks: Symlinks) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    options.custom_flags(nonblocking_flags(symlinks));
    regular(
        options
            .open(path)
            .map_err(|error| reword_loop(error, path))?,
        path,
    )
}

/// Open-or-create `path` for writing as a regular file, never following
/// a symlink, never blocking, born `0o600`: the lock file's shape, and
/// any other file that legitimately persists between runs.
pub fn open_or_create_private(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    options.mode(0o600);
    options.custom_flags(nonblocking_flags(Symlinks::Refuse));
    regular(
        options
            .open(path)
            .map_err(|error| reword_loop(error, path))?,
        path,
    )
}

/// Create `path` exclusively — it must not exist, in any form — as an
/// owner-only regular file. The shape of every file whose name the
/// process chose and whose existence would mean someone else got there
/// first: a staging file, a probe.
pub fn create_private(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true).read(true);
    options.mode(0o600);
    options.custom_flags(nonblocking_flags(Symlinks::Refuse));
    regular(
        options
            .open(path)
            .map_err(|error| reword_loop(error, path))?,
        path,
    )
}

/// Create-or-truncate `path` as an owner-only regular file, never
/// following a symlink: an output the operator asked for by name (a
/// report, an event log) — replaced if it is already a regular file,
/// refused if it is anything else. An existing file keeps its own
/// mode; the process cannot know why the operator set it.
pub fn replace_private(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    options.mode(0o600);
    options.custom_flags(nonblocking_flags(Symlinks::Refuse));
    regular(
        options
            .open(path)
            .map_err(|error| reword_loop(error, path))?,
        path,
    )
}

/// Open `path` for appending as a regular file, never following a
/// symlink, born `0o600` if it did not exist: a journal's shape.
pub fn append_private(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true).read(true);
    options.mode(0o600);
    options.custom_flags(nonblocking_flags(Symlinks::Refuse));
    regular(
        options
            .open(path)
            .map_err(|error| reword_loop(error, path))?,
        path,
    )
}

/// Read a document the operator named, at most `cap` bytes: the file
/// must be regular (its kind judged on the handle, a FIFO refused at
/// once), and a file past the cap refuses naming the cap — the read
/// stops one byte past it, so the refusal says "at least". Follows a
/// symlink, since naming a document through one is ordinary.
pub fn read_document(path: &Path, cap: u64) -> io::Result<Vec<u8>> {
    use std::io::Read as _;
    let file = open_regular(path, Symlinks::Follow)?;
    let mut bytes = Vec::new();
    file.take(cap + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > cap {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "the file is at least {} bytes, over the {cap}-byte cap",
                bytes.len()
            ),
        ));
    }
    Ok(bytes)
}

/// Judge an existing directory the process is about to treat as its
/// own: a real directory, not a symlink to one; owned by the effective
/// user; writable by nobody else. A directory failing any of those is
/// one whose entries someone else may choose — a lock, a manifest, a
/// staging file planted there is theirs, not this process's — so it is
/// refused before any entry under it is opened. Readability is not
/// judged here: what the process writes under it is born owner-only,
/// so a listable directory leaks names, not data.
pub fn verify_private_dir(dir: &Path) -> io::Result<()> {
    let meta = std::fs::symlink_metadata(dir)?;
    if meta.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{}: a symlink, not a directory of this process's own",
                dir.display()
            ),
        ));
    }
    if !meta.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            format!("{}: not a directory", dir.display()),
        ));
    }
    let euid = effective_uid();
    if meta.uid() != euid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{}: owned by uid {}, not this process's uid {euid} — refusing to treat \
                 another user's directory as private",
                dir.display(),
                meta.uid()
            ),
        ));
    }
    if meta.mode() & 0o022 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{}: mode {:04o} lets other users write it — whoever can write a \
                 directory chooses what its lock and journal are; chmod it (0700 is \
                 what this process creates), or choose one that is",
                dir.display(),
                meta.mode() & 0o7777
            ),
        ));
    }
    Ok(())
}

/// Create `dir` (and any missing parent) owner-only, or verify an
/// existing one is — the one way a process adopts a directory as its
/// own. A parent that already existed keeps its mode: the process
/// asked for the leaf.
pub fn create_or_verify_private_dir(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    verify_private_dir(dir)
}

/// This process's effective user id — what a file it creates is owned
/// by, and what a directory must be owned by to be its own. Read from
/// the process's own metadata rather than through a libc call: the
/// workspace keeps to safe code.
fn effective_uid() -> u32 {
    // `/proc/self` is owned by the effective uid on Linux; elsewhere
    // the metadata of a file this process just created is the same
    // answer, and the temp dir is always writable.
    std::fs::metadata("/proc/self")
        .map(|meta| meta.uid())
        .or_else(|_| {
            let probe = tempfile_probe()?;
            probe.metadata().map(|meta| meta.uid())
        })
        .unwrap_or(u32::MAX)
}

fn tempfile_probe() -> io::Result<File> {
    let path = std::env::temp_dir().join(format!(
        ".rdlt-uid-probe-{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let file = create_private(&path)?;
    let _ = std::fs::remove_file(&path);
    Ok(file)
}

fn nonblocking_flags(symlinks: Symlinks) -> i32 {
    match symlinks {
        Symlinks::Follow => libc::O_NONBLOCK,
        Symlinks::Refuse => libc::O_NONBLOCK | libc::O_NOFOLLOW,
    }
}

/// The handle's kind, judged after the open: what the flags could not
/// refuse (a device, a directory, a socket) refuses here.
fn regular(file: File, path: &Path) -> io::Result<File> {
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{}: not a regular file", path.display()),
        ));
    }
    Ok(file)
}

/// `O_NOFOLLOW` reports a symlink as `ELOOP`; the refusal names the
/// plant rather than a loop.
fn reword_loop(error: io::Error, path: &Path) -> io::Error {
    if error.raw_os_error() == Some(libc::ELOOP) {
        return io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{}: a symlink — refusing to follow it to wherever it points",
                path.display()
            ),
        );
    }
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn fifo(dir: &Path) -> std::path::PathBuf {
        let path = dir.join("pipe");
        let status = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .expect("mkfifo runs");
        assert!(status.success());
        path
    }

    /// A FIFO refuses at once rather than parking the opener; a symlink
    /// refuses on every writing open and is followed on a document
    /// read; a device refuses on its handle.
    #[test]
    fn the_three_plants_refuse_on_every_open() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pipe = fifo(dir.path());
        assert!(open_regular(&pipe, Symlinks::Follow).is_err());
        assert!(open_or_create_private(&pipe).is_err());
        assert!(append_private(&pipe).is_err());
        assert!(replace_private(&pipe).is_err(), "a FIFO is not replaced");

        let victim = dir.path().join("victim");
        std::fs::write(&victim, b"precious").expect("victim");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&victim, &link).expect("link");
        for (name, result) in [
            ("replace", replace_private(&link).map(drop)),
            ("append", append_private(&link).map(drop)),
            ("open_or_create", open_or_create_private(&link).map(drop)),
            ("create", create_private(&link).map(drop)),
        ] {
            assert!(result.is_err(), "{name} follows nothing");
        }
        assert_eq!(std::fs::read(&victim).expect("victim"), b"precious");
        assert!(
            open_regular(&link, Symlinks::Follow).is_ok(),
            "a document read follows the link to the file"
        );
        assert!(open_regular(&link, Symlinks::Refuse).is_err());

        assert!(open_regular(Path::new("/dev/zero"), Symlinks::Follow).is_err());
        assert!(read_document(Path::new("/dev/zero"), 16).is_err());
    }

    /// Files born here are owner-only whatever the umask; an existing
    /// output keeps its mode; exclusive creation refuses what exists.
    #[test]
    fn created_files_are_private_and_creation_is_exclusive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out");
        drop(replace_private(&path).expect("born"));
        assert_eq!(
            std::fs::metadata(&path).expect("meta").mode() & 0o777,
            0o600
        );
        assert!(create_private(&path).is_err(), "it exists");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        drop(replace_private(&path).expect("replaced"));
        assert_eq!(
            std::fs::metadata(&path).expect("meta").mode() & 0o777,
            0o644,
            "an existing file's mode is the operator's"
        );
    }

    /// The bounded document read: at the cap it reads, one over refuses
    /// naming the cap, and a sparse file past it costs no more than the
    /// cap to refuse.
    #[test]
    fn a_document_read_is_capped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("doc");
        std::fs::write(&path, b"x".repeat(16)).expect("doc");
        assert_eq!(read_document(&path, 16).expect("at the cap").len(), 16);
        let refusal = read_document(&path, 15).expect_err("over");
        assert!(
            refusal.to_string().contains("over the 15-byte cap"),
            "{refusal}"
        );
        let sparse = dir.path().join("sparse");
        File::create(&sparse)
            .expect("create")
            .set_len(1 << 40)
            .expect("sparse");
        assert!(read_document(&sparse, 16).is_err());
    }

    /// A directory is the process's own only when it is a real
    /// directory, owned by it, and writable by nobody else.
    #[test]
    fn a_private_dir_is_owned_and_unshared() {
        let dir = tempfile::tempdir().expect("tempdir");
        let own = dir.path().join("own");
        create_or_verify_private_dir(&own).expect("born private");
        assert_eq!(std::fs::metadata(&own).expect("meta").mode() & 0o777, 0o700);

        std::fs::set_permissions(&own, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        verify_private_dir(&own).expect("listable is not writable: adopted");
        std::fs::set_permissions(&own, std::fs::Permissions::from_mode(0o770)).expect("chmod");
        let refusal = verify_private_dir(&own).expect_err("group-writable");
        assert!(refusal.to_string().contains("0770"), "{refusal}");
        std::fs::set_permissions(&own, std::fs::Permissions::from_mode(0o702)).expect("chmod");
        assert!(verify_private_dir(&own).is_err(), "other-writable");
        std::fs::set_permissions(&own, std::fs::Permissions::from_mode(0o700)).expect("chmod");
        verify_private_dir(&own).expect("private again");

        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&own, &link).expect("link");
        assert!(
            verify_private_dir(&link).is_err(),
            "a symlink is not adopted"
        );
        let file = dir.path().join("file");
        std::fs::write(&file, b"").expect("file");
        assert!(verify_private_dir(&file).is_err());
    }
}
