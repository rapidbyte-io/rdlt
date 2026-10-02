//! Secrets in files: private files beneath the directories the operator lists, reached one
//! name at a time, following no link.

use std::fs::File;
use std::io::Read as _;
use std::path::{Component, Path, PathBuf};

use rdlt_connector::{BoxFuture, Secret};
use rustix::fs::{CWD, Mode, OFlags};

use super::{SecretFault, SecretKind, SecretReference, SecretResolver, admit};
use crate::limits::SECRET_BYTES;

/// Resolves `${file:/absolute/path}` to the text of a file beneath one of the directories its
/// operator lists, each this user's alone to write: a regular file, reached from the directory
/// one name at a time through no link, that neither its group nor others may read or write,
/// of [`SECRET_BYTES`] at most; one line end at its end is not the secret's.
#[derive(Clone, Debug)]
pub struct FileSecrets {
    directories: Vec<PathBuf>,
}

impl FileSecrets {
    /// Reads private files beneath `directories`, each an absolute path, and refuses every
    /// other file.
    pub fn within(directories: impl IntoIterator<Item = impl Into<PathBuf>>) -> Self {
        Self {
            directories: directories.into_iter().map(Into::into).collect(),
        }
    }

    /// The directory listed that holds `path`, and the names beneath it that lead there.
    fn beneath<'a>(
        &'a self,
        path: &'a Path,
    ) -> Result<(&'a Path, Vec<&'a std::ffi::OsStr>), SecretFault> {
        if !path.is_absolute() {
            return Err(SecretFault::Relative);
        }
        let plain = path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)));
        let listed = self
            .directories
            .iter()
            .filter(|directory| directory.is_absolute());
        for directory in listed {
            let Ok(rest) = path.strip_prefix(directory) else {
                continue;
            };
            let names: Vec<_> = rest.components().map(Component::as_os_str).collect();
            if plain && !names.is_empty() {
                return Ok((directory, names));
            }
        }
        Err(SecretFault::Refused)
    }
}

/// `opened`, refused unless it is this user's alone, its group and others neither reading
/// nor writing it when `read` says it is read too.
fn private(opened: &File, read: bool) -> Result<(), SecretFault> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = opened.metadata().map_err(SecretFault::Io)?;
    let (owner, mode) = (metadata.uid(), metadata.mode() & 0o7777);
    let others = if read { 0o077 } else { 0o022 };
    if owner != rustix::process::geteuid().as_raw() || mode & others != 0 {
        return Err(SecretFault::Shared { owner, mode });
    }
    Ok(())
}

/// What a refused open means for a secret.
fn opening(error: rustix::io::Errno) -> SecretFault {
    match error {
        rustix::io::Errno::LOOP | rustix::io::Errno::MLINK | rustix::io::Errno::NOTDIR => {
            SecretFault::NotRegular
        }
        rustix::io::Errno::NOENT => SecretFault::Missing,
        error => SecretFault::Io(error.into()),
    }
}

/// The text of the private file `names` lead to beneath `directory`.
fn read_private(
    directory: &Path,
    names: &[&std::ffi::OsStr],
) -> Result<Secret<String>, SecretFault> {
    let listing = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
    let mut at =
        File::from(rustix::fs::openat(CWD, directory, listing, Mode::empty()).map_err(opening)?);
    private(&at, false)?;
    let Some((last, through)) = names.split_last() else {
        return Err(SecretFault::Refused);
    };
    for name in through {
        let opened = rustix::fs::openat(&at, *name, listing.union(OFlags::NOFOLLOW), Mode::empty());
        at = File::from(opened.map_err(opening)?);
    }
    // No link followed at the name, no waiting on a pipe, nothing kept across an exec.
    let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let opened = rustix::fs::openat(&at, *last, flags | OFlags::NOCTTY, Mode::empty());
    let file = File::from(opened.map_err(opening)?);
    let metadata = file.metadata().map_err(SecretFault::Io)?;
    if !metadata.is_file() {
        return Err(SecretFault::NotRegular);
    }
    private(&file, true)?;
    if metadata.len() > SECRET_BYTES.saturating_add(2) {
        return Err(SecretFault::TooLong {
            limit: SECRET_BYTES,
        });
    }
    let room = usize::try_from(metadata.len())
        .unwrap_or(0)
        .saturating_add(1);
    let mut bytes = zeroize::Zeroizing::new(Vec::with_capacity(room));
    file.take(SECRET_BYTES.saturating_add(3))
        .read_to_end(&mut bytes)
        .map_err(SecretFault::Io)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| SecretFault::NotText)?;
    let text = text.strip_suffix('\n').unwrap_or(text);
    let text = text.strip_suffix('\r').unwrap_or(text);
    admit(text.len())?;
    Ok(Secret::new(text.to_owned()))
}

impl SecretResolver for FileSecrets {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            if reference.kind != SecretKind::File {
                return Err(SecretFault::Refused);
            }
            let path = Path::new(&reference.name);
            let (directory, names) = self.beneath(path)?;
            let directory = directory.to_owned();
            let names: Vec<std::ffi::OsString> = names.into_iter().map(ToOwned::to_owned).collect();
            let reading = tokio::task::spawn_blocking(move || {
                let names: Vec<&std::ffi::OsStr> = names.iter().map(AsRef::as_ref).collect();
                read_private(&directory, &names)
            });
            reading
                .await
                .map_err(|error| SecretFault::Io(std::io::Error::other(error)))?
        })
    }

    fn directories(&self) -> Vec<PathBuf> {
        self.directories.clone()
    }
}
