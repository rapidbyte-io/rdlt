//! The tree an install makes in the connector directory: its launcher, the id it records, its
//! modes, and its place, where it replaces only an earlier install of its connector, in one step.

#[cfg(test)]
mod tests;

use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use rustix::fs::{FlockOperation, RenameFlags};
use rustix::io::Errno;

use super::{InstallError, blocking, io_error};

/// Bytes of a script's first line the kernel reads, its newline among them.
const SHEBANG_BYTES: usize = 256;

/// The file in a tree that names the connector installed there.
const ID_FILE: &str = "rdlt-connector.id";

/// The beginning of the name of what an install makes beside its trees while it runs.
const STAGED: &str = ".rdlt-install-";

/// The file an install holds locked in the connector directory while it runs.
const LOCK_FILE: &str = ".rdlt-install.lock";

/// Whether `name` is a dotted Python name, each part an ASCII identifier.
fn dotted(name: &str) -> bool {
    name.split('.').all(|part| {
        let mut chars = part.chars();
        chars
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && chars.all(|rest| rest.is_ascii_alphanumeric() || rest == '_')
    })
}

/// The module and the attribute of the entry point `target`, `module:attribute`.
pub(super) fn target(target: &str) -> Result<(&str, &str), InstallError> {
    let refused = || InstallError::EntryPointTarget {
        target: target.to_owned(),
    };
    let (module, attribute) = target.split_once(':').ok_or_else(refused)?;
    if dotted(module) && dotted(attribute) {
        Ok((module, attribute))
    } else {
        Err(refused())
    }
}

/// The launcher of `module`'s `attribute` in the tree at `tree`: run by the tree's interpreter,
/// isolated from the environment's `PYTHON*` variables, user site-packages and the script's
/// directory.
pub(super) fn launcher(
    tree: &Path,
    module: &str,
    attribute: &str,
) -> Result<Vec<u8>, InstallError> {
    let interpreter = tree.join("bin/python");
    if interpreter
        .as_os_str()
        .as_bytes()
        .iter()
        .any(u8::is_ascii_whitespace)
    {
        return Err(InstallError::ShebangWhitespace { path: interpreter });
    }
    let mut text = b"#!".to_vec();
    text.extend_from_slice(interpreter.as_os_str().as_bytes());
    text.extend_from_slice(b" -I\n");
    if text.len() > SHEBANG_BYTES {
        return Err(InstallError::ShebangTooLong {
            length: text.len() - 1,
        });
    }
    text.extend_from_slice(serving(module, attribute).as_bytes());
    Ok(text)
}

/// The Python that serves `module`'s `attribute`: the connector class it names, or each class of
/// the tuple it names, a source and a destination one process serves together, and nothing else,
/// so an installed connector serves no certification probe.
pub(super) fn serving(module: &str, attribute: &str) -> String {
    let head = attribute
        .split_once('.')
        .map_or(attribute, |(head, _)| head);
    format!(
        "from rdlt.connector._serve import installed\nfrom {module} import {head}\n\
         raise SystemExit(installed({attribute}))\n"
    )
}

/// The connector directory `connectors`, made where it is not there, where it lies: the launcher
/// names its interpreter by this path, which the host binds a tree at.
pub(super) async fn made(connectors: &Path) -> Result<PathBuf, InstallError> {
    let connectors = connectors.to_owned();
    blocking(move || made_now(&connectors)).await
}

/// [`made`], on a thread that may block.
fn made_now(connectors: &Path) -> Result<PathBuf, InstallError> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(connectors)
        .map_err(io_error(format!("making {}", connectors.display())))?;
    std::fs::canonicalize(connectors)
        .map_err(io_error(format!("resolving {}", connectors.display())))
}

/// The lock of `connectors`, held while the returned file is open: one install at a time makes
/// and sweeps what lies beside its trees.
pub(super) async fn locked(connectors: &Path) -> Result<File, InstallError> {
    let connectors = connectors.to_owned();
    blocking(move || locked_now(&connectors)).await
}

/// [`locked`], on a thread that may block.
fn locked_now(connectors: &Path) -> Result<File, InstallError> {
    let path = connectors.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(&path)
        .map_err(io_error(format!("opening {}", path.display())))?;
    match rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(file),
        Err(Errno::WOULDBLOCK) => Err(InstallError::InstallRunning {
            connectors: connectors.to_owned(),
        }),
        Err(error) => Err(io_error(format!("locking {}", path.display()))(
            error.into(),
        )),
    }
}

/// Removes what an install that was killed left in `connectors`; what cannot be removed stays
/// for the next.
pub(super) async fn swept(connectors: &Path) {
    let Ok(mut entries) = tokio::fs::read_dir(connectors).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        if !name.as_bytes().starts_with(STAGED.as_bytes()) {
            continue;
        }
        let directory = entry.file_type().await.is_ok_and(|kind| kind.is_dir());
        if directory {
            tokio::fs::remove_dir_all(entry.path()).await.ok();
        } else {
            tokio::fs::remove_file(entry.path()).await.ok();
        }
    }
}

/// Refuses `tree` where it holds anything but an earlier install of the connector `id`.
pub(super) async fn replaceable(tree: &Path, id: &str) -> Result<(), InstallError> {
    let (tree, id) = (tree.to_owned(), id.to_owned());
    blocking(move || replaceable_now(&tree, &id)).await
}

/// [`replaceable`], on a thread that may block.
fn replaceable_now(tree: &Path, id: &str) -> Result<(), InstallError> {
    let metadata = match tree.symlink_metadata() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        held => held.map_err(io_error(format!("reading {}", tree.display())))?,
    };
    let recorded = tree.join(ID_FILE);
    let installed = metadata.is_dir()
        && recorded.symlink_metadata().is_ok_and(|held| held.is_file())
        && std::fs::read_to_string(&recorded).is_ok_and(|held| held.trim_end() == id);
    if installed {
        Ok(())
    } else {
        Err(InstallError::NameTaken {
            path: tree.to_owned(),
            id: id.to_owned(),
        })
    }
}

/// What an install makes beside the trees of a connector directory while it runs, in a directory
/// of its own that is removed when it drops: the tree, the environment pip runs from, and the
/// requirements file pip reads.
pub(super) struct Staging {
    directory: tempfile::TempDir,
    /// The connector directory's lock, held until what was staged is removed.
    _locked: File,
}

impl Staging {
    /// The directory itself, where the install's processes run.
    pub(super) fn path(&self) -> &Path {
        self.directory.path()
    }

    /// The tree being made.
    pub(super) fn tree(&self) -> PathBuf {
        self.path().join("tree")
    }

    /// The environment pip runs from.
    pub(super) fn installer(&self) -> PathBuf {
        self.path().join("installer")
    }

    /// The requirements file pip reads.
    pub(super) fn requirements(&self) -> PathBuf {
        self.path().join("requirements.txt")
    }
}

/// A [`Staging`] beside the trees of `connectors`, holding their lock, `locked`, until it is
/// removed: once the tree at `tree` is there to replace, it is seen that the file system can
/// exchange two names in one step, before anything is made.
pub(super) async fn staging(
    connectors: &Path,
    locked: File,
    tree: &Path,
) -> Result<Staging, InstallError> {
    let (connectors, tree) = (connectors.to_owned(), tree.to_owned());
    blocking(move || {
        let directory = tempfile::Builder::new()
            .prefix(STAGED)
            .tempdir_in(&connectors)
            .map_err(io_error(format!(
                "making a directory in {}",
                connectors.display()
            )))?;
        exchangeable(directory.path(), &tree)?;
        Ok(Staging {
            directory,
            _locked: locked,
        })
    })
    .await
}

/// Refuses, where the tree at `tree` is there to replace, a file system that cannot exchange two
/// names of `directory` in one step.
fn exchangeable(directory: &Path, tree: &Path) -> Result<(), InstallError> {
    if tree.symlink_metadata().is_err() {
        return Ok(());
    }
    let (one, other) = (directory.join("one"), directory.join("other"));
    for made in [&one, &other] {
        std::fs::create_dir(made).map_err(io_error(format!(
            "making a directory in {}",
            directory.display()
        )))?;
    }
    exchanged(&one, &other, tree)?;
    for made in [&one, &other] {
        std::fs::remove_dir(made).map_err(io_error(format!("removing {}", made.display())))?;
    }
    Ok(())
}

/// Exchanges the names `one` and `other`, as replacing the tree at `tree` does: a file system that
/// cannot is told from any other failure.
fn exchanged(one: &Path, other: &Path, tree: &Path) -> Result<(), InstallError> {
    let cwd = rustix::fs::CWD;
    rustix::fs::renameat_with(cwd, one, cwd, other, RenameFlags::EXCHANGE).map_err(|error| {
        let unable = [Errno::INVAL, Errno::OPNOTSUPP, Errno::NOTSUP, Errno::NOSYS];
        if unable.contains(&error) {
            InstallError::Unexchangeable {
                tree: tree.to_owned(),
                source: error.into(),
            }
        } else {
            let context = format!("exchanging {} with {}", one.display(), other.display());
            io_error(context)(error.into())
        }
    })
}

/// Writes `launcher` and the id of its connector into the environment at `environment`, and
/// takes write away from group and others throughout it.
pub(super) async fn finished(
    environment: &Path,
    launcher: Vec<u8>,
    id: &str,
) -> Result<(), InstallError> {
    let environment = environment.to_owned();
    let id = format!("{id}\n");
    blocking(move || {
        let path = environment.join("bin/rdlt-connector");
        std::fs::write(&path, launcher).map_err(io_error("writing the launcher"))?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .map_err(io_error("making the launcher executable"))?;
        std::fs::write(environment.join(ID_FILE), id)
            .map_err(io_error("writing the connector's id"))?;
        private(&environment)
    })
    .await
}

/// Takes write away from group and others throughout `tree`, links aside: a sandboxed
/// connector's tree is its operator's alone.
fn private(tree: &Path) -> Result<(), InstallError> {
    for entry in walkdir::WalkDir::new(tree).follow_links(false) {
        let entry = entry.map_err(|error| InstallError::Io {
            context: format!("reading {}", tree.display()),
            source: error.into(),
        })?;
        if entry.path_is_symlink() {
            continue;
        }
        let path = entry.path();
        let mode = entry
            .metadata()
            .map_err(|error| InstallError::Io {
                context: format!("reading {}", path.display()),
                source: error.into(),
            })?
            .permissions()
            .mode();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & !0o022))
            .map_err(io_error(format!("setting the mode of {}", path.display())))?;
    }
    Ok(())
}

/// Puts the tree `staging` made at `tree`, in place of what is there, and removes the rest of
/// `staging`, the tree replaced among it; why that is left, where it could not be removed.
pub(super) async fn placed(staging: Staging, tree: &Path) -> Result<Option<String>, InstallError> {
    let tree = tree.to_owned();
    blocking(move || replaced(staging, &tree)).await
}

/// [`placed`], on a thread that may block, which alone removes `staging`: an install dropped while
/// its tree is put in place removes nothing of it.
fn replaced(staging: Staging, tree: &Path) -> Result<Option<String>, InstallError> {
    let staged = staging.tree();
    if tree.symlink_metadata().is_err() {
        std::fs::rename(&staged, tree).map_err(io_error(format!(
            "moving the environment to {}",
            tree.display()
        )))?;
        return Ok(left(staging, tree));
    }
    // The two change places at once, so one of them is in place throughout.
    exchanged(&staged, tree, tree)?;
    Ok(left(staging, tree))
}

/// Removes `staging`, all but the tree now at `tree`, and then lets go of the lock it holds; why
/// it is left, where it could not be removed.
fn left(staging: Staging, tree: &Path) -> Option<String> {
    let kept = staging.path().to_owned();
    staging.directory.close().err().map(|error| {
        format!(
            "what the install made beside {} is left at {}, which the next install removes: \
             {error}",
            tree.display(),
            kept.display()
        )
    })
}
