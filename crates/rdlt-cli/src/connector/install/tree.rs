//! The tree an install makes in the connector directory: its launcher, the id it records, its
//! modes, and its place, where it replaces only an earlier install of its connector.

#[cfg(test)]
mod tests;

use std::fs::File;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use rustix::fs::FlockOperation;

use super::{InstallError, blocking, io_error};

/// Bytes of a script's first line the kernel reads, its newline among them.
const SHEBANG_BYTES: usize = 256;

/// The file in a tree that names the connector installed there.
const ID_FILE: &str = "rdlt-connector.id";

/// The beginnings of the names of what an install makes beside its tree while it runs.
const STAGED: [&str; 2] = [".rdlt-install-", ".rdlt-replaced-"];

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
/// the tuple it names, a source and a destination one process serves together.
pub(super) fn serving(module: &str, attribute: &str) -> String {
    let head = attribute
        .split_once('.')
        .map_or(attribute, |(head, _)| head);
    format!(
        "from rdlt.connector import serve\nfrom {module} import {head}\n\
         raise SystemExit(serve(*{attribute}) if isinstance({attribute}, tuple) else \
         serve({attribute}))\n"
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
        Err(rustix::io::Errno::WOULDBLOCK) => Err(InstallError::InstallRunning {
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
        let stale = STAGED
            .iter()
            .any(|staged| name.as_bytes().starts_with(staged.as_bytes()));
        if !stale {
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

/// A directory of the install's own beside the trees of `connectors`, removed when it drops.
pub(super) async fn staging(connectors: &Path) -> Result<tempfile::TempDir, InstallError> {
    let connectors = connectors.to_owned();
    blocking(move || {
        tempfile::Builder::new()
            .prefix(STAGED[0])
            .tempdir_in(&connectors)
            .map_err(io_error(format!(
                "making a directory in {}",
                connectors.display()
            )))
    })
    .await
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

/// Puts the environment `staged` at `tree`, in `connectors`, in place of what is there; why the
/// tree it replaced is left, where it could not be removed.
pub(super) async fn placed(
    staged: &Path,
    tree: &Path,
    connectors: &Path,
) -> Result<Option<String>, InstallError> {
    let (staged, tree, connectors) = (staged.to_owned(), tree.to_owned(), connectors.to_owned());
    blocking(move || replaced(&staged, &tree, &connectors)).await
}

/// [`placed`], on a thread that may block.
fn replaced(staged: &Path, tree: &Path, connectors: &Path) -> Result<Option<String>, InstallError> {
    let moving = || io_error(format!("moving the environment to {}", tree.display()));
    if tree.symlink_metadata().is_err() {
        std::fs::rename(staged, tree).map_err(moving())?;
        return Ok(None);
    }
    let replaced = tempfile::Builder::new()
        .prefix(STAGED[1])
        .tempdir_in(connectors)
        .map_err(io_error(format!(
            "making a directory in {}",
            connectors.display()
        )))?;
    let old = replaced.path().join("tree");
    std::fs::rename(tree, &old).map_err(io_error(format!("moving {} aside", tree.display())))?;
    if let Err(error) = std::fs::rename(staged, tree) {
        // What was installed stays installed.
        std::fs::rename(&old, tree).ok();
        return Err(moving()(error));
    }
    let kept = replaced.path().to_owned();
    // The new tree is in place: what is left of the old is swept by the next install.
    Ok(replaced.close().err().map(|error| {
        format!(
            "the tree replaced is left at {}, which the next install removes: {error}",
            kept.display()
        )
    }))
}
