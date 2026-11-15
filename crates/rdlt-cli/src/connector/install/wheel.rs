//! The wheel an install is asked for: an existing wheel file, copied once, and the copy read from
//! its archive, never imported or run.

use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;

use super::{InstallError, blocking, io_error};

/// What a wheel says of itself, read from its archive.
#[derive(Debug, serde::Deserialize)]
pub(super) struct Wheel {
    /// Its distribution's name.
    pub(super) name: String,
    /// Its `file:` URL.
    pub(super) uri: String,
    /// The SHA-256 digest of its bytes, in hex.
    pub(super) sha256: String,
    /// Each `rdlt.connectors` entry point it names: its name, a connector's id, and its target.
    pub(super) entry_points: Vec<(String, String)>,
    /// Each requirement its metadata names, as written.
    pub(super) requires: Vec<String>,
}

/// What the reading answers: the wheel, or why it is none.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Read {
    Wheel(Wheel),
    Refused { refused: String },
}

/// The script that reads a wheel, run by the base interpreter with `-I -S`: its standard library
/// alone, no `.pth` file of any environment.
const READ: &str = include_str!("wheel.py");

impl Wheel {
    /// Its one `rdlt.connectors` entry point: a connector's id, and its target.
    pub(super) fn entry_point(&self) -> Result<(&str, &str), InstallError> {
        match self.entry_points.as_slice() {
            [(name, target)] => Ok((name, target)),
            found => Err(InstallError::EntryPoints { found: found.len() }),
        }
    }
}

/// A copy of the wheel an install is asked for, in a directory of its own that is removed when it
/// drops: the operator's file is read once, and the copy is what is described, hashed and
/// installed.
pub(super) struct Held {
    directory: tempfile::TempDir,
    name: OsString,
}

impl Held {
    /// The copy, named as the wheel is, since pip reads a wheel's name.
    pub(super) fn path(&self) -> PathBuf {
        self.directory.path().join(&self.name)
    }
}

/// A copy of the wheel at `path`, once it is seen to be an existing regular file named `*.whl` that
/// can be read: nothing else reaches pip, which would take a name for a package of an index, or a
/// leading `-` for one of its options.
pub(super) async fn held(path: &Path) -> Result<Held, InstallError> {
    let path = path.to_owned();
    blocking(move || held_now(&path)).await
}

/// [`held`], on a thread that may block.
fn held_now(path: &Path) -> Result<Held, InstallError> {
    let refused = |why: &str| InstallError::Wheel {
        path: path.to_owned(),
        why: why.to_owned(),
    };
    let absolute =
        std::path::absolute(path).map_err(io_error(format!("resolving {}", path.display())))?;
    let metadata = match std::fs::metadata(&absolute) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(refused("nothing is there"));
        }
        Err(source) => return Err(io_error(format!("reading {}", path.display()))(source)),
    };
    if !metadata.is_file() {
        return Err(refused("it is not a regular file"));
    }
    let whl = absolute.extension() == Some(OsStr::new("whl"));
    let Some(name) = absolute.file_name().filter(|_| whl) else {
        return Err(refused("its name does not end in .whl"));
    };
    let reading = || io_error(format!("reading {}", path.display()));
    let mut wheel = std::fs::File::open(&absolute).map_err(reading())?;
    // Absolute, as a file's URL is.
    let temporary = std::path::absolute(std::env::temp_dir())
        .map_err(io_error("resolving the directory for temporary files"))?;
    let directory = tempfile::Builder::new()
        .prefix("rdlt-install-")
        .tempdir_in(temporary)
        .map_err(io_error("making a directory for the wheel's copy"))?;
    let held = Held {
        directory,
        name: name.to_owned(),
    };
    let mut copy =
        std::fs::File::create_new(held.path()).map_err(io_error("making the wheel's copy"))?;
    io::copy(&mut wheel, &mut copy).map_err(reading())?;
    Ok(held)
}

/// What the wheel at `path`, a copy of the operator's `named`, says of itself, read by `python`,
/// once it is seen to need nothing by a URL: pip would build such a package from its source, as
/// no index's are, and a connector's dependencies come from an index.
pub(super) async fn described(
    python: &Path,
    path: &Path,
    named: &Path,
) -> Result<Wheel, InstallError> {
    let failed = || InstallError::InterpreterFailed {
        python: python.to_owned(),
        asked: "reading the wheel",
    };
    let output = Command::new(python)
        .args(["-I", "-S", "-c", READ])
        .arg(path)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|_| failed())?;
    if !output.status.success() {
        return Err(failed());
    }
    match serde_json::from_slice(&output.stdout).map_err(|_| failed())? {
        // It reaches pip's command line and a requirements file's line: a name of anything else
        // could be an option of pip's or a line of its own there.
        Read::Wheel(wheel) if !distribution(&wheel.name) => Err(InstallError::Wheel {
            path: named.to_owned(),
            why: format!(
                "its metadata names the distribution {:?}, which is no distribution's name",
                wheel.name
            ),
        }),
        Read::Wheel(wheel) if wheel.requires.iter().any(|required| direct(required)) => {
            Err(InstallError::Wheel {
                path: named.to_owned(),
                why: "its metadata names a package it needs by a URL, which pip would build from \
                      its source: what a connector needs comes from an index"
                    .to_owned(),
            })
        }
        Read::Wheel(wheel) => Ok(wheel),
        Read::Refused { refused } => Err(InstallError::Wheel {
            path: named.to_owned(),
            why: refused,
        }),
    }
}

/// Whether `required`, a requirement as PEP 508 writes one, names its package by a URL: an `@`
/// before its markers.
fn direct(required: &str) -> bool {
    required
        .split(';')
        .next()
        .is_some_and(|named| named.contains('@'))
}

/// Whether `name` is a distribution's name as PEP 508 has it: ASCII letters and digits, and `.`,
/// `_` and `-` between them.
pub(super) fn distribution(name: &str) -> bool {
    let bytes = name.as_bytes();
    let inner = |byte: &u8| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-');
    match (bytes.first(), bytes.last()) {
        (Some(first), Some(last)) => {
            first.is_ascii_alphanumeric() && last.is_ascii_alphanumeric() && bytes.iter().all(inner)
        }
        _ => false,
    }
}
