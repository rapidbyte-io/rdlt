//! The wheel an install is asked for: an existing file a wheel's name names, copied once, and the
//! copy read from its archive, never imported or run, and named to pip by its `file:` URL.

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;

use super::{InstallError, blocking, io_error};

/// What a wheel says of itself, read from its archive.
#[derive(Debug, serde::Deserialize)]
pub(super) struct Wheel {
    /// Its distribution's name.
    pub(super) name: String,
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
    name: String,
}

impl Held {
    /// The copy, named as the wheel is, since pip reads a wheel's name.
    pub(super) fn path(&self) -> PathBuf {
        self.directory.path().join(&self.name)
    }
}

/// A copy of the wheel at `path`, once it is seen to be an existing regular file that can be read,
/// named as a wheel is: nothing else reaches pip, which would build a file of any other name, or
/// take a name for a package of an index, or a leading `-` for one of its options.
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
    let named = absolute.file_name().and_then(|name| name.to_str());
    let Some(name) = named.filter(|name| file_name(name)) else {
        return Err(refused(
            "its name is no wheel's, {name}-{version}-{python}-{abi}-{platform}.whl",
        ));
    };
    let reading = || io_error(format!("reading {}", path.display()));
    let mut wheel = std::fs::File::open(&absolute).map_err(reading())?;
    // Absolute and with every link resolved, as the copy's URL names it.
    let temporary = std::fs::canonicalize(std::env::temp_dir())
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

/// What names the wheel copied to `held`, the distribution `name`, to pip: its name and `file:` URL.
///
/// It is written once the copy's name is a wheel's and its path absolute, with no `..` in it, so
/// that pip reads it as that URL and no other file. The operator's `named` names it where it is
/// refused.
pub(super) fn reference(name: &str, held: &Path, named: &Path) -> Result<String, InstallError> {
    let refused = |why: &str| InstallError::Wheel {
        path: named.to_owned(),
        why: why.to_owned(),
    };
    // It reaches pip's command line and a requirements file's line: a name of anything else
    // could be an option of pip's or a line of its own there.
    if !distribution(name) {
        let why = format!(
            "its metadata names the distribution {name:?}, which is no distribution's name"
        );
        return Err(refused(&why));
    }
    let file = held.file_name().and_then(|file| file.to_str());
    if !held.is_absolute() || !file.is_some_and(file_name) {
        return Err(refused("its copy is named by no absolute path to a wheel"));
    }
    let mut uri = "file://".to_owned();
    for component in held.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => {
                uri.push('/');
                for byte in part.as_bytes() {
                    if byte.is_ascii_alphanumeric() || b"-._~".contains(byte) {
                        uri.push(char::from(*byte));
                    } else {
                        write!(uri, "%{byte:02X}").expect("a string takes what is written to it");
                    }
                }
            }
            _ => return Err(refused("its copy's path holds a step out of a directory")),
        }
    }
    Ok(format!("{name} @ {uri}"))
}

/// Whether `file` is a wheel's file name as pip reads one:
/// `{name}-{version}(-{build})?-{python}-{abi}-{platform}.whl`, the name and the tags of ASCII
/// letters, digits, `.` and `_`, the name starting with no `.`, the build with a digit, and the
/// version PEP 440's.
pub(super) fn file_name(file: &str) -> bool {
    let Some(stem) = file.strip_suffix(".whl") else {
        return false;
    };
    let tagged = |part: &str| {
        !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._".contains(&byte))
    };
    let parts: Vec<&str> = stem.split('-').collect();
    let (name, version, build, tags) = match parts.as_slice() {
        [name, version, python, abi, platform] => (*name, *version, None, [python, abi, platform]),
        [name, version, build, python, abi, platform] => {
            (*name, *version, Some(*build), [python, abi, platform])
        }
        _ => return false,
    };
    let numbered = build.is_none_or(|build| {
        tagged(build) && build.starts_with(|first: char| first.is_ascii_digit())
    });
    tagged(name)
        && !name.starts_with('.')
        && super::version::version(version)
        && numbered
        && tags.iter().all(|tag| tagged(tag))
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
