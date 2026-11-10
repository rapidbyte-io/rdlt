//! `rdlt connector install`: a connector's wheel as a directory connector, one isolated,
//! operator-owned environment with its launcher.

#[cfg(test)]
mod tests;

use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use rdlt_host::Bubblewrap;

/// What to install, and where.
#[derive(Clone, Debug)]
pub(crate) struct Installation {
    /// The connector's wheel.
    pub(crate) wheel: PathBuf,
    /// The interpreter the environment is made with; `python3` on the path where none is named.
    pub(crate) python: Option<PathBuf>,
    /// A requirements file holding, each with its hash, every package the wheel needs.
    pub(crate) requirements: Option<PathBuf>,
    /// The connector directory the tree is installed in, made where it is not there.
    pub(crate) connectors: PathBuf,
}

/// Why a connector was not installed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InstallError {
    #[error("{} could not be asked what interpreter it is", python.display())]
    InterpreterFailed { python: PathBuf },
    #[error("{} is Python {found}; a connector needs 3.11 or later", python.display())]
    InterpreterTooOld { python: PathBuf, found: String },
    #[error(
        "{} lies outside the system directories a sandboxed connector sees ({}), so the \
         connector could not start there: install it with a system Python, `--python \
         /usr/bin/python3.N` (3.11 or later), or run the connector remotely, serving its \
         launcher with `--listen` and naming that endpoint in the pipeline",
        path.display(),
        Bubblewrap::SYSTEM.join(", ")
    )]
    InterpreterUnreachable { path: PathBuf },
    #[error("{} could not make the connector's environment", python.display())]
    VenvFailed { python: PathBuf },
    #[error("pip could not install {what}")]
    PipFailed { what: &'static str },
    #[error("the wheel names {found} rdlt.connectors entry points; a connector's wheel names one")]
    EntryPoints { found: usize },
    #[error("the entry point {target:?} names no attribute of a module")]
    EntryPointTarget { target: String },
    #[error("the entry point {name:?} names no connector id")]
    ConnectorId {
        name: String,
        #[source]
        source: rdlt_connector::IdError,
    },
    #[error("the launcher's first line would be {length} bytes, more than the kernel reads")]
    ShebangTooLong { length: usize },
    #[error(
        "the launcher's interpreter, {}, holds whitespace, where the kernel ends its first line's \
         program",
        path.display()
    )]
    ShebangWhitespace { path: PathBuf },
    #[error("{context}")]
    Io {
        context: String,
        #[source]
        source: io::Error,
    },
}

impl InstallError {
    /// The stable code of the refusal.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::InterpreterFailed { .. } => "interpreter_failed",
            Self::InterpreterTooOld { .. } => "interpreter_too_old",
            Self::InterpreterUnreachable { .. } => "interpreter_unreachable",
            Self::VenvFailed { .. } => "venv_failed",
            Self::PipFailed { .. } => "pip_failed",
            Self::EntryPoints { .. } | Self::EntryPointTarget { .. } => "entry_points",
            Self::ConnectorId { .. } => "connector_id",
            Self::ShebangTooLong { .. } => "shebang_too_long",
            Self::ShebangWhitespace { .. } => "shebang_whitespace",
            Self::Io { .. } => "io",
        }
    }
}

/// Bytes of a script's first line the kernel reads, its newline among them.
const SHEBANG_BYTES: usize = 256;

/// What an interpreter says it is: its version, and where its executable and standard library
/// lie, every link resolved.
#[derive(serde::Deserialize)]
struct Facts {
    version: (u32, u32, u32),
    executable: PathBuf,
    stdlib: PathBuf,
}

/// The facts query, run by the interpreter asked.
const FACTS: &str = include_str!("install/facts.py");

/// The query of the environment's one connector: each `rdlt.connectors` entry point's name and
/// value.
const ENTRY_POINTS: &str = "import importlib.metadata as m, json\n\
    print(json.dumps([[e.name, e.value] for e in m.entry_points(group='rdlt.connectors')]))";

/// An error of `context`, from `source`.
fn io_error(context: impl Into<String>) -> impl FnOnce(io::Error) -> InstallError {
    let context = context.into();
    move |source| InstallError::Io { context, source }
}

/// Whether `command` ran and succeeded, what it prints for a person going to standard error, so
/// standard output holds the command line's result alone.
fn ran(command: &mut Command) -> bool {
    command
        .stdin(Stdio::null())
        .stdout(io::stderr())
        .status()
        .is_ok_and(|status| status.success())
}

/// What `python` says it is, once it is seen to be one a connector may run: 3.11 or later, and
/// on Linux lying where the sandbox shows the system's directories.
fn facts(python: &Path) -> Result<Facts, InstallError> {
    let failed = || InstallError::InterpreterFailed {
        python: python.to_owned(),
    };
    let output = Command::new(python)
        .args(["-I", "-c", FACTS])
        .stdin(Stdio::null())
        .output()
        .map_err(|_| failed())?;
    let facts: Facts = serde_json::from_slice(&output.stdout).map_err(|_| failed())?;
    if facts.version < (3, 11, 0) {
        let (major, minor, micro) = facts.version;
        return Err(InstallError::InterpreterTooOld {
            python: python.to_owned(),
            found: format!("{major}.{minor}.{micro}"),
        });
    }
    if cfg!(target_os = "linux") {
        for path in [&facts.executable, &facts.stdlib] {
            if !Bubblewrap::SYSTEM
                .iter()
                .any(|system| path.starts_with(system))
            {
                return Err(InstallError::InterpreterUnreachable { path: path.clone() });
            }
        }
    }
    Ok(facts)
}

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

/// The launcher of the connector `target` (`module:attribute`) in the tree at `tree`: run by the
/// tree's interpreter, isolated from the environment's `PYTHON*` variables, user site-packages
/// and the script's directory.
fn launcher(tree: &Path, target: &str) -> Result<Vec<u8>, InstallError> {
    let refused = || InstallError::EntryPointTarget {
        target: target.to_owned(),
    };
    let (module, attribute) = target.split_once(':').ok_or_else(refused)?;
    if !dotted(module) || !dotted(attribute) {
        return Err(refused());
    }
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
    let head = attribute
        .split_once('.')
        .map_or(attribute, |(head, _)| head);
    let body = format!(
        "from rdlt.connector import serve\nfrom {module} import {head}\n\
         raise SystemExit(serve({attribute}))\n"
    );
    text.extend_from_slice(body.as_bytes());
    Ok(text)
}

/// The `rdlt.connectors` entry point of the environment `python` runs, which holds a single one:
/// its name, the connector's id, and its target.
fn entry_point(python: &Path) -> Result<(String, String), InstallError> {
    let output = Command::new(python)
        .args(["-I", "-c", ENTRY_POINTS])
        .stdin(Stdio::null())
        .output()
        .map_err(io_error("listing the environment's entry points"))?;
    let entries: Vec<(String, String)> = serde_json::from_slice(&output.stdout)
        .map_err(|_| InstallError::EntryPoints { found: 0 })?;
    match <[_; 1]>::try_from(entries) {
        Ok([entry]) => Ok(entry),
        Err(entries) => Err(InstallError::EntryPoints {
            found: entries.len(),
        }),
    }
}

/// Installs the wheel, and the requirements where given, into the environment `python` runs.
fn packages(python: &Path, installation: &Installation) -> Result<(), InstallError> {
    let pip = |arguments: &[&OsStr]| {
        let mut command = Command::new(python);
        command
            .args([
                "-I",
                "-m",
                "pip",
                "--no-input",
                "--disable-pip-version-check",
                "install",
            ])
            .args(arguments);
        ran(&mut command)
    };
    let wheel = installation.wheel.as_os_str();
    let Some(requirements) = &installation.requirements else {
        return if pip(&[wheel]) {
            Ok(())
        } else {
            Err(InstallError::PipFailed { what: "the wheel" })
        };
    };
    let hashed = ["--require-hashes", "--no-deps", "-r"].map(OsStr::new);
    if !pip(&[hashed[0], hashed[1], hashed[2], requirements.as_os_str()]) {
        return Err(InstallError::PipFailed {
            what: "the requirements",
        });
    }
    if !pip(&[OsStr::new("--no-deps"), wheel]) {
        return Err(InstallError::PipFailed { what: "the wheel" });
    }
    if !ran(Command::new(python).args(["-I", "-m", "pip", "--disable-pip-version-check", "check"]))
    {
        return Err(InstallError::PipFailed {
            what: "a package the wheel needs and the requirements leave out",
        });
    }
    Ok(())
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

/// Puts the environment `staged` at `tree`, in `connectors`, in place of what is there.
fn placed(staged: &Path, tree: &Path, connectors: &Path) -> Result<(), InstallError> {
    let moving = || io_error(format!("moving the environment to {}", tree.display()));
    if tree.symlink_metadata().is_err() {
        return std::fs::rename(staged, tree).map_err(moving());
    }
    let replaced = tempfile::Builder::new()
        .prefix(".rdlt-replaced-")
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
    replaced.close().map_err(io_error(format!(
        "removing the tree {} held",
        tree.display()
    )))
}

/// Installs `installation`'s wheel as a directory connector; the tree installed,
/// `rdlt-connector-<the id's last segment>` in the connector directory.
///
/// # Errors
///
/// An [`InstallError`] naming the step that failed; nothing is left in the connector directory
/// then but what was there before.
pub(crate) fn install(installation: &Installation) -> Result<PathBuf, InstallError> {
    let python = installation
        .python
        .clone()
        .unwrap_or_else(|| PathBuf::from("python3"));
    let facts = facts(&python)?;
    let connectors = &installation.connectors;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o755)
        .create(connectors)
        .map_err(io_error(format!("making {}", connectors.display())))?;
    let staging = tempfile::Builder::new()
        .prefix(".rdlt-install-")
        .tempdir_in(connectors)
        .map_err(io_error(format!(
            "making a directory in {}",
            connectors.display()
        )))?;
    let environment = staging.path();
    // Made by the interpreter's real executable, so its link leads where that lies.
    let mut made = Command::new(&facts.executable);
    made.args(["-I", "-m", "venv"]).arg(environment);
    if !ran(&mut made) {
        return Err(InstallError::VenvFailed { python });
    }
    let interpreter = environment.join("bin/python");
    packages(&interpreter, installation)?;
    let (name, target) = entry_point(&interpreter)?;
    let id = rdlt::ConnectorId::parse(&name)
        .map_err(|source| InstallError::ConnectorId { name, source })?;
    let last = id.as_str().rsplit('.').next().unwrap_or(id.as_str());
    let tree = connectors.join(format!("rdlt-connector-{last}"));
    let launcher_path = environment.join("bin/rdlt-connector");
    std::fs::write(&launcher_path, launcher(&tree, &target)?)
        .map_err(io_error("writing the launcher"))?;
    std::fs::set_permissions(&launcher_path, std::fs::Permissions::from_mode(0o755))
        .map_err(io_error("making the launcher executable"))?;
    private(environment)?;
    placed(environment, &tree, connectors)?;
    Ok(tree)
}
