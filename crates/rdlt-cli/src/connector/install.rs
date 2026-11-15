//! `rdlt connector install`: a connector's wheel as a directory connector, one isolated,
//! operator-owned environment with its launcher, made without running any of the connector's
//! code.
//!
//! No interpreter the install starts imports from the tree it makes, and nothing pip installs is
//! built from its source. The wheel is copied once and read from its archive by the base
//! interpreter, with no `site`, and refused where it needs a package by a URL; a requirements file
//! is read in rdlt's own grammar, wheels alone, and written again for pip, each line seen to read
//! for pip as the line checked; pip runs from an environment of its own beside the tree, takes
//! wheels alone from an index, and installs into the tree as its prefix; and the tree is then read
//! as data alone.

mod environment;
mod requirements;
#[cfg(test)]
mod tests;
mod tree;
mod wheel;

use std::io;
use std::path::PathBuf;

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

/// What an install made.
#[derive(Debug)]
pub(crate) struct Installed {
    /// The tree, `rdlt-connector-<the id's last segment>` in the connector directory.
    pub(crate) tree: PathBuf,
    /// Why what it made beside the tree, the tree it replaced among it, is left there, where its
    /// removal failed.
    pub(crate) left: Option<String>,
}

/// Why a connector was not installed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InstallError {
    #[error("{} is no wheel file: {why}", path.display())]
    Wheel { path: PathBuf, why: String },
    #[error("{} failed {asked}", python.display())]
    InterpreterFailed {
        python: PathBuf,
        asked: &'static str,
    },
    #[error("{} is Python {found}; a connector needs 3.11 or later", python.display())]
    InterpreterTooOld { python: PathBuf, found: String },
    #[error(
        "{} lies outside the system directories a sandbox shows ({}), so a host that sandboxes \
         its connectors could not start one installed with it: install it with a system Python, \
         `--python /usr/bin/python3.N` (3.11 or later), or serve it without installing it, from \
         an environment holding its wheel, with `python -I -c '{serving}' --listen ADDRESS` and \
         the TLS flags, the pipeline naming that endpoint, which runs the connector's code with \
         your access, outside any sandbox",
        path.display(),
        Bubblewrap::SYSTEM.join(", ")
    )]
    InterpreterUnreachable { path: PathBuf, serving: String },
    #[error(
        "replacing {} needs a file system that exchanges two names in one step, which this one \
         does not: remove {} and install again",
        tree.display(),
        tree.display()
    )]
    Unexchangeable {
        tree: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{} could not make the connector's environment", python.display())]
    VenvFailed { python: PathBuf },
    #[error("{} line {line} is refused: {why}", path.display())]
    Requirements {
        path: PathBuf,
        line: usize,
        why: requirements::Refused,
    },
    #[error("pip could not install the wheel and the packages it needs")]
    PipFailed,
    #[error(
        "the wheel or a package it needs replaces {}, which is the environment's own",
        path.display()
    )]
    ReplacesEnvironment { path: PathBuf },
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
    #[error(
        "{} holds what is no install of {id}: it is left as it is, so install elsewhere or remove \
         it",
        path.display()
    )]
    NameTaken { path: PathBuf, id: String },
    #[error("another install is installing into {}", connectors.display())]
    InstallRunning { connectors: PathBuf },
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
            Self::Wheel { .. } => "wheel",
            Self::InterpreterFailed { .. } => "interpreter_failed",
            Self::InterpreterTooOld { .. } => "interpreter_too_old",
            Self::InterpreterUnreachable { .. } => "interpreter_unreachable",
            Self::VenvFailed { .. } => "venv_failed",
            Self::Requirements { .. } => "requirements",
            Self::PipFailed => "pip_failed",
            Self::ReplacesEnvironment { .. } => "replaces_environment",
            Self::EntryPoints { .. } | Self::EntryPointTarget { .. } => "entry_points",
            Self::ConnectorId { .. } => "connector_id",
            Self::ShebangTooLong { .. } => "shebang_too_long",
            Self::ShebangWhitespace { .. } => "shebang_whitespace",
            Self::NameTaken { .. } => "name_taken",
            Self::InstallRunning { .. } => "install_running",
            Self::Io { .. } | Self::Unexchangeable { .. } => "io",
        }
    }
}

/// An error of `context`, from `source`.
fn io_error(context: impl Into<String>) -> impl FnOnce(io::Error) -> InstallError {
    let context = context.into();
    move |source| InstallError::Io { context, source }
}

/// What `work`, which touches the file system and may block, answers, done on a thread that may
/// block; a panic there is resumed here.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work)
        .await
        .unwrap_or_else(|joined| std::panic::resume_unwind(joined.into_panic()))
}

/// Installs `installation`'s wheel as a directory connector, replacing an earlier install of the
/// same connector.
///
/// Only a copy of an existing wheel file reaches pip, named by its `file:` URL, which no index is
/// asked for; with requirements, read in rdlt's grammar, `--require-hashes` covers every package,
/// the wheel among them.
///
/// Dropped before it ends, as it is when a signal ends the command line, it kills the processes
/// it started, every process of their groups, and removes what it was making; once its tree is
/// being put in place, the putting ends first.
///
/// # Errors
///
/// An [`InstallError`] naming the step that failed; the connector directory holds nothing new
/// then, but its lock file.
pub(crate) async fn install(installation: &Installation) -> Result<Installed, InstallError> {
    let held = wheel::held(&installation.wheel).await?;
    let requirements = match &installation.requirements {
        Some(path) => Some(requirements::read(path).await?),
        None => None,
    };
    let named = installation
        .python
        .clone()
        .unwrap_or_else(|| PathBuf::from("python3"));
    // A path is taken from here: the install's processes run in a directory of their own.
    let python = if named.components().count() > 1 {
        std::path::absolute(&named).map_err(io_error(format!("resolving {}", named.display())))?
    } else {
        named
    };
    let facts = environment::facts(&python).await?;
    let described = wheel::described(&python, &held.path(), &installation.wheel).await?;
    let requirements = match requirements {
        Some(mut lines) => {
            lines.push(requirements::wheel_line(&described, &installation.wheel)?);
            Some(lines)
        }
        None => None,
    };
    let (name, target) = described.entry_point()?;
    let id = rdlt::ConnectorId::parse(name).map_err(|source| InstallError::ConnectorId {
        name: name.to_owned(),
        source,
    })?;
    let (module, attribute) = tree::target(target)?;
    environment::reachable(&facts, module, attribute)?;
    let connectors = tree::made(&installation.connectors).await?;
    let last = id.as_str().rsplit('.').next().unwrap_or(id.as_str());
    let at = connectors.join(format!("rdlt-connector-{last}"));
    let launcher = tree::launcher(&at, module, attribute)?;
    let locked = tree::locked(&connectors).await?;
    tree::swept(&connectors).await;
    tree::replaceable(&at, id.as_str()).await?;
    let staging = tree::staging(&connectors, locked, &at).await?;
    let made = async {
        environment::made(&python, &staging).await?;
        environment::installed(&staging, &described, requirements.as_deref()).await?;
        tree::finished(&staging.tree(), launcher, id.as_str()).await
    }
    .await;
    // The wheel's copy, and where the install failed all it made, are removed where that may
    // block; dropped with the install, they are removed at once.
    blocking(move || drop(held)).await;
    if let Err(error) = made {
        blocking(move || drop(staging)).await;
        return Err(error);
    }
    let left = tree::placed(staging, &at).await?;
    Ok(Installed { tree: at, left })
}
