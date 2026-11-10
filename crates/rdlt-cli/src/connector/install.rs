//! `rdlt connector install`: a connector's wheel as a directory connector, one isolated,
//! operator-owned environment with its launcher, made without running any of the connector's
//! code.
//!
//! The wheel's entry point is read from its archive by the base interpreter, with no `site`, so
//! no `.pth` file runs; pip installs the wheel and what it needs in one run, binaries only, so no
//! build backend runs; and the check that nothing is missing runs with no `site` either.

mod environment;
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
    /// Why the tree it replaced is left beside it, where its removal failed.
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
         the TLS flags, the pipeline naming that endpoint",
        path.display(),
        Bubblewrap::SYSTEM.join(", ")
    )]
    InterpreterUnreachable { path: PathBuf, serving: String },
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
            Self::PipFailed { .. } => "pip_failed",
            Self::EntryPoints { .. } | Self::EntryPointTarget { .. } => "entry_points",
            Self::ConnectorId { .. } => "connector_id",
            Self::ShebangTooLong { .. } => "shebang_too_long",
            Self::ShebangWhitespace { .. } => "shebang_whitespace",
            Self::NameTaken { .. } => "name_taken",
            Self::InstallRunning { .. } => "install_running",
            Self::Io { .. } => "io",
        }
    }
}

/// An error of `context`, from `source`.
fn io_error(context: impl Into<String>) -> impl FnOnce(io::Error) -> InstallError {
    let context = context.into();
    move |source| InstallError::Io { context, source }
}

/// Installs `installation`'s wheel as a directory connector, replacing an earlier install of the
/// same connector.
///
/// Dropped before it ends, as it is when a signal ends the command line, it kills the processes
/// it started, every process of their groups, and removes the environment it was making.
///
/// # Errors
///
/// An [`InstallError`] naming the step that failed; the connector directory holds nothing new
/// then, but its lock file.
pub(crate) async fn install(installation: &Installation) -> Result<Installed, InstallError> {
    let wheel = wheel::checked(&installation.wheel)?;
    let python = installation
        .python
        .clone()
        .unwrap_or_else(|| PathBuf::from("python3"));
    let facts = environment::facts(&python).await?;
    let described = wheel::described(&python, &wheel).await?;
    let (name, target) = described.entry_point()?;
    let id = rdlt::ConnectorId::parse(name).map_err(|source| InstallError::ConnectorId {
        name: name.to_owned(),
        source,
    })?;
    let (module, attribute) = tree::target(target)?;
    environment::reachable(&facts, module, attribute)?;
    let connectors = tree::made(&installation.connectors)?;
    let last = id.as_str().rsplit('.').next().unwrap_or(id.as_str());
    let at = connectors.join(format!("rdlt-connector-{last}"));
    let launcher = tree::launcher(&at, module, attribute)?;
    let _locked = tree::locked(&connectors)?;
    tree::swept(&connectors).await;
    tree::replaceable(&at, id.as_str())?;
    let staging = tree::staging(&connectors)?;
    environment::made(&python, staging.path()).await?;
    let requirements = installation.requirements.as_deref();
    environment::installed(staging.path(), &described, requirements).await?;
    tree::finished(staging.path(), launcher, id.as_str()).await?;
    let left = tree::placed(staging.path(), &at, &connectors).await?;
    Ok(Installed { tree: at, left })
}
