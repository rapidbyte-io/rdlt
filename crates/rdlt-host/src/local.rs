//! Process placement: a connector found by its path, or by name on the connector directories and
//! `PATH`, is spawned with its socket on file descriptor 3, and respawned when it is lost.

pub(crate) mod process;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    BoxFuture, ConnectorError, ConnectorId, ConnectorSpec, Destination, Role, Source,
};
use sha2::Digest as _;

use crate::kills::Kills;
use crate::supervise::{Gate, Spawned, Start, SupervisedDestination, SupervisedSource, Supervisor};
pub use process::{LastWords, Witness};
use process::{Launch, Process, executable};

use crate::provider::{ConnectorRef, Digest, Placed, Placement, Provider, ProviderError};
use crate::remote::Options;
use crate::wire::Wire;

/// Places connectors in processes of their own.
#[derive(Clone, Debug)]
pub struct Local {
    dirs: Vec<PathBuf>,
    grace: Duration,
    env_passthrough: Vec<String>,
    options: Options,
    kills: Option<Kills>,
}

impl Default for Local {
    fn default() -> Self {
        Self {
            dirs: Vec::new(),
            grace: Duration::from_secs(10),
            env_passthrough: Vec::new(),
            options: Options::default(),
            kills: None,
        }
    }
}

impl Local {
    /// Finds connectors on `PATH`; stops them with a grace period of 10 s.
    pub fn new() -> Self {
        Self::default()
    }

    /// Looks for connectors in `dir` before `PATH`.
    #[must_use]
    pub fn connector_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dirs.push(dir.into());
        self
    }

    /// Gives a stopped connector `grace` to exit before it is killed.
    #[must_use]
    pub fn grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// Keeps the environment variable `name` in each connector's environment, which otherwise
    /// starts empty.
    #[must_use]
    pub fn env_passthrough(mut self, name: impl Into<String>) -> Self {
        self.env_passthrough.push(name.into());
        self
    }

    /// Spawns each connector so that `kills` kills it: with `SIGKILL`, and no grace.
    #[must_use]
    pub fn kills(mut self, kills: &Kills) -> Self {
        self.kills = Some(kills.clone());
        self
    }

    /// Runs each connection with `options`.
    #[must_use]
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// The binary `reference` names, as an absolute path: its path, or
    /// `rdlt-connector-<the id's last segment>` in the connector directories, then on `PATH`.
    pub fn resolve(&self, reference: &ConnectorRef) -> Result<PathBuf, ProviderError> {
        let not_found = |source| ProviderError::NotFound {
            id: reference.id.clone(),
            source,
        };
        // Absolute: spawning a bare name would search `PATH`, and a respawn could run elsewhere.
        let absolute =
            |path: &Path| std::path::absolute(path).map_err(|error| not_found(Some(error)));
        if let Some(path) = &reference.path {
            return if executable(path) {
                absolute(path)
            } else {
                let error = std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("{} is not an executable file", path.display()),
                );
                Err(not_found(Some(error)))
            };
        }
        let name = binary_name(&reference.id);
        let path = std::env::var_os("PATH").unwrap_or_default();
        self.dirs
            .iter()
            .cloned()
            .chain(std::env::split_paths(&path))
            .map(|dir| dir.join(&name))
            .find(|candidate| executable(candidate))
            .ok_or_else(|| not_found(None))
            .and_then(|found| absolute(&found))
    }

    /// A raw connection to the connector `reference` names, before its handshake: its binary,
    /// spawned with the connection's other end on file descriptor 3, stops once the wire is
    /// dropped.
    ///
    /// Call it within a tokio runtime, which drains and reaps the process.
    ///
    /// # Errors
    ///
    /// [`ProviderError::NotFound`] when the binary cannot be found, and
    /// [`ProviderError::SpawnFailed`] when it cannot be spawned.
    pub fn wire(&self, reference: &ConnectorRef) -> Result<Wire, ProviderError> {
        let path = self.resolve(reference)?;
        let launch = self.launch(reference, &path, None);
        let (stream, process) =
            Process::launched(&launch).map_err(|source| spawn_failed(reference, &path, source))?;
        Ok(Wire::new(Box::new(stream), Some(process)))
    }

    fn launch(&self, reference: &ConnectorRef, path: &Path, digest: Option<Digest>) -> Launch {
        Launch {
            id: reference.id.clone(),
            path: path.to_owned(),
            digest,
            env_passthrough: self.env_passthrough.clone(),
            grace: self.grace,
            kills: self.kills.clone(),
        }
    }

    /// Spawns the connector `reference` names as `role`, and checks it is that connector.
    async fn start(
        &self,
        reference: &ConnectorRef,
        role: Role,
        config: &serde_json::Value,
    ) -> Result<(Supervisor, ConnectorSpec, PathBuf, Digest), ProviderError> {
        let path = self.resolve(reference)?;
        let digest = digest(&path)
            .await
            .map_err(|source| spawn_failed(reference, &path, source))?;
        if let Some(expected) = reference.digest.filter(|expected| *expected != digest) {
            return Err(ProviderError::DigestMismatch {
                id: reference.id.clone(),
                path,
                expected,
                found: digest,
            });
        }
        let launch = self.launch(reference, &path, Some(digest));
        let found_at = path.display().to_string();
        let gate = Gate {
            reference,
            found_at: &found_at,
        };
        let supervisor = Supervisor::start(
            Start::Spawn(launch),
            role,
            config.clone(),
            self.options,
            &gate,
        )
        .await
        .map_err(|spawned| match spawned {
            Spawned::Io(source) | Spawned::Unreachable(source) | Spawned::Tls(source) => {
                spawn_failed(reference, &path, source)
            }
            Spawned::Connect(source) => handshake_failed(reference, source),
            Spawned::Refused(refused) => refused,
        })?;
        let spec = supervisor.spec();
        Ok((supervisor, spec, path, digest))
    }
}

fn spawn_failed(reference: &ConnectorRef, path: &Path, source: std::io::Error) -> ProviderError {
    ProviderError::SpawnFailed {
        id: reference.id.clone(),
        path: path.to_owned(),
        source,
    }
}

fn handshake_failed(reference: &ConnectorRef, source: ConnectorError) -> ProviderError {
    ProviderError::HandshakeFailed {
        id: reference.id.clone(),
        source: Box::new(source),
    }
}

/// `rdlt-connector-` and the last segment of `id`.
fn binary_name(id: &ConnectorId) -> String {
    let last = id.as_str().rsplit('.').next().unwrap_or(id.as_str());
    format!("rdlt-connector-{last}")
}

/// The SHA-256 digest of the file at `path`.
pub(crate) async fn digest(path: &Path) -> std::io::Result<Digest> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(path)?;
        let mut hasher = sha2::Sha256::new();
        std::io::copy(&mut file, &mut hasher)?;
        Ok(Digest(hasher.finalize().into()))
    })
    .await
    .map_err(std::io::Error::other)?
}

impl Provider for Local {
    fn source<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Source>>, ProviderError>> {
        Box::pin(async move {
            let (supervisor, spec, path, digest) =
                self.start(reference, Role::Source, config).await?;
            Ok(Placed {
                connector: Box::new(SupervisedSource(Arc::new(supervisor))) as Box<dyn Source>,
                spec,
                placement: Placement::Process { path },
                digest: Some(digest),
            })
        })
    }

    fn destination<'a>(
        &'a self,
        reference: &'a ConnectorRef,
        config: &'a serde_json::Value,
    ) -> BoxFuture<'a, Result<Placed<Box<dyn Destination>>, ProviderError>> {
        Box::pin(async move {
            let (supervisor, spec, path, digest) =
                self.start(reference, Role::Destination, config).await?;
            let capabilities = supervisor
                .capabilities()
                .await
                .map_err(|source| handshake_failed(reference, source))?;
            let destination = SupervisedDestination {
                supervisor: Arc::new(supervisor),
                capabilities,
            };
            Ok(Placed {
                connector: Box::new(destination) as Box<dyn Destination>,
                spec,
                placement: Placement::Process { path },
                digest: Some(digest),
            })
        })
    }
}
