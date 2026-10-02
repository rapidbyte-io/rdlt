//! Process placement: a connector found by its path, or by name in the directories its
//! operator named, is opened once, spawned from that open file with its socket on file
//! descriptor 3, inside a sandbox unless its binaries are stated to be trusted, and respawned
//! when it is lost.

mod binary;
mod bubblewrap;
mod grants;
pub(crate) mod process;
mod provide;
mod sandbox;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{ConnectorSpec, Role};

use crate::kills::Kills;
use crate::secrets::{Config, SecretResolver, Secrets};
use crate::supervise::{Configured, Gate, Spawned, Start, Supervisor};
use binary::{Binary, Unfit};
pub use bubblewrap::Bubblewrap;
use grants::{Chain, Claim, Guarded, Lease, Leases, Opened, Program, Roots};
pub use process::{Interrupts, LastWords, Lingering, StopsSpawned, Witness, spawned, stop_spawned};
use process::{Launch, Process};
pub(crate) use provide::refused;
use provide::{binary_name, handshake_failed, spawn_failed};
pub use sandbox::{
    Bind, Confined, Grants, Launcher, NetworkGrant, OsError, Sandbox, SandboxError, Stops,
};

use crate::provider::{ConnectorRef, Digest, Honours, Isolation, ProviderError};
use crate::remote::Options;
use crate::wire::Wire;

/// Places connectors in processes of their own.
///
/// A connector's code is not trusted: it is spawned inside the [`Sandbox`] the provider was
/// built with ([`Local::sandboxed`]), unless its operator states that every binary the
/// provider spawns is trusted ([`Local::trusting_binaries`]). There is no third way to build
/// one.
///
/// Each connector leads a process group this process owns. Dropping a connector asks its
/// group to stop before the drop returns; the kill that follows its grace needs this process
/// to be running still. A host therefore calls [`stop_spawned`] before it exits, or holds a
/// [`StopsSpawned`], and listens for the signals that would end it ([`Interrupts`]).
#[derive(Clone, Debug)]
pub struct Local {
    sandbox: Option<Arc<dyn Sandbox>>,
    /// Paths every connector may read.
    shared_reads: Vec<PathBuf>,
    /// Where a reference's grants may be made.
    roots: Roots,
    /// Where the host keeps its own files.
    guarded_dirs: Vec<PathBuf>,
    /// What each placement of this process holds now.
    leases: Leases,
    dirs: Vec<PathBuf>,
    grace: Duration,
    env_passthrough: Vec<String>,
    options: Options,
    kills: Option<Kills>,
    told: Option<process::Told>,
    secrets: Arc<dyn SecretResolver>,
}

impl Local {
    fn new(sandbox: Option<Arc<dyn Sandbox>>) -> Self {
        Self {
            sandbox,
            shared_reads: Vec::new(),
            roots: Roots::default(),
            guarded_dirs: Vec::new(),
            leases: Leases::process(),
            dirs: Vec::new(),
            grace: Duration::from_secs(10),
            env_passthrough: Vec::new(),
            options: Options::default(),
            kills: None,
            told: None,
            secrets: Arc::new(Secrets::new()),
        }
    }

    /// Spawns each connector inside `sandbox`, with nothing of the host's but what is granted
    /// every connector ([`grant_read`](Self::grant_read)) and what its reference grants it;
    /// stops them with a grace period of 10 s.
    pub fn sandboxed(sandbox: impl Sandbox + 'static) -> Self {
        Self::new(Some(Arc::new(sandbox)))
    }

    /// Spawns each connector with this process's own access to files, the network and other
    /// processes: for binaries their operator trusts as the host itself.
    pub fn trusting_binaries() -> Self {
        Self::new(None)
    }

    /// Looks for connectors named without a path in `dir`, an absolute path to a directory no
    /// other user may write; nowhere else is searched.
    #[must_use]
    pub fn connector_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dirs.push(dir.into());
        self
    }

    /// Lets every sandboxed connector read `path`, an absolute path, as a system directory is
    /// read: what one pipeline's connector alone may read or write is granted on its
    /// reference ([`ConnectorRef::grant_write`]).
    #[must_use]
    pub fn grant_read(mut self, path: impl Into<PathBuf>) -> Self {
        self.shared_reads.push(path.into());
        self
    }

    /// Lets a reference grant its connector reads within `root`, an absolute path: a
    /// reference's grants lie within the roots its operator names, and none are made where
    /// none is named.
    #[must_use]
    pub fn grantable_read(mut self, root: impl Into<PathBuf>) -> Self {
        self.roots.read.push(root.into());
        self
    }

    /// Lets a reference grant its connector reads and writes within `root`, an absolute path.
    ///
    /// No placement is made while `root` holds, or lies within, what decides what the host
    /// runs or keeps: a connector directory, a binary placed or its directory, a script's
    /// interpreter, the sandbox's launcher, the host's executable's directory, a directory
    /// the host keeps its own files in ([`guarded_dir`](Self::guarded_dir)), or one secrets
    /// are read from.
    #[must_use]
    pub fn grantable_write(mut self, root: impl Into<PathBuf>) -> Self {
        self.roots.write.push(root.into());
        self
    }

    /// Names `dir`, an absolute path, as a directory the host keeps its own files in, as its
    /// write-ahead log, its state, or secrets it reads itself: no root grants may write within
    /// may hold it or lie within it.
    #[must_use]
    pub fn guarded_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.guarded_dirs.push(dir.into());
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

    /// Resolves the secret references of each configuration with `secrets`.
    #[must_use]
    pub fn secrets(mut self, secrets: impl SecretResolver + 'static) -> Self {
        self.secrets = Arc::new(secrets);
        self
    }

    /// Tells `told` the process id of each connector as it is spawned, one spawned again after
    /// it was lost too: the id of the process group it leads.
    ///
    /// [`spawned`] lists only the connectors that run; what is told here is every connector
    /// there was, before it has answered anything.
    #[must_use]
    pub fn on_spawn(mut self, told: impl Fn(u32) + Send + Sync + 'static) -> Self {
        self.told = Some(process::Told::new(told));
        self
    }

    /// Where the binary `reference` names is: its path, or
    /// `rdlt-connector-<the id's last segment>` in the first connector directory that holds it.
    ///
    /// # Errors
    ///
    /// As [`wire`](Self::wire) fails before it spawns.
    pub fn resolve(&self, reference: &ConnectorRef) -> Result<PathBuf, ProviderError> {
        Ok(self.found(reference)?.path().to_owned())
    }

    /// What this provider honours of a reference.
    fn honours(&self) -> Honours {
        Honours {
            placement: "process",
            path: true,
            endpoint: false,
            digest: cfg!(target_os = "linux"),
            grants: true,
            isolation: match self.sandbox {
                Some(_) => &[Isolation::Process, Isolation::Sandbox],
                None => &[Isolation::Process],
            },
        }
    }

    /// Opens the binary `reference` names, once every requirement of the reference is one
    /// this provider honours.
    fn found(&self, reference: &ConnectorRef) -> Result<Binary, ProviderError> {
        self.honours().admit(reference)?;
        let found = match &reference.path {
            Some(path) => Binary::at(path),
            None => Binary::named(&self.dirs, &binary_name(&reference.id)),
        };
        found.map_err(|unfit| match unfit {
            Unfit::Absent(source) => ProviderError::NotFound {
                id: reference.id.clone(),
                source,
            },
            Unfit::Shared { path, owner, mode } => ProviderError::Shared {
                id: reference.id.clone(),
                path,
                owner,
                mode,
            },
        })
    }

    /// How the binary `reference` names is launched, and its digest where this platform
    /// executes what it hashed: the binary is opened, and its digest compared with what the
    /// reference requires.
    async fn launch(&self, reference: &ConnectorRef) -> Result<Launch, ProviderError> {
        let binary = Arc::new(self.found(reference)?);
        let found = Arc::clone(&binary);
        let digest = if cfg!(target_os = "linux") {
            let hashed = Arc::clone(&binary);
            let hashing = tokio::task::spawn_blocking(move || hashed.digest());
            let hashed = hashing.await.map_err(std::io::Error::other).flatten();
            Some(hashed.map_err(|source| spawn_failed(reference, binary.path(), source))?)
        } else {
            None
        };
        if let (Some(expected), Some(found)) = (reference.digest, digest)
            && expected != found
        {
            return Err(ProviderError::DigestMismatch {
                id: reference.id.clone(),
                path: binary.path().to_owned(),
                expected,
                found,
            });
        }
        let confinement = self.confinement(reference)?;
        let lease = self.lease(reference, &found, confinement.is_some())?;
        Ok(Launch {
            id: reference.id.clone(),
            binary,
            digest,
            env_passthrough: self.env_passthrough.clone(),
            grace: self.grace,
            kills: self.kills.clone(),
            told: self.told.clone(),
            confinement,
            lease: Arc::new(lease),
            state_bytes: Some(self.options.limits.state_bytes)
                .filter(|bytes| *bytes != rdlt_wire::limits::STATE_BYTES),
        })
    }

    /// What confines the connector `reference` names: the provider's sandbox, and the
    /// network the reference grants; none for a trusted binary, which has the host's access.
    fn confinement(
        &self,
        reference: &ConnectorRef,
    ) -> Result<Option<process::Confinement>, ProviderError> {
        let Some(sandbox) = &self.sandbox else {
            return Ok(None);
        };
        if cfg!(not(target_os = "linux")) {
            return Err(ProviderError::Sandbox {
                id: reference.id.clone(),
                source: SandboxError::Unsupported,
            });
        }
        Ok(Some(process::Confinement {
            sandbox: Arc::clone(sandbox),
            network: reference.grants.network,
        }))
    }

    /// What a placement of `reference`, run from `binary`, holds while it lives: the programs
    /// it runs and, where it is `confined`, what it is granted, each path opened once and
    /// checked against the roots, what is guarded, and what other placements hold now.
    fn lease(
        &self,
        reference: &ConnectorRef,
        binary: &Binary,
        confined: bool,
    ) -> Result<Lease, ProviderError> {
        let programs =
            programs(binary).map_err(|source| spawn_failed(reference, binary.path(), source))?;
        let ungranted = Grants::default();
        let (grants, shared_reads) = if confined {
            (&reference.grants, self.shared_reads.as_slice())
        } else {
            (&ungranted, &[][..])
        };
        let claim = Claim {
            grants,
            shared_reads,
            roots: &self.roots,
            guarded: &self.guarded(),
            programs: &programs,
        };
        self.leases
            .take(&claim)
            .map_err(|source| ProviderError::Sandbox {
                id: reference.id.clone(),
                source,
            })
    }

    /// What no root grants may write within may hold: where connectors are found, the
    /// host's executable is, the host keeps its own files, and secrets are read from; and what
    /// confines a connector.
    fn guarded(&self) -> Guarded {
        let mut directories = self.dirs.clone();
        let executable = std::env::current_exe().ok();
        directories.extend(executable.and_then(|exe| exe.parent().map(Path::to_owned)));
        directories.extend(self.guarded_dirs.iter().cloned());
        directories.extend(self.secrets.directories());
        let files = self.sandbox.as_ref().map(|sandbox| sandbox.programs());
        Guarded {
            directories,
            files: files.unwrap_or_default(),
        }
    }

    /// A raw connection to the connector `reference` names, before its handshake: its binary,
    /// found, checked and spawned as [`source`](crate::Provider::source) spawns it, with the
    /// connection's other end on file descriptor 3, stops once the wire is dropped.
    ///
    /// # Errors
    ///
    /// [`ProviderError::Unsupported`] for a reference that requires what this provider does
    /// not honour, [`ProviderError::NotFound`] or [`ProviderError::Shared`] for a binary that
    /// is not there or not its operator's alone, [`ProviderError::DigestMismatch`] for one of
    /// another digest than required, [`ProviderError::Sandbox`] where its sandbox cannot be
    /// made, and [`ProviderError::SpawnFailed`] when it cannot be spawned.
    pub async fn wire(&self, reference: &ConnectorRef) -> Result<Wire, ProviderError> {
        let launch = self.launch(reference).await?;
        let launched = Process::launching(launch, crate::secrets::Redactions::new()).await;
        let (stream, process) = launched.map_err(|unspawned| {
            let (launch, unspawned) = *unspawned;
            refused(&launch, unspawned)
        })?;
        Ok(Wire::new(stream, Some(process)))
    }

    /// Spawns the connector `reference` names as `role`, and checks it is that connector.
    async fn start(
        &self,
        reference: &ConnectorRef,
        role: Role,
        config: &serde_json::Value,
    ) -> Result<(Supervisor, ConnectorSpec, PathBuf, Option<Digest>), ProviderError> {
        let launch = self.launch(reference).await?;
        let (path, digest) = (launch.binary.path().to_owned(), launch.digest);
        let found_at = path.display().to_string();
        let gate = Gate {
            reference,
            found_at: &found_at,
        };
        let configured = Configured {
            config: Config::from(config),
            secrets: Arc::clone(&self.secrets),
        };
        let supervisor =
            Supervisor::start(Start::Spawn(launch), role, configured, &self.options, &gate)
                .await
                .map_err(|spawned| match spawned {
                    Spawned::Io(source) | Spawned::Unreachable(source) | Spawned::Tls(source) => {
                        spawn_failed(reference, &path, source)
                    }
                    Spawned::Connect(source) => handshake_failed(reference, source),
                    Spawned::Secret(source) => ProviderError::Secret {
                        id: reference.id.clone(),
                        source,
                    },
                    Spawned::Refused(refused) => refused,
                })?;
        let spec = supervisor.spec();
        Ok((supervisor, spec, path, digest))
    }
}

/// The programs a placement of `binary` runs: the binary, and a script's interpreter where it
/// names one that is there.
fn programs(binary: &Binary) -> std::io::Result<Vec<Program>> {
    let path = binary.path().to_owned();
    let chain = Chain::of(binary.file(), &path)?;
    let mut programs = vec![Program { path, chain }];
    if let Some(interpreter) = binary.interpreter()?
        && let Ok(opened) = Opened::at(&interpreter)
    {
        programs.push(Program {
            path: interpreter,
            chain: opened.chain,
        });
    }
    Ok(programs)
}
