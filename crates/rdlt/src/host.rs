//! The host: what its operator allows the pipelines built on it.

mod file;
#[cfg(test)]
pub(crate) mod tests;
#[cfg(feature = "remote")]
mod tls;

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectorSpec, DestinationConnector, DestinationFactory, Role, Secret, SourceConnector,
    SourceFactory, destination_factory, source_factory,
};
use rdlt_engine::{SystemEnv, Transform};
use rdlt_host::{
    Config, Provider as _, Redactions, Registry, SecretError, SecretResolver, Secrets,
};
use tokio::sync::OnceCell;

pub use file::{HostFile, RemoteFiles};
#[cfg(feature = "remote")]
pub use tls::Tls;

use crate::document::{KEY_WRITTEN_OUT, refers_to_secret};
use crate::limits::BUILD_WAIT;
use crate::{Connector, Error, Side, codes};

/// What a host's operator allows the pipelines built on it: where local connectors are found and
/// how they run, which paths a pipeline may let a connector reach, which secrets a configuration
/// may refer to, how remote connectors are reached, and the code compiled in.
///
/// Nothing a pipeline's document says widens it. `Host::new()` runs local connectors in the
/// sandbox, finds them nowhere, lets a pipeline grant nothing and resolves no secret.
///
/// ```
/// # #[cfg(feature = "local")] {
/// let host = rdlt::Host::new()
///     .connectors("/opt/rdlt/bin")
///     .writable("/data")
///     .allow_env(["DB_PASSWORD"]);
/// # drop(host);
/// # }
/// ```
#[must_use]
pub struct Host {
    #[cfg(feature = "local")]
    local: LocalPolicy,
    secrets: Secrets,
    #[cfg(feature = "remote")]
    remote: Option<Tls>,
    sources: Vec<Arc<dyn SourceFactory>>,
    destinations: Vec<Arc<dyn DestinationFactory>>,
    transforms: BTreeMap<String, Arc<dyn Transform>>,
    env: OnceCell<Arc<SystemEnv>>,
    build_wait: Duration,
}

/// A mask key's resolved reference, which is the secret alone.
#[derive(serde::Deserialize)]
struct Keyed {
    key: Secret<String>,
}

/// How local connectors are found and run.
#[cfg(feature = "local")]
#[derive(Clone, Debug, Default)]
struct LocalPolicy {
    trusted: bool,
    network: bool,
    connectors: Vec<PathBuf>,
    writable: Vec<PathBuf>,
    readable: Vec<PathBuf>,
    always_readable: Vec<PathBuf>,
}

impl Default for Host {
    fn default() -> Self {
        Self {
            #[cfg(feature = "local")]
            local: LocalPolicy::default(),
            secrets: Secrets::new(),
            #[cfg(feature = "remote")]
            remote: None,
            sources: Vec::new(),
            destinations: Vec::new(),
            transforms: BTreeMap::new(),
            env: OnceCell::new(),
            build_wait: BUILD_WAIT,
        }
    }
}

impl Host {
    /// A host that allows nothing yet, and runs local connectors in the sandbox.
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs local connectors with this process's own access, in no sandbox: for binaries the
    /// operator trusts as the host itself.
    #[cfg(feature = "local")]
    pub fn trust_binaries(mut self) -> Self {
        self.local.trusted = true;
        self
    }

    /// Lets a pipeline let its sandboxed connectors share this host's network, its loopback
    /// services included, which a document's `access: { network: true }` asks for and nothing
    /// but this allows.
    #[cfg(feature = "local")]
    pub fn allow_network(mut self) -> Self {
        self.local.network = true;
        self
    }

    /// Finds local connectors, as `rdlt-connector-<the id's last segment>`, in `dir`, an
    /// absolute path no other user may write; nowhere else is searched.
    #[cfg(feature = "local")]
    pub fn connectors(mut self, dir: impl Into<PathBuf>) -> Self {
        self.local.connectors.push(dir.into());
        self
    }

    /// Lets a pipeline let its connectors read and write beneath `root`, an absolute path.
    #[cfg(feature = "local")]
    pub fn writable(mut self, root: impl Into<PathBuf>) -> Self {
        self.local.writable.push(root.into());
        self
    }

    /// Lets a pipeline let its connectors read beneath `root`, an absolute path.
    #[cfg(feature = "local")]
    pub fn readable(mut self, root: impl Into<PathBuf>) -> Self {
        self.local.readable.push(root.into());
        self
    }

    /// Lets every sandboxed connector read `path`, an absolute path.
    #[cfg(feature = "local")]
    pub fn always_readable(mut self, path: impl Into<PathBuf>) -> Self {
        self.local.always_readable.push(path.into());
        self
    }

    /// Gives each thing building a pipeline asks of something else, a connector's placement or
    /// a mask key's secret, `wait` to answer, in place of [`BUILD_WAIT`].
    pub fn build_wait(mut self, wait: Duration) -> Self {
        self.build_wait = wait;
        self
    }

    /// Lets a configuration's `${env:NAME}` reach each of `names`.
    pub fn allow_env(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.secrets = self.secrets.allow_env(names);
        self
    }

    /// Lets a configuration's `${file:/path}` reach private files beneath each of
    /// `directories`, each an absolute path.
    pub fn allow_files(
        mut self,
        directories: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) -> Self {
        self.secrets = self.secrets.allow_files(directories);
        self
    }

    /// Resolves a configuration's `${secret:name}` with `store`.
    pub fn secret_store(mut self, store: impl SecretResolver + 'static) -> Self {
        self.secrets = self.secrets.secret_store(store);
        self
    }

    /// Reaches connectors whose reference names an endpoint over mutual TLS, as `tls` says.
    #[cfg(feature = "remote")]
    pub fn remote(mut self, tls: Tls) -> Self {
        self.remote = Some(tls);
        self
    }

    /// Runs source `C` in process, compiled in and trusted as the host's own code.
    ///
    /// ```
    /// use rdlt_connector_reference::{MemoryDestination, MemorySource};
    ///
    /// let host = rdlt::Host::new()
    ///     .source::<MemorySource>()
    ///     .destination::<MemoryDestination>();
    /// # drop(host);
    /// ```
    pub fn source<C: SourceConnector>(self) -> Self {
        self.source_factory(Arc::from(source_factory::<C>()))
    }

    /// Runs the source `factory` connects in process, trusted as the host's own code, under the
    /// id its spec gives.
    pub fn source_factory(mut self, factory: Arc<dyn SourceFactory>) -> Self {
        self.sources.push(factory);
        self
    }

    /// Runs destination `C` in process, compiled in and trusted as the host's own code.
    pub fn destination<C: DestinationConnector>(self) -> Self {
        self.destination_factory(Arc::from(destination_factory::<C>()))
    }

    /// Runs the destination `factory` connects in process, trusted as the host's own code, under
    /// the id its spec gives.
    pub fn destination_factory(mut self, factory: Arc<dyn DestinationFactory>) -> Self {
        self.destinations.push(factory);
        self
    }

    /// Lets a document's `transform: [{ code: name }]` run `transform`, the host's own code.
    ///
    /// ```
    /// use rdlt::{Host, Op};
    ///
    /// let host = Host::new().transform("contact", Op::rename([("email", "contact")]));
    /// # drop(host);
    /// ```
    pub fn transform(mut self, name: impl Into<String>, transform: impl Transform) -> Self {
        self.transforms.insert(name.into(), Arc::new(transform));
        self
    }

    /// What `connector` says it is, as `role`, from its handshake, before any configuration
    /// reaches it; placed as a pipeline would place it.
    ///
    /// # Errors
    ///
    /// The placement's error, as building a pipeline reports it.
    pub async fn spec(&self, connector: &Connector, role: Role) -> Result<ConnectorSpec, Error> {
        let reference = crate::lower::reference(&Side::new(connector.clone()), role)?;
        self.provider()
            .spec(&reference, role)
            .await
            .map_err(|error| Error::placement(role, error))
    }

    /// How long building a pipeline waits for each thing it asks of something else.
    pub(crate) fn wait(&self) -> Duration {
        self.build_wait
    }

    /// The registry a pipeline is placed by: the compiled-in connectors, then remote and local
    /// placement as this host allows them.
    pub(crate) fn provider(&self) -> Registry {
        let mut registry = Registry::trusted().secrets(self.secrets.clone());
        for factory in &self.sources {
            registry = registry.trusted_source_factory(Arc::clone(factory));
        }
        for factory in &self.destinations {
            registry = registry.trusted_destination_factory(Arc::clone(factory));
        }
        #[cfg(feature = "remote")]
        if let Some(tls) = &self.remote {
            registry = registry.remote(tls.remote().secrets(self.secrets.clone()));
        }
        #[cfg(feature = "local")]
        {
            registry = registry.local(self.local());
        }
        registry
    }

    /// The mask key the secret reference `text` refers to, resolved as a configuration's
    /// references are.
    ///
    /// A key written out is refused, never resolved or shown: a document holding it would hold
    /// the secret. A store that does not answer within the host's wait fails retryably.
    pub(crate) async fn key(&self, text: &str) -> Result<Secret<String>, Error> {
        if !refers_to_secret(text) {
            return Err(Error::config(codes::MASK_KEY_LITERAL, KEY_WRITTEN_OUT));
        }
        let held = Config::from(&serde_json::json!({ "key": text }));
        // Nothing shows a mask key, so no text needs the key scrubbed from it.
        let redactions = Redactions::new();
        let resolving = held.resolved(&self.secrets, &redactions);
        let Ok(resolved) = tokio::time::timeout(self.build_wait, resolving).await else {
            return Err(Error::secret_expired(self.build_wait));
        };
        let resolved = resolved.map_err(Error::secret)?;
        let Keyed { key } =
            serde_json::from_str(&resolved).map_err(|_| Error::secret(SecretError::NotJson))?;
        Ok(key)
    }

    /// Refuses `side`, the pipeline's `role`, asking to share this host's network where its
    /// connector would run in the sandbox and the host has not allowed that: nothing a document
    /// says widens what its host allows.
    ///
    /// A trusted binary has the host's access and a remote connector its own network; a
    /// compiled-in connector honours no access, as placing it says.
    #[cfg(feature = "local")]
    pub(crate) fn admits_network(&self, side: &Side, role: Role) -> Result<(), Error> {
        let remote = matches!(
            &side.connector,
            Connector::Reference(reference) if reference.endpoint.is_some()
        );
        let allowed = self.local.trusted || self.local.network;
        if !side.access.network || allowed || remote || self.compiled_in(&side.connector, role) {
            return Ok(());
        }
        Err(Error::refused(
            role,
            "shares the host's network only where the host allows it, with `allow_network`",
        ))
    }

    /// Whether `connector`, as `role`, is one compiled into this host.
    #[cfg(feature = "local")]
    fn compiled_in(&self, connector: &Connector, role: Role) -> bool {
        let named = |spec: &ConnectorSpec| spec.id.as_str() == connector.id();
        match role {
            Role::Source => self.sources.iter().any(|factory| named(factory.spec())),
            Role::Destination => self
                .destinations
                .iter()
                .any(|factory| named(factory.spec())),
        }
    }

    /// The code transform registered as `name`.
    pub(crate) fn code(&self, name: &str) -> Option<Arc<dyn Transform>> {
        self.transforms.get(name).map(Arc::clone)
    }

    /// The names of the code transforms the host registered.
    pub(crate) fn codes(&self) -> impl Iterator<Item = &str> {
        self.transforms.keys().map(String::as_str)
    }

    /// The environment every pipeline of this host runs on: one compute pool of a thread a core
    /// the process may run on, started on first use by one pipeline however many build at once,
    /// and tried again by the next where it failed to start.
    pub(crate) async fn environment(&self) -> Result<Arc<SystemEnv>, Error> {
        let starting = || async {
            let started = SystemEnv::try_from_host().map_err(|error| {
                let message = "starting the compute pool";
                Error::internal(codes::ENVIRONMENT_UNAVAILABLE, message, error)
            })?;
            Ok(Arc::new(started))
        };
        self.env.get_or_try_init(starting).await.map(Arc::clone)
    }

    /// Local placement as this host allows it: sandboxed, unless the host trusts its binaries.
    #[cfg(feature = "local")]
    fn local(&self) -> rdlt_host::Local {
        use rdlt_host::{Bubblewrap, Local};
        let policy = &self.local;
        let mut local = if policy.trusted {
            Local::trusting_binaries()
        } else {
            Local::sandboxed(Bubblewrap::new())
        };
        for dir in &policy.connectors {
            local = local.connectors(dir);
        }
        for root in &policy.writable {
            local = local.writable(root);
        }
        for root in &policy.readable {
            local = local.readable(root);
        }
        for path in &policy.always_readable {
            local = local.always_readable(path);
        }
        local.secrets(self.secrets.clone())
    }
}

impl fmt::Debug for Host {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let named = |spec: &ConnectorSpec| spec.id.to_string();
        let sources: Vec<String> = self
            .sources
            .iter()
            .map(|factory| named(factory.spec()))
            .collect();
        let destinations: Vec<String> = self
            .destinations
            .iter()
            .map(|factory| named(factory.spec()))
            .collect();
        let transforms: Vec<&String> = self.transforms.keys().collect();
        let mut debug = formatter.debug_struct("Host");
        #[cfg(feature = "local")]
        debug.field("local", &self.local);
        #[cfg(feature = "remote")]
        debug.field("remote", &self.remote);
        debug
            .field("secrets", &self.secrets)
            .field("sources", &sources)
            .field("destinations", &destinations)
            .field("transforms", &transforms)
            .field("build_wait", &self.build_wait)
            .finish_non_exhaustive()
    }
}
