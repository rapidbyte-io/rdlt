//! The host: what its operator allows the pipelines built on it.

#[cfg(test)]
pub(crate) mod tests;
#[cfg(feature = "remote")]
mod tls;

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use rdlt_connector::{
    ConnectorSpec, DestinationConnector, DestinationFactory, Role, Secret, SourceConnector,
    SourceFactory, destination_factory, source_factory,
};
use rdlt_engine::Transform;
use rdlt_host::{
    Config, Provider as _, Redactions, Registry, SecretError, SecretResolver, Secrets,
};

#[cfg(feature = "remote")]
pub use tls::Tls;

use crate::document::refers_to_secret;
use crate::{Connector, Error, Side, codes};

/// What a host's operator allows the pipelines built on it: where local connectors are found and
/// how they run, which paths a pipeline may let a connector reach, which secrets a configuration
/// may refer to, how remote connectors are reached, and the code compiled in.
///
/// Nothing a pipeline's document says widens it. `Host::new()` runs local connectors in the
/// sandbox, finds them nowhere, lets a pipeline grant nothing and resolves no secret.
///
/// ```
/// let host = rdlt::Host::new()
///     .connectors("/opt/rdlt/bin")
///     .writable("/data")
///     .allow_env(["DB_PASSWORD"]);
/// # drop(host);
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
    /// the secret.
    pub(crate) async fn key(&self, text: &str) -> Result<Secret<String>, Error> {
        if !refers_to_secret(text) {
            let message = "a mask's key is a secret reference, ${env:NAME}, ${file:/path} or \
                           ${secret:name}, never the key itself: keep the key where the host \
                           resolves secrets";
            return Err(Error::config(codes::MASK_KEY_LITERAL, message));
        }
        let held = Config::from(&serde_json::json!({ "key": text }));
        // Nothing shows a mask key, so no text needs the key scrubbed from it.
        let redactions = Redactions::new();
        let resolving = held.resolved(&self.secrets, &redactions);
        let resolved = resolving.await.map_err(Error::secret)?;
        let Keyed { key } =
            serde_json::from_str(&resolved).map_err(|_| Error::secret(SecretError::NotJson))?;
        Ok(key)
    }

    /// The code transform registered as `name`.
    pub(crate) fn code(&self, name: &str) -> Option<Arc<dyn Transform>> {
        self.transforms.get(name).map(Arc::clone)
    }

    /// The names of the code transforms the host registered.
    pub(crate) fn codes(&self) -> impl Iterator<Item = &str> {
        self.transforms.keys().map(String::as_str)
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
            .finish()
    }
}
