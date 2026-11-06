//! Resolving secret references: the trait an embedder implements, and the resolvers of
//! environment variables and private files, each reaching only what its operator lists.

mod env;
mod file;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use rdlt_connector::{BoxFuture, Secret};

use crate::limits::SECRET_BYTES;

pub use env::EnvSecrets;
pub(crate) use file::FileSecrets;

/// What a secret reference names its secret by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretKind {
    /// An environment variable of the host: `${env:NAME}`.
    Env,
    /// A file only the host's user may read: `${file:/absolute/path}`.
    File,
    /// A name the embedder's resolver knows: `${secret:name}`.
    Named,
}

impl fmt::Display for SecretKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Env => "env",
            Self::File => "file",
            Self::Named => "secret",
        })
    }
}

/// A reference to a secret: its kind, and the name that kind knows it by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretReference {
    /// What names the secret.
    pub kind: SecretKind,
    /// The variable, the path or the name.
    pub name: String,
}

/// Why a reference did not resolve; none says what the secret, or the reference, holds.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SecretFault {
    /// Nothing is set by that name.
    #[error("nothing is set by that name")]
    Missing,
    /// The resolver resolves no reference of that kind.
    #[error("no resolver resolves references of that kind")]
    Unsupported,
    /// The host's operator lets no reference reach what this one names: a variable not listed,
    /// a file outside the directories given, or a kind no resolver was given for.
    #[error("the host resolves no such reference")]
    Refused,
    /// The secret is not UTF-8 text.
    #[error("the secret is not text")]
    NotText,
    /// The secret is longer than a secret may be.
    #[error("the secret is longer than {limit} bytes")]
    TooLong {
        /// Bytes a secret may take.
        limit: u64,
    },
    /// The file's path is not absolute.
    #[error("the file's path is not absolute")]
    Relative,
    /// The path names a link, a directory, a pipe or a device, not a regular file.
    #[error("the path names no regular file")]
    NotRegular,
    /// The file belongs to another user, or its group or others may read or write it.
    #[error("the file is not private: it belongs to user {owner} with mode {mode:o}")]
    Shared {
        /// The user the file belongs to.
        owner: u32,
        /// The file's permission bits.
        mode: u32,
    },
    /// Reading the file failed.
    #[error("the file could not be read")]
    Io(#[source] std::io::Error),
    /// The embedder's resolver failed.
    #[error("the resolver failed")]
    Failed(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Resolves secret references: what an embedder implements to reach its secret store.
pub trait SecretResolver: fmt::Debug + Send + Sync {
    /// The secret `reference` names.
    ///
    /// # Errors
    ///
    /// A [`SecretFault`] saying why it has none.
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>>;

    /// The directories this resolver reads secrets from, which no grant may write.
    fn directories(&self) -> Vec<PathBuf> {
        Vec::new()
    }
}

/// Refuses a secret of `bytes`, beyond [`SECRET_BYTES`].
fn admit(bytes: usize) -> Result<(), SecretFault> {
    if u64::try_from(bytes).unwrap_or(u64::MAX) > SECRET_BYTES {
        return Err(SecretFault::TooLong {
            limit: SECRET_BYTES,
        });
    }
    Ok(())
}

/// The resolvers a provider uses, each allowed by the operator: none by default, so that a
/// configuration's author reaches no secret of the host's but those the operator allows.
///
/// `${env:NAME}` reaches the variables [`allow_env`](Self::allow_env) and
/// [`allow_env_prefix`](Self::allow_env_prefix) allow, `${file:/path}` the directories
/// [`allow_files`](Self::allow_files) allows, and `${secret:name}` the
/// [`secret_store`](Self::secret_store); every other reference is refused.
#[derive(Clone, Default)]
pub struct Secrets {
    env: Vec<EnvSecrets>,
    files: Option<FileSecrets>,
    store: Option<Arc<dyn SecretResolver>>,
}

impl fmt::Debug for Secrets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Secrets")
            .field("env", &self.env)
            .field("files", &self.files)
            .field("store", &self.store.is_some())
            .finish()
    }
}

impl Secrets {
    /// Resolves nothing: every reference is refused.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lets `${env:NAME}` reach each of `names`, beside what was allowed before.
    #[must_use]
    pub fn allow_env(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.env.push(EnvSecrets::allowing(names));
        self
    }

    /// Lets `${env:NAME}` reach each variable whose name starts with `prefix`, which may not be
    /// empty: under an empty one nothing is reached.
    #[must_use]
    pub fn allow_env_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.env.push(EnvSecrets::prefixed(prefix));
        self
    }

    /// Lets `${file:/path}` reach private files beneath each of `directories`, each an absolute
    /// path, beside those allowed before.
    #[must_use]
    pub fn allow_files(
        mut self,
        directories: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) -> Self {
        let allowed = match self.files.take() {
            Some(files) => files.extended(directories),
            None => FileSecrets::within(directories),
        };
        self.files = Some(allowed);
        self
    }

    /// Resolves `${secret:name}` with `resolver`: the operator's store of named secrets.
    #[must_use]
    pub fn secret_store(mut self, resolver: impl SecretResolver + 'static) -> Self {
        self.store = Some(Arc::new(resolver));
        self
    }
}

impl SecretResolver for Secrets {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            match reference.kind {
                SecretKind::Env => {
                    for env in &self.env {
                        match env.resolve(reference).await {
                            Err(SecretFault::Refused) => {}
                            answered => return answered,
                        }
                    }
                    Err(SecretFault::Refused)
                }
                SecretKind::File => match &self.files {
                    Some(files) => files.resolve(reference).await,
                    None => Err(SecretFault::Refused),
                },
                SecretKind::Named => match &self.store {
                    Some(store) => store.resolve(reference).await,
                    None => Err(SecretFault::Refused),
                },
            }
        })
    }

    fn directories(&self) -> Vec<PathBuf> {
        let files = self.files.iter().flat_map(SecretResolver::directories);
        let store = self.store.iter().flat_map(|store| store.directories());
        files.chain(store).collect()
    }
}
