//! Resolving secret references: the trait an embedder implements, and the resolvers of
//! environment variables and private files.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::Read as _;
use std::path::Path;
use std::sync::Arc;

use rdlt_connector::{BoxFuture, Secret};
use rustix::fs::{Mode, OFlags};

use crate::limits::SECRET_BYTES;

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
}

/// Resolves `${env:NAME}` to the host's environment variable `NAME`, and `${secret:name}` to
/// its variable `RDLT_SECRET_<NAME>`, the name in upper case with `_` for every character
/// that is neither a letter nor a digit.
#[derive(Clone, Copy, Debug)]
pub struct EnvSecrets {
    variable: fn(&OsStr) -> Option<OsString>,
}

impl Default for EnvSecrets {
    fn default() -> Self {
        Self {
            variable: |name| std::env::var_os(name),
        }
    }
}

impl EnvSecrets {
    /// Reads the host's environment.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads variables through `variable`, in place of the host's environment.
    #[cfg(test)]
    pub(crate) fn reading(variable: fn(&OsStr) -> Option<OsString>) -> Self {
        Self { variable }
    }

    fn read(self, variable: &str) -> Result<Secret<String>, SecretFault> {
        let value = (self.variable)(OsStr::new(variable)).ok_or(SecretFault::Missing)?;
        let value = value.into_string().map_err(|_| SecretFault::NotText)?;
        let secret = Secret::new(value);
        admit(secret.expose().len())?;
        Ok(secret)
    }
}

/// The variable `${secret:name}` is read from.
fn named_variable(name: &str) -> String {
    let letter = |c: char| match c {
        c if c.is_ascii_alphanumeric() => c.to_ascii_uppercase(),
        _ => '_',
    };
    format!(
        "RDLT_SECRET_{}",
        name.chars().map(letter).collect::<String>()
    )
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

impl SecretResolver for EnvSecrets {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            match reference.kind {
                SecretKind::Env => self.read(&reference.name),
                SecretKind::Named => self.read(&named_variable(&reference.name)),
                SecretKind::File => Err(SecretFault::Unsupported),
            }
        })
    }
}

/// Resolves `${file:/absolute/path}` to the text of a file that is the host's user's alone:
/// a regular file, reached through no link at its name, that neither its group nor others
/// may read or write, of [`SECRET_BYTES`] at most; one line end at its end is not the
/// secret's.
#[derive(Clone, Copy, Debug, Default)]
pub struct FileSecrets;

impl FileSecrets {
    /// Reads private files.
    pub fn new() -> Self {
        Self
    }
}

/// The text of the private file at `path`.
fn read_private(path: &Path) -> Result<Secret<String>, SecretFault> {
    if !path.is_absolute() {
        return Err(SecretFault::Relative);
    }
    // No link followed at the name, no waiting on a pipe, nothing kept across an exec.
    let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let file = match rustix::fs::open(path, flags | OFlags::NOCTTY, Mode::empty()) {
        Ok(fd) => std::fs::File::from(fd),
        Err(rustix::io::Errno::LOOP | rustix::io::Errno::MLINK) => {
            return Err(SecretFault::NotRegular);
        }
        Err(rustix::io::Errno::NOENT) => return Err(SecretFault::Missing),
        Err(error) => return Err(SecretFault::Io(error.into())),
    };
    let metadata = file.metadata().map_err(SecretFault::Io)?;
    if !metadata.is_file() {
        return Err(SecretFault::NotRegular);
    }
    let (owner, mode) = {
        use std::os::unix::fs::MetadataExt as _;
        (metadata.uid(), metadata.mode() & 0o7777)
    };
    if owner != rustix::process::geteuid().as_raw() || mode & 0o077 != 0 {
        return Err(SecretFault::Shared { owner, mode });
    }
    if metadata.len() > SECRET_BYTES.saturating_add(2) {
        return Err(SecretFault::TooLong {
            limit: SECRET_BYTES,
        });
    }
    let mut bytes = zeroize::Zeroizing::new(Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(0)
            .saturating_add(1),
    ));
    file.take(SECRET_BYTES.saturating_add(3))
        .read_to_end(&mut bytes)
        .map_err(SecretFault::Io)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| SecretFault::NotText)?;
    let text = text.strip_suffix('\n').unwrap_or(text);
    let text = text.strip_suffix('\r').unwrap_or(text);
    admit(text.len())?;
    Ok(Secret::new(text.to_owned()))
}

impl SecretResolver for FileSecrets {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            if reference.kind != SecretKind::File {
                return Err(SecretFault::Unsupported);
            }
            let path = std::path::PathBuf::from(&reference.name);
            let reading = tokio::task::spawn_blocking(move || read_private(&path));
            reading
                .await
                .map_err(|error| SecretFault::Io(std::io::Error::other(error)))?
        })
    }
}

/// The resolvers a provider uses: environment variables for `env`, private files for `file`,
/// and for `secret` the embedder's resolver, or `RDLT_SECRET_<NAME>` variables without one.
#[derive(Clone, Default)]
pub struct Secrets {
    env: EnvSecrets,
    named: Option<Arc<dyn SecretResolver>>,
}

impl fmt::Debug for Secrets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Secrets")
            .field("named", &self.named.is_some())
            .finish_non_exhaustive()
    }
}

impl Secrets {
    /// Environment variables and private files.
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolves `${secret:name}` with `resolver`.
    #[must_use]
    pub fn named(mut self, resolver: impl SecretResolver + 'static) -> Self {
        self.named = Some(Arc::new(resolver));
        self
    }

    /// Reads variables through `env`, in place of the host's environment.
    #[cfg(test)]
    pub(crate) fn reading(mut self, env: EnvSecrets) -> Self {
        self.env = env;
        self
    }
}

impl SecretResolver for Secrets {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        match (reference.kind, &self.named) {
            (SecretKind::File, _) => FileSecrets.resolve(reference),
            (SecretKind::Named, Some(named)) => named.resolve(reference),
            (SecretKind::Env | SecretKind::Named, _) => self.env.resolve(reference),
        }
    }
}
