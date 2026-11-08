//! The operator's host file: what a host allows, in the words `Host`'s builders use.

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Error, Host, codes};

/// What an operator allows a host, as a file says it: `connectors`, `trust_binaries`,
/// `writable`, `readable`, `always_readable`, `allow_env`, `allow_files`, `allow_network` and
/// `remote`.
///
/// ```
/// # async fn example() -> Result<(), rdlt::Error> {
/// let file = rdlt::HostFile::parse("connectors: [/opt/rdlt/bin]\nwritable: [/data]\n")?;
/// let host = file.host().await?;
/// # drop(host);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct HostFile {
    /// Directories local connectors are found in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connectors: Vec<PathBuf>,
    /// Whether local connectors run with the host's own access, in no sandbox.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub trust_binaries: bool,
    /// Roots a pipeline may let its connectors write beneath.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writable: Vec<PathBuf>,
    /// Roots a pipeline may let its connectors read beneath.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub readable: Vec<PathBuf>,
    /// Paths every sandboxed connector reads.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub always_readable: Vec<PathBuf>,
    /// Environment variables a configuration's `${env:NAME}` reaches.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_env: Vec<String>,
    /// Directories a configuration's `${file:/path}` reaches beneath.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_files: Vec<PathBuf>,
    /// Whether a pipeline may let its connectors share the host's network, in the sandbox too.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_network: bool,
    /// The host's TLS for remote connectors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<RemoteFiles>,
}

/// The host's certificate chain, key and CA bundle, as PEM files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RemoteFiles {
    /// The host's certificate chain.
    pub cert: PathBuf,
    /// The host's private key.
    pub key: PathBuf,
    /// The CA bundle remote connectors' certificates chain to.
    pub ca: PathBuf,
}

impl RemoteFiles {
    /// The certificate chain at `cert`, the key at `key` and the CA bundle at `ca`.
    pub fn new(cert: impl Into<PathBuf>, key: impl Into<PathBuf>, ca: impl Into<PathBuf>) -> Self {
        Self {
            cert: cert.into(),
            key: key.into(),
            ca: ca.into(),
        }
    }
}

impl HostFile {
    /// The host file `text` holds, read as a document is read; a text left empty or commented
    /// out allows nothing, as `HostFile::default()`.
    ///
    /// # Errors
    ///
    /// As [`Document::parse`](crate::Document::parse), saying "the host file".
    pub fn parse(text: &str) -> Result<Self, Error> {
        crate::document::hostile_read_or_default(text, "the host file")
    }

    /// The host the file allows.
    ///
    /// # Errors
    ///
    /// `host_setting_unavailable` for a setting of a feature this build lacks, never ignored;
    /// `tls_unreadable` for a TLS file that cannot be opened, is no regular file, or takes past
    /// [`BUILD_WAIT`](crate::limits::BUILD_WAIT) to open.
    pub async fn host(&self) -> Result<Host, Error> {
        self.refuse_unbuilt(cfg!(feature = "local"), cfg!(feature = "remote"))?;
        let host = Host::new()
            .allow_env(self.allow_env.iter().cloned())
            .allow_files(self.allow_files.iter().cloned());
        #[cfg(feature = "local")]
        let host = self.local(host);
        #[cfg(feature = "remote")]
        let host = match &self.remote {
            Some(remote) => {
                host.remote(crate::Tls::from_files(&remote.cert, &remote.key, &remote.ca).await?)
            }
            None => host,
        };
        Ok(host)
    }

    /// Refuses what the file sets that a build without `local` or `remote`, as these flags say
    /// this one is, cannot do: a setting is never silently ignored.
    fn refuse_unbuilt(&self, local: bool, remote: bool) -> Result<(), Error> {
        let needs = |setting: &str, feature: &str| {
            let message = format!(
                "the host file sets `{setting}`, which needs the facade's `{feature}` feature, \
                 which this build lacks"
            );
            Err(Error::config(codes::HOST_SETTING_UNAVAILABLE, message))
        };
        if !remote && self.remote.is_some() {
            return needs("remote", "remote");
        }
        if !local {
            let set = [
                ("connectors", !self.connectors.is_empty()),
                ("trust_binaries", self.trust_binaries),
                ("writable", !self.writable.is_empty()),
                ("readable", !self.readable.is_empty()),
                ("always_readable", !self.always_readable.is_empty()),
                ("allow_network", self.allow_network),
            ];
            if let Some((setting, _)) = set.into_iter().find(|(_, set)| *set) {
                return needs(setting, "local");
            }
        }
        Ok(())
    }

    /// `host`, placing local connectors as the file says.
    #[cfg(feature = "local")]
    fn local(&self, mut host: Host) -> Host {
        if self.trust_binaries {
            host = host.trust_binaries();
        }
        if self.allow_network {
            host = host.allow_network();
        }
        for dir in &self.connectors {
            host = host.connectors(dir);
        }
        for root in &self.writable {
            host = host.writable(root);
        }
        for root in &self.readable {
            host = host.readable(root);
        }
        for path in &self.always_readable {
            host = host.always_readable(path);
        }
        host
    }
}
