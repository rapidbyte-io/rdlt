//! A log store's configuration, and what it is checked for before anything is asked of a store.

use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use rdlt_engine::{Clock, LocalWal, WalStore};
use rdlt_host::{SecretReference, SecretResolver};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::LogStoreError;
use crate::limits::{PART_BYTES_LEAST, PART_BYTES_MOST};

/// Where a pipeline's write-ahead logs are kept.
///
/// As JSON, `{"local": {"base": "/var/lib/rdlt/logs"}}` or `{"s3": {...}}` with the fields of
/// [`S3Config`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LogStoreConfig {
    /// In a directory of the local file system, the engine's user's alone.
    Local {
        /// The directory, made where it is missing.
        base: PathBuf,
    },
    /// In an S3 bucket, or a store that answers as S3 does.
    S3(S3Config),
}

/// Logs in an S3 bucket beneath a prefix, reached as its credentials say.
///
/// Each credential is one secret reference, `${env:NAME}`, `${file:/path}` or `${secret:name}`,
/// which only the resolver the operator gives may resolve; a credential written in the
/// configuration itself is refused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    /// The bucket: 3 to 63 lower-case letters, digits, dots and hyphens, beginning and ending
    /// with a letter or digit.
    pub bucket: String,
    /// What the logs' keys begin with: segments of `[A-Za-z0-9._-]` joined by `/`.
    pub prefix: String,
    /// The bucket's region, as `eu-west-1`.
    pub region: String,
    /// The store's address, where it is not AWS's: `https://host[:port]`, or `http://` to a
    /// loopback address given as an IP address, as a store on the same machine is reached.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Whether the bucket is named in the request's path, as stores other than AWS's often
    /// ask, rather than in its host.
    #[serde(default)]
    pub path_style: bool,
    /// A reference to the access key's id.
    pub access_key_id: String,
    /// A reference to the access key's secret.
    pub secret_access_key: String,
    /// A reference to a session token, for temporary credentials.
    #[serde(default)]
    pub session_token: Option<String>,
    /// Bytes: the length of a part of a chunk uploaded in parts, from 5 MiB to 5 GiB; 8 MiB
    /// where none is given.
    #[serde(default)]
    pub part_bytes: Option<u64>,
}

/// An S3 configuration once checked.
#[derive(Debug)]
pub(crate) struct Checked {
    pub(crate) endpoint: Option<Endpoint>,
    pub(crate) credentials: References,
    pub(crate) part_bytes: Option<NonZeroUsize>,
}

/// The store's address, and whether it is reached without TLS.
#[derive(Debug)]
pub(crate) struct Endpoint {
    pub(crate) url: String,
    pub(crate) plaintext: bool,
}

/// The references a store's credentials are resolved from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct References {
    pub(crate) key_id: SecretReference,
    pub(crate) secret_key: SecretReference,
    pub(crate) token: Option<SecretReference>,
}

impl LogStoreConfig {
    /// The configuration `document` holds.
    ///
    /// # Errors
    ///
    /// `config_invalid` for a document that is not a log store's configuration, naming the
    /// field at fault and never what it holds.
    pub fn parse(document: &serde_json::Value) -> Result<Self, LogStoreError> {
        rdlt_connector::parse_config(document).map_err(LogStoreError::Document)
    }

    /// The store the configuration names, opened: a bucket's once a probe found it does what a
    /// log needs, its credentials resolved through `secrets` and its requests tried on `clock`.
    ///
    /// # Errors
    ///
    /// A [`LogStoreError`] naming the field at fault, the secret that did not resolve, or what
    /// the store refused or failed at.
    pub async fn open(
        &self,
        secrets: Arc<dyn SecretResolver>,
        clock: Arc<dyn Clock>,
    ) -> Result<Arc<dyn WalStore>, LogStoreError> {
        match self {
            Self::Local { base } => Ok(Arc::new(LocalWal::new(base))),
            Self::S3(config) => Ok(Arc::new(crate::s3::open(config, secrets, clock).await?)),
        }
    }
}

fn refused(field: &'static str, why: &'static str) -> LogStoreError {
    LogStoreError::Config { field, why }
}

impl S3Config {
    /// The configuration checked: its bucket, region, endpoint, credentials and part length.
    pub(crate) fn checked(&self) -> Result<Checked, LogStoreError> {
        bucket(&self.bucket)?;
        let plain = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
        if self.region.is_empty() || self.region.len() > 64 || !self.region.chars().all(plain) {
            return Err(refused("region", "is not a region's name"));
        }
        let endpoint = self.endpoint.as_deref().map(endpoint).transpose()?;
        let reference = |field, text: &str| {
            SecretReference::parse(text).map_err(|_| refused(field, "is not one secret reference"))
        };
        let credentials = References {
            key_id: reference("access_key_id", &self.access_key_id)?,
            secret_key: reference("secret_access_key", &self.secret_access_key)?,
            token: self
                .session_token
                .as_deref()
                .map(|token| reference("session_token", token))
                .transpose()?,
        };
        let part_bytes = match self.part_bytes {
            None => None,
            Some(bytes) if (PART_BYTES_LEAST..=PART_BYTES_MOST).contains(&bytes) => {
                usize::try_from(bytes).ok().and_then(NonZeroUsize::new)
            }
            Some(_) => return Err(refused("part_bytes", "is not from 5 MiB to 5 GiB")),
        };
        Ok(Checked {
            endpoint,
            credentials,
            part_bytes,
        })
    }
}

/// Refuses a bucket name S3 would not take, or one read as an address.
fn bucket(name: &str) -> Result<(), LogStoreError> {
    let allowed = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-';
    let edge = |c: Option<char>| c.is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let named = (3..=63).contains(&name.len())
        && name.chars().all(allowed)
        && edge(name.chars().next())
        && edge(name.chars().last())
        && !name.contains("..")
        && name.parse::<IpAddr>().is_err();
    if named {
        Ok(())
    } else {
        Err(refused("bucket", "is not a bucket's name"))
    }
}

/// The endpoint `text` names: an `https` address with no user, path, query or fragment, or an
/// `http` one to a loopback IP address.
fn endpoint(text: &str) -> Result<Endpoint, LogStoreError> {
    let url = Url::parse(text).map_err(|_| refused("endpoint", "is not an address"))?;
    let bare = url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
        && url.path() == "/";
    if !bare {
        return Err(refused("endpoint", "names a user, path, query or fragment"));
    }
    let loopback = match url.host() {
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        _ => false,
    };
    let plaintext = match url.scheme() {
        "https" => false,
        "http" if loopback => true,
        "http" => {
            return Err(refused(
                "endpoint",
                "is reached without TLS where it is not a loopback IP address",
            ));
        }
        _ => return Err(refused("endpoint", "is neither https nor http")),
    };
    Ok(Endpoint {
        url: text.trim_end_matches('/').to_owned(),
        plaintext,
    })
}

#[cfg(test)]
mod tests;
