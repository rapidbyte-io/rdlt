//! The host's identity and CA for connectors reached over mutual TLS.

use std::fs::File;
use std::path::{Path, PathBuf};

use rdlt_host::{Identity, Remote};

use crate::{Error, codes};

/// The host's certificate chain and key, and the CA bundle its remote connectors' certificates
/// chain to, each a PEM file read when the host dials.
///
/// ```no_run
/// let tls = rdlt::Tls::from_files("/etc/rdlt/host.pem", "/etc/rdlt/host.key", "/etc/rdlt/ca.pem")?;
/// let host = rdlt::Host::new().remote(tls);
/// # drop(host);
/// # Ok::<(), rdlt::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tls {
    identity: Identity,
    ca: PathBuf,
}

impl Tls {
    /// The files at `cert`, `key` and `ca`, each opened now so a missing one fails here.
    ///
    /// # Errors
    ///
    /// `tls_unreadable`, naming the file that cannot be opened.
    pub fn from_files(
        cert: impl Into<PathBuf>,
        key: impl Into<PathBuf>,
        ca: impl Into<PathBuf>,
    ) -> Result<Self, Error> {
        let (cert, key, ca) = (cert.into(), key.into(), ca.into());
        for path in [&cert, &key, &ca] {
            readable(path)?;
        }
        Ok(Self {
            identity: Identity { cert, key },
            ca,
        })
    }

    /// The remote placement this identity and CA dial with.
    pub(crate) fn remote(&self) -> Remote {
        Remote::new(self.identity.clone(), self.ca.clone())
    }
}

fn readable(path: &Path) -> Result<(), Error> {
    File::open(path).map(drop).map_err(|error| {
        let message = format!("reading {}", path.display());
        Error::config_caused(codes::TLS_UNREADABLE, message, error)
    })
}
