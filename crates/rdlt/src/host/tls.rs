//! The host's identity and CA for connectors reached over mutual TLS.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use rdlt_host::{Identity, Remote};
use rustix::fs::{Mode, OFlags};

use crate::limits::BUILD_WAIT;
use crate::{Error, codes};

/// The host's certificate chain and key, and the CA bundle its remote connectors' certificates
/// chain to, each a PEM file read when the host dials.
///
/// ```no_run
/// # async fn example() -> Result<(), rdlt::Error> {
/// let tls = rdlt::Tls::from_files("/etc/rdlt/host.pem", "/etc/rdlt/host.key", "/etc/rdlt/ca.pem")
///     .await?;
/// let host = rdlt::Host::new().remote(tls);
/// # drop(host);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tls {
    identity: Identity,
    ca: PathBuf,
}

impl Tls {
    /// The files at `cert`, `key` and `ca`, each opened now so a missing one fails here, on a
    /// blocking task so that a stalled path holds up no runtime worker.
    ///
    /// # Errors
    ///
    /// `tls_unreadable`, naming the file that cannot be opened or is no regular file; or saying
    /// that opening them took longer than [`BUILD_WAIT`].
    pub async fn from_files(
        cert: impl Into<PathBuf>,
        key: impl Into<PathBuf>,
        ca: impl Into<PathBuf>,
    ) -> Result<Self, Error> {
        let (cert, key, ca) = (cert.into(), key.into(), ca.into());
        let paths = [cert.clone(), key.clone(), ca.clone()];
        let opening =
            tokio::task::spawn_blocking(move || paths.iter().try_for_each(|path| readable(path)));
        let unopened = |cause: io::Error| {
            Error::config_caused(codes::TLS_UNREADABLE, "opening the TLS files", cause)
        };
        match tokio::time::timeout(BUILD_WAIT, opening).await {
            Ok(Ok(opened)) => opened?,
            Ok(Err(joined)) => return Err(unopened(io::Error::other(joined))),
            Err(_) => {
                let late = format!("not opened within {BUILD_WAIT:?}");
                return Err(unopened(io::Error::new(io::ErrorKind::TimedOut, late)));
            }
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

/// Opens the regular file at `path`: a pipe or a terminal is opened without waiting and refused
/// on what was opened, so nothing can hold this up, and no name is checked apart from its file.
fn readable(path: &Path) -> Result<(), Error> {
    let unread = |error: io::Error| {
        let message = format!("reading {}", path.display());
        Error::config_caused(codes::TLS_UNREADABLE, message, error)
    };
    let flags = OFlags::RDONLY
        .union(OFlags::NONBLOCK)
        .union(OFlags::NOCTTY)
        .union(OFlags::CLOEXEC);
    let opened = rustix::fs::open(path, flags, Mode::empty());
    let file = File::from(opened.map_err(|error| unread(error.into()))?);
    if file.metadata().map_err(unread)?.is_file() {
        return Ok(());
    }
    let other = io::Error::new(io::ErrorKind::InvalidInput, "it is no regular file");
    Err(unread(other))
}
