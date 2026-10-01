//! The PEM files a TLS configuration is built from: certificates, revocation lists, and a private
//! key that is its user's alone.

use std::fs::{File, Metadata, OpenOptions};
use std::io::BufReader;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::Path;

use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, CertificateRevocationListDer, PrivateKeyDer};

use super::TlsError;

/// The permission bits of a file's group and of others.
const NOT_THE_OWNERS: u32 = 0o077;

/// Opens the file at `path` to read, without waiting for it, and answers what the handle is of.
///
/// A pipe would otherwise hold the open until something wrote to it.
fn open(path: &Path) -> std::io::Result<(File, Metadata)> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::fcntl::OFlag::O_NONBLOCK.bits())
        .open(path)?;
    let metadata = file.metadata()?;
    Ok((file, metadata))
}

/// The PEM file at `path`, a regular file, to read.
fn pem(path: &Path) -> Result<BufReader<File>, TlsError> {
    let (file, metadata) = open(path).map_err(|error| TlsError::Pem {
        path: path.to_owned(),
        source: rustls::pki_types::pem::Error::Io(error),
    })?;
    if !metadata.is_file() {
        return Err(TlsError::NotAFile {
            path: path.to_owned(),
        });
    }
    Ok(BufReader::new(file))
}

/// Every certificate in the PEM file at `path`; at least one.
pub(super) fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certificates = CertificateDer::pem_reader_iter(pem(path)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::Pem {
            path: path.to_owned(),
            source,
        })?;
    if certificates.is_empty() {
        return Err(TlsError::NoCertificate {
            path: path.to_owned(),
        });
    }
    Ok(certificates)
}

/// Every revocation list in the PEM file at `path`; at least one.
pub(super) fn revocations(
    path: &Path,
) -> Result<Vec<CertificateRevocationListDer<'static>>, TlsError> {
    let lists = CertificateRevocationListDer::pem_reader_iter(pem(path)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsError::Pem {
            path: path.to_owned(),
            source,
        })?;
    if lists.is_empty() {
        return Err(TlsError::NoRevocationList {
            path: path.to_owned(),
        });
    }
    Ok(lists)
}

/// The private key in the PEM file at `path`, a regular file the user owns and no one else has
/// access to.
///
/// The file is examined and read through one handle, so what was examined is what is read.
pub(super) fn key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let opening = |source| TlsError::Key {
        path: path.to_owned(),
        source,
    };
    // Its kind and owner are examined before a byte of it is read.
    let (file, metadata) = open(path).map_err(opening)?;
    if !metadata.is_file() || metadata.uid() != nix::unistd::geteuid().as_raw() {
        return Err(TlsError::KeyOwner {
            path: path.to_owned(),
        });
    }
    let mode = metadata.permissions().mode() & 0o7777;
    if mode & NOT_THE_OWNERS != 0 {
        return Err(TlsError::KeyMode {
            path: path.to_owned(),
            mode,
        });
    }
    PrivateKeyDer::from_pem_reader(BufReader::new(file)).map_err(|source| TlsError::Pem {
        path: path.to_owned(),
        source,
    })
}

/// The trust anchors of the CA bundle at `path`.
pub(super) fn roots(path: &Path) -> Result<RootCertStore, TlsError> {
    let mut roots = RootCertStore::empty();
    for certificate in certificates(path)? {
        roots.add(certificate).map_err(|source| TlsError::Anchor {
            path: path.to_owned(),
            source,
        })?;
    }
    Ok(roots)
}
