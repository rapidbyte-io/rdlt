//! The protocol's TLS for connectors reached over the network: TLS 1.3 alone, with mutual
//! authentication, on the `ring` provider, from certificates and keys in PEM files.
//!
//! Both ends take their configuration from here, so the policy is in one place: a host verifies
//! the connector's certificate against its CA bundle and the endpoint's name, and a connector
//! requires every host to present a certificate its own CA bundle issued.

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

/// The application protocol spoken over the TLS, and no other: HTTP/2, for gRPC.
pub const ALPN: &[u8] = b"h2";

/// A certificate chain and its private key, in PEM files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// The certificate chain, leaf first.
    pub cert: PathBuf,
    /// The private key.
    pub key: PathBuf,
}

/// Why a TLS configuration could not be built.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A PEM file could not be read or parsed.
    #[error("reading {} failed", path.display())]
    Pem {
        /// The file.
        path: PathBuf,
        /// Why.
        #[source]
        source: rustls::pki_types::pem::Error,
    },
    /// A PEM file holds no certificate.
    #[error("{} holds no certificate", path.display())]
    NoCertificate {
        /// The file.
        path: PathBuf,
    },
    /// A CA bundle's certificate is not a valid trust anchor.
    #[error("{} holds a certificate that is no valid trust anchor", path.display())]
    Anchor {
        /// The file.
        path: PathBuf,
        /// Why.
        #[source]
        source: rustls::Error,
    },
    /// The certificates and key do not make a configuration: a key that does not match its
    /// certificate, for one.
    #[error("the certificates and key do not make a TLS configuration")]
    Config(#[source] rustls::Error),
    /// The CA bundle cannot verify clients.
    #[error("the client CA bundle cannot verify clients")]
    Verifier(#[source] rustls::server::VerifierBuilderError),
}

/// The configuration a connector serves with: `identity` presented to every host, and a
/// certificate the CA bundle at `client_ca` issued required of every host.
///
/// # Errors
///
/// A [`TlsError`] when a file cannot be read, holds nothing usable, or the key does not match.
pub fn server_config(identity: &Identity, client_ca: &Path) -> Result<ServerConfig, TlsError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let roots = Arc::new(roots(client_ca)?);
    let verifier = WebPkiClientVerifier::builder_with_provider(roots, Arc::clone(&provider))
        .build()
        .map_err(TlsError::Verifier)?;
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(TlsError::Config)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificates(&identity.cert)?, key(&identity.key)?)
        .map_err(TlsError::Config)?;
    config.alpn_protocols = vec![ALPN.to_vec()];
    Ok(config)
}

/// The configuration a host connects with: `identity` presented to every connector, and every
/// connector's certificate verified against the CA bundle at `ca`.
///
/// # Errors
///
/// A [`TlsError`] when a file cannot be read, holds nothing usable, or the key does not match.
pub fn client_config(identity: &Identity, ca: &Path) -> Result<ClientConfig, TlsError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(TlsError::Config)?
        .with_root_certificates(roots(ca)?)
        .with_client_auth_cert(certificates(&identity.cert)?, key(&identity.key)?)
        .map_err(TlsError::Config)?;
    config.alpn_protocols = vec![ALPN.to_vec()];
    Ok(config)
}

/// Every certificate in the PEM file at `path`; at least one.
fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let pem = |source| TlsError::Pem {
        path: path.to_owned(),
        source,
    };
    let certificates = CertificateDer::pem_file_iter(path)
        .map_err(pem)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(pem)?;
    if certificates.is_empty() {
        return Err(TlsError::NoCertificate {
            path: path.to_owned(),
        });
    }
    Ok(certificates)
}

/// The private key in the PEM file at `path`.
fn key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    PrivateKeyDer::from_pem_file(path).map_err(|source| TlsError::Pem {
        path: path.to_owned(),
        source,
    })
}

/// The trust anchors of the CA bundle at `path`.
fn roots(path: &Path) -> Result<RootCertStore, TlsError> {
    let mut roots = RootCertStore::empty();
    for certificate in certificates(path)? {
        roots.add(certificate).map_err(|source| TlsError::Anchor {
            path: path.to_owned(),
            source,
        })?;
    }
    Ok(roots)
}
