//! The protocol's TLS for connectors reached over the network: TLS 1.3 alone, with mutual
//! authentication, on rustls's aws-lc-rs provider, from certificates and keys in PEM files.
//!
//! Both ends take their configuration from here, so the policy is in one place: a host verifies
//! the connector's certificate against its CA bundle and the endpoint's name, and a connector
//! requires every host to present a certificate its own CA bundle issued that names a host the
//! connector was told to accept. Neither end resumes a session.

mod files;
mod hosts;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::Arc;

use rustls::client::Resumption;
use rustls::crypto::CryptoProvider;
use rustls::server::{NoServerSessionStorage, WebPkiClientVerifier};
use rustls::{ClientConfig, ServerConfig};

pub use hosts::{Hosts, InvalidHosts};

use files::{certificates, key, revocations, roots};
use hosts::Named;

/// The application protocol spoken over the TLS, and no other: HTTP/2, for gRPC.
pub const ALPN: &[u8] = b"h2";

/// The cryptography of every TLS connection: rustls's aws-lc-rs provider, with its cipher suites,
/// key exchange groups and signature schemes, a post-quantum hybrid exchange preferred.
#[must_use]
pub fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// A certificate chain and its private key, in PEM files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// The certificate chain, leaf first.
    pub cert: PathBuf,
    /// The private key, a file of the user's alone: one its group or others can read, or another
    /// user owns, is refused.
    pub key: PathBuf,
}

/// The hosts a listening connector accepts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Accepted {
    /// The CA bundle every host's certificate must come from.
    pub ca: PathBuf,
    /// The hosts' names: a certificate is accepted where it names one of them.
    pub hosts: Hosts,
    /// The revocation lists of the CA bundle's authorities, in one PEM file; none checks no
    /// revocation.
    pub crl: Option<PathBuf>,
}

/// Why a TLS configuration could not be built.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
    /// A file named for certificates or revocation lists is no regular file.
    #[error("{} is not a regular file", path.display())]
    NotAFile {
        /// The file.
        path: PathBuf,
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
    /// A private key file could not be opened or examined.
    #[error("opening the private key {} failed", path.display())]
    Key {
        /// The file.
        path: PathBuf,
        /// Why.
        #[source]
        source: std::io::Error,
    },
    /// A private key file's group or others have access to it.
    #[error(
        "the private key {} has mode {mode:03o}: its group or others have access to it",
        path.display()
    )]
    KeyMode {
        /// The file.
        path: PathBuf,
        /// Its permission bits.
        mode: u32,
    },
    /// A private key file is not a regular file the user owns.
    #[error("the private key {} is not a regular file this user owns", path.display())]
    KeyOwner {
        /// The file.
        path: PathBuf,
    },
    /// A revocation list file holds no list.
    #[error("{} holds no revocation list", path.display())]
    NoRevocationList {
        /// The file.
        path: PathBuf,
    },
    /// The certificates and key do not make a configuration: a key that does not match its
    /// certificate, for one.
    #[error("the certificates and key do not make a TLS configuration")]
    Config(#[source] rustls::Error),
    /// The CA bundle cannot verify clients.
    #[error("the client CA bundle cannot verify clients")]
    Verifier(#[source] rustls::server::VerifierBuilderError),
}

/// The configuration a connector serves with: `identity` presented to every host, and of every
/// host a certificate that `accepted`'s CA bundle issued, that is valid now, that no revocation
/// list of `accepted` revokes, and that names a host `accepted` lists.
///
/// Every connection is a full handshake: no session is resumed, so a certificate is checked each
/// time it is presented. With revocation lists, every certificate of a host's chain needs a
/// current list of its issuer.
///
/// # Errors
///
/// A [`TlsError`] when a file cannot be read, holds nothing usable, the key file is not the
/// user's alone, or the key does not match.
pub fn server_config(identity: &Identity, accepted: &Accepted) -> Result<ServerConfig, TlsError> {
    let provider = provider();
    let roots = Arc::new(roots(&accepted.ca)?);
    let mut verifier = WebPkiClientVerifier::builder_with_provider(roots, Arc::clone(&provider));
    if let Some(crl) = &accepted.crl {
        verifier = verifier
            .with_crls(revocations(crl)?)
            .enforce_revocation_expiration();
    }
    let verifier = verifier.build().map_err(TlsError::Verifier)?;
    let verifier = Arc::new(Named::new(verifier, accepted.hosts.clone()));
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(TlsError::Config)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificates(&identity.cert)?, key(&identity.key)?)
        .map_err(TlsError::Config)?;
    config.alpn_protocols = vec![ALPN.to_vec()];
    // A resumed session restores the host's certificate without verifying it again.
    config.session_storage = Arc::new(NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    Ok(config)
}

/// The configuration a host connects with: `identity` presented to every connector, and every
/// connector's certificate verified against the CA bundle at `ca`, in a full handshake each time.
///
/// # Errors
///
/// A [`TlsError`] when a file cannot be read, holds nothing usable, the key file is not the
/// user's alone, or the key does not match.
pub fn client_config(identity: &Identity, ca: &std::path::Path) -> Result<ClientConfig, TlsError> {
    let mut config = ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(TlsError::Config)?
        .with_root_certificates(roots(ca)?)
        .with_client_auth_cert(certificates(&identity.cert)?, key(&identity.key)?)
        .map_err(TlsError::Config)?;
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.resumption = Resumption::disabled();
    Ok(config)
}
