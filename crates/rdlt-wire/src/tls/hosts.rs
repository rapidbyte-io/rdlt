//! The hosts a listening connector accepts: an allowlist of the names their certificates carry.

use std::collections::BTreeSet;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{CertificateError, DigitallySignedStruct, DistinguishedName, SignatureScheme};

/// The names of the hosts a connector accepts, at least one.
///
/// A certificate names a host in its subject alternative names: a DNS name, compared without
/// regard to ASCII case, or a URI, compared exactly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hosts(Arc<BTreeSet<String>>);

/// A list of accepted hosts that names none, or holds a name no certificate can carry.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum InvalidHosts {
    /// No host is named.
    #[error("a connector accepts the hosts named to it, and none is named")]
    None,
    /// A name is neither a DNS name nor a URI, so no certificate names a host by it.
    #[error(
        "`{0}` names no host: a host is named by a DNS name, an international one in its \
         `xn--` form and none with a wildcard or a final dot, or by a URI"
    )]
    Name(String),
}

impl Hosts {
    /// The hosts `names` name.
    ///
    /// # Errors
    ///
    /// [`InvalidHosts`] when `names` is empty, or holds a name no certificate can carry.
    pub fn new<N: Into<String>>(names: impl IntoIterator<Item = N>) -> Result<Self, InvalidHosts> {
        let names: BTreeSet<String> = names.into_iter().map(Into::into).collect();
        if names.is_empty() {
            return Err(InvalidHosts::None);
        }
        if let Some(name) = names.iter().find(|name| !nameable(name)) {
            return Err(InvalidHosts::Name(name.clone()));
        }
        Ok(Self(Arc::new(names)))
    }

    /// How many hosts are named: one at least.
    pub fn count(&self) -> usize {
        self.0.len()
    }

    /// The listed name `certificate` carries, the first in the list's order; none where it
    /// carries none of them, or cannot be read.
    pub fn named(&self, certificate: &CertificateDer<'_>) -> Option<&str> {
        let certificate = webpki::EndEntityCert::try_from(certificate).ok()?;
        let dns: Vec<&str> = certificate.valid_dns_names().collect();
        let uris: Vec<&str> = certificate.valid_uri_names().collect();
        self.0
            .iter()
            .find(|listed| {
                dns.iter().any(|name| name.eq_ignore_ascii_case(listed))
                    || uris.iter().any(|uri| uri == listed)
            })
            .map(String::as_str)
    }
}

/// Whether a certificate can name a host `name`: by a DNS name, or by a URI.
fn nameable(name: &str) -> bool {
    dns(name) || uri(name)
}

/// Whether `name` is a DNS name as a certificate carries one: labels of letters, digits, hyphens
/// and underscores, no wildcard, no final dot, and no IP address.
fn dns(name: &str) -> bool {
    let labelled = name.split('.').all(|label| {
        !label.is_empty()
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    });
    labelled
        && name.parse::<std::net::IpAddr>().is_err()
        && rustls::pki_types::DnsName::try_from(name).is_ok()
}

/// Whether `name` is a URI: a scheme, a colon, and printable ASCII without a space after it.
fn uri(name: &str) -> bool {
    let Some((scheme, rest)) = name.split_once(':') else {
        return false;
    };
    let schemed = scheme.starts_with(|first: char| first.is_ascii_alphabetic())
        && scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'));
    // A host and a port is no URI, though it has a colon.
    let ported = !rest.is_empty() && rest.bytes().all(|byte| byte.is_ascii_digit());
    schemed && !rest.is_empty() && !ported && rest.bytes().all(|byte| byte.is_ascii_graphic())
}

/// `inner`, which then accepts only a certificate that names one of `hosts`.
#[derive(Debug)]
pub(super) struct Named {
    inner: Arc<dyn ClientCertVerifier>,
    hosts: Hosts,
}

impl Named {
    pub(super) fn new(inner: Arc<dyn ClientCertVerifier>, hosts: Hosts) -> Self {
        Self { inner, hosts }
    }
}

impl ClientCertVerifier for Named {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        if self.hosts.named(end_entity).is_none() {
            return Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner
            .verify_tls12_signature(message, certificate, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner
            .verify_tls13_signature(message, certificate, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
