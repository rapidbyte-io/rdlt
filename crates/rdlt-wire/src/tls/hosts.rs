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

/// A list of accepted hosts that names none, or holds an empty name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a connector accepts the hosts named to it: at least one, and none with an empty name")]
pub struct NoHosts;

impl Hosts {
    /// The hosts `names` name.
    ///
    /// # Errors
    ///
    /// [`NoHosts`] when `names` is empty, or holds an empty name.
    pub fn new<N: Into<String>>(names: impl IntoIterator<Item = N>) -> Result<Self, NoHosts> {
        let names: BTreeSet<String> = names.into_iter().map(Into::into).collect();
        if names.is_empty() || names.iter().any(String::is_empty) {
            return Err(NoHosts);
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
