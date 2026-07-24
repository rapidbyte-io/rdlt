//! Quarantined certificate verifiers for the deliberately-weaker libpq
//! levels. SAFE Rust throughout — "dangerous" is rustls's API vocabulary for
//! custom verification, not `unsafe`.
//!
//! - [`AcceptAnyCert`]: `require`/`prefer` — encrypt, validate NOTHING.
//!   Exactly libpq's `sslmode=require`. Never a default here; the policy
//!   layer only builds it for those modes, and the README says `verify_full`
//!   is what production should use.
//! - [`ChainOnly`]: `verify_ca` — full webpki chain verification, hostname
//!   check waived (ONLY the name-mismatch error is forgiven).

use std::sync::Arc;

use rustls::DigitallySignedStruct;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

/// The one crypto provider this crate uses (ring): chosen EXPLICITLY at
/// every construction site — never the ambient process default, which is
/// ambiguous when multiple provider features land in the dependency tree.
pub(crate) fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

#[derive(Debug)]
pub(crate) struct AcceptAnyCert {
    provider: Arc<CryptoProvider>,
}

impl AcceptAnyCert {
    pub fn new() -> Self {
        Self {
            provider: provider(),
        }
    }
}

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
pub(crate) struct ChainOnly {
    inner: Arc<WebPkiServerVerifier>,
}

impl ChainOnly {
    pub fn new(roots: rustls::RootCertStore) -> Result<Self, rustls::client::VerifierBuilderError> {
        Ok(Self {
            inner: WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider())
                .build()?,
        })
    }
}

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            // verify_ca: chain must hold; ONLY hostname mismatch is waived.
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
