//! A listening connector demands a certificate of every host: one that offers none is refused
//! at the handshake, over a real connection.

use std::sync::Arc;
use std::time::Duration;

use rdlt_testkit::tls::Pki;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::network::{listening, stop};

/// A client's configuration that trusts `pki`'s CA and offers no certificate of its own.
fn anonymous(pki: &Pki) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(pki.ca()).expect("the CA reads") {
        roots
            .add(certificate.expect("a certificate"))
            .expect("a valid anchor");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    config
}

#[tokio::test]
async fn a_host_that_offers_no_certificate_is_refused_at_the_handshake() {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["localhost"]);
    let (connector, address) = listening(&pki, &server, "127.0.0.1:0").await;
    let stream = tokio::net::TcpStream::connect(&address)
        .await
        .expect("the connector accepts");
    let dialing = tokio_rustls::TlsConnector::from(Arc::new(anonymous(&pki)));
    let name = ServerName::try_from("localhost").expect("a server name");
    // The host's side of a TLS 1.3 handshake ends before the connector has judged it: what
    // the connector then answers is its refusal, and nothing of HTTP/2.
    let answered = async {
        let mut tls = dialing.connect(name, stream).await?;
        tls.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await?;
        let mut answer = Vec::new();
        tls.read_to_end(&mut answer).await?;
        Ok::<_, std::io::Error>(answer)
    };
    let answered = tokio::time::timeout(Duration::from_secs(60), answered).await;
    let refused = answered
        .expect("the connector answers")
        .expect_err("a host without a certificate is refused");
    let alert = refused.get_ref().and_then(|inner| inner.downcast_ref());
    assert!(
        matches!(
            alert,
            Some(rustls::Error::AlertReceived(
                rustls::AlertDescription::CertificateRequired
            ))
        ),
        "{refused:?}"
    );
    stop(&connector);
}
