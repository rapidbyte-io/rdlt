use std::sync::Arc;

use rdlt_testkit::tls::{Files, Pki};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection};

use super::{ALPN, Identity, TlsError, client_config, server_config};

fn identity(files: &Files) -> Identity {
    Identity {
        cert: files.cert.clone(),
        key: files.key.clone(),
    }
}

/// A handshake between `client`, connecting to `name`, and `server`, over memory: the
/// connections once it completes, or the first error either end meets.
fn handshake(
    client: ClientConfig,
    server: ServerConfig,
    name: &str,
) -> Result<(ClientConnection, ServerConnection), rustls::Error> {
    let name = ServerName::try_from(name.to_owned()).expect("a valid server name");
    let mut client = ClientConnection::new(Arc::new(client), name)?;
    let mut server = ServerConnection::new(Arc::new(server))?;
    for _ in 0..16 {
        if !client.is_handshaking() && !server.is_handshaking() {
            break;
        }
        let mut bytes = Vec::new();
        client.write_tls(&mut bytes).expect("writes to memory");
        server
            .read_tls(&mut bytes.as_slice())
            .expect("reads from memory");
        server.process_new_packets()?;
        let mut bytes = Vec::new();
        server.write_tls(&mut bytes).expect("writes to memory");
        client
            .read_tls(&mut bytes.as_slice())
            .expect("reads from memory");
        client.process_new_packets()?;
    }
    Ok((client, server))
}

#[test]
fn a_host_and_a_connector_of_one_ca_handshake_tls_1_3_for_http2() {
    let pki = Pki::new("ca");
    let server = server_config(&identity(&pki.server("server", &["localhost"])), &pki.ca())
        .expect("the server's configuration builds");
    let client = client_config(&identity(&pki.client("client")), &pki.ca())
        .expect("the client's configuration builds");
    let (client, server) = handshake(client, server, "localhost").expect("they handshake");
    assert_eq!(
        client.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
    assert_eq!(client.alpn_protocol(), Some(ALPN));
    assert!(
        server
            .peer_certificates()
            .is_some_and(|chain| !chain.is_empty())
    );
}

#[test]
fn a_host_whose_certificate_another_ca_issued_is_refused() {
    let (pki, other) = (Pki::new("ca"), Pki::new("other"));
    let server = server_config(&identity(&pki.server("server", &["localhost"])), &pki.ca())
        .expect("the server's configuration builds");
    let client = client_config(&identity(&other.client("client")), &pki.ca())
        .expect("the client's configuration builds");
    assert!(matches!(
        handshake(client, server, "localhost"),
        Err(rustls::Error::InvalidCertificate(_))
    ));
}

#[test]
fn a_host_without_a_certificate_is_refused() {
    let pki = Pki::new("ca");
    let server = server_config(&identity(&pki.server("server", &["localhost"])), &pki.ca())
        .expect("the server's configuration builds");
    let mut roots = rustls::RootCertStore::empty();
    for certificate in super::certificates(&pki.ca()).expect("the CA reads") {
        roots.add(certificate).expect("a valid anchor");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut anonymous = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .with_root_certificates(roots)
        .with_no_client_auth();
    anonymous.alpn_protocols = vec![ALPN.to_vec()];
    assert!(matches!(
        handshake(anonymous, server, "localhost"),
        Err(rustls::Error::NoCertificatesPresented)
    ));
}

#[test]
fn a_connector_whose_certificate_names_another_host_or_ca_is_refused() {
    let (pki, other) = (Pki::new("ca"), Pki::new("other"));
    let client = || {
        client_config(&identity(&pki.client("client")), &pki.ca())
            .expect("the client's configuration builds")
    };
    let elsewhere = server_config(&identity(&pki.server("server", &["elsewhere"])), &pki.ca())
        .expect("the server's configuration builds");
    assert!(matches!(
        handshake(client(), elsewhere, "localhost"),
        Err(rustls::Error::InvalidCertificate(_))
    ));
    let impostor = server_config(
        &identity(&other.server("server", &["localhost"])),
        &pki.ca(),
    )
    .expect("the server's configuration builds");
    assert!(matches!(
        handshake(client(), impostor, "localhost"),
        Err(rustls::Error::InvalidCertificate(_))
    ));
}

#[test]
fn unusable_files_are_refused_by_name() {
    let pki = Pki::new("ca");
    let server = identity(&pki.server("server", &["localhost"]));
    let missing = pki.dir().join("missing.pem");
    let empty = pki.dir().join("empty.pem");
    std::fs::write(&empty, "").expect("the file is written");
    let refused = |identity: &Identity, ca: &std::path::Path| {
        server_config(identity, ca).expect_err("refused")
    };
    assert!(matches!(refused(&server, &missing), TlsError::Pem { path, .. } if path == missing));
    assert!(matches!(refused(&server, &empty), TlsError::NoCertificate { path } if path == empty));
    let unkeyed = Identity {
        key: missing.clone(),
        ..server.clone()
    };
    assert!(matches!(refused(&unkeyed, &pki.ca()), TlsError::Pem { .. }));
    // A key that is not the certificate's.
    let mismatched = Identity {
        key: pki.client("client").key,
        ..server
    };
    assert!(matches!(
        refused(&mismatched, &pki.ca()),
        TlsError::Config(_)
    ));
    let client = identity(&pki.client("host"));
    assert!(matches!(
        client_config(&client, &empty),
        Err(TlsError::NoCertificate { .. })
    ));
}
