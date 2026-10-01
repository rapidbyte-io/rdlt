use std::sync::Arc;

use rdlt_testkit::tls::{Files, Pki};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection};

use super::{ALPN, Accepted, Hosts, Identity, NoHosts, TlsError, client_config, server_config};

fn identity(files: &Files) -> Identity {
    Identity {
        cert: files.cert.clone(),
        key: files.key.clone(),
    }
}

/// The hosts `pki` issued a certificate to that a connector accepts: those named `hosts`.
fn accepted(pki: &Pki, hosts: &[&str]) -> Accepted {
    Accepted {
        ca: pki.ca(),
        hosts: Hosts::new(hosts.iter().copied()).expect("hosts are named"),
        crl: None,
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
    let server = server_config(
        &identity(&pki.server("server", &["localhost"])),
        &accepted(&pki, &["client"]),
    )
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
    let server = server_config(
        &identity(&pki.server("server", &["localhost"])),
        &accepted(&pki, &["client"]),
    )
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
    let server = server_config(
        &identity(&pki.server("server", &["localhost"])),
        &accepted(&pki, &["client"]),
    )
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
    let elsewhere = server_config(
        &identity(&pki.server("server", &["elsewhere"])),
        &accepted(&pki, &["client"]),
    )
    .expect("the server's configuration builds");
    assert!(matches!(
        handshake(client(), elsewhere, "localhost"),
        Err(rustls::Error::InvalidCertificate(_))
    ));
    let impostor = server_config(
        &identity(&other.server("server", &["localhost"])),
        &accepted(&pki, &["client"]),
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
        let accepted = Accepted {
            ca: ca.to_owned(),
            ..accepted(&pki, &["client"])
        };
        server_config(identity, &accepted).expect_err("refused")
    };
    assert!(matches!(refused(&server, &missing), TlsError::Pem { path, .. } if path == missing));
    assert!(matches!(refused(&server, &empty), TlsError::NoCertificate { path } if path == empty));
    let unkeyed = Identity {
        key: missing.clone(),
        ..server.clone()
    };
    assert!(matches!(refused(&unkeyed, &pki.ca()), TlsError::Key { path, .. } if path == missing));
    let unreadable = Identity {
        key: empty.clone(),
        ..server.clone()
    };
    std::fs::set_permissions(&empty, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .expect("chmod");
    assert!(matches!(refused(&unreadable, &pki.ca()), TlsError::Pem { path, .. } if path == empty));
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

/// As [`handshake`], between shared configurations, pumped until the connector's tickets, if
/// any, have reached the host.
fn connect(
    client: &Arc<ClientConfig>,
    server: &Arc<ServerConfig>,
) -> Result<(ClientConnection, ServerConnection), rustls::Error> {
    let name = ServerName::try_from("localhost").expect("a valid server name");
    let mut client = ClientConnection::new(Arc::clone(client), name)?;
    let mut server = ServerConnection::new(Arc::clone(server))?;
    for _ in 0..16 {
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
fn every_connection_is_a_full_handshake() {
    let pki = Pki::new("ca");
    let server = server_config(
        &identity(&pki.server("server", &["localhost"])),
        &accepted(&pki, &["client"]),
    )
    .expect("the server's configuration builds");
    let client = client_config(&identity(&pki.client("client")), &pki.ca())
        .expect("the client's configuration builds");
    let (client, server) = (Arc::new(client), Arc::new(server));
    for _ in 0..3 {
        let (host, connector) = connect(&client, &server).expect("they handshake");
        assert_eq!(host.handshake_kind(), Some(rustls::HandshakeKind::Full));
        assert_eq!(
            connector.handshake_kind(),
            Some(rustls::HandshakeKind::Full)
        );
    }
}

#[test]
fn a_key_file_others_can_read_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;
    let pki = Pki::new("ca");
    let server = identity(&pki.server("server", &["localhost"]));
    let client = identity(&pki.client("client"));
    for bit in [
        0o400, 0o200, 0o100, 0o040, 0o020, 0o010, 0o004, 0o002, 0o001,
    ] {
        let mode = 0o600 | bit;
        for key in [&server.key, &client.key] {
            std::fs::set_permissions(key, std::fs::Permissions::from_mode(mode)).expect("chmod");
        }
        let served = server_config(&server, &accepted(&pki, &["client"]));
        let dialed = client_config(&client, &pki.ca());
        if mode & 0o077 == 0 {
            assert!(served.is_ok() && dialed.is_ok(), "mode {mode:o}");
        } else {
            assert!(
                matches!(served, Err(TlsError::KeyMode { mode: found, .. }) if found == mode),
                "mode {mode:o}"
            );
            assert!(
                matches!(dialed, Err(TlsError::KeyMode { mode: found, .. }) if found == mode),
                "mode {mode:o}"
            );
        }
    }
}

#[test]
fn a_key_that_is_no_regular_file_of_the_user_is_refused() {
    let pki = Pki::new("ca");
    let server = identity(&pki.server("server", &["localhost"]));
    let directory = Identity {
        key: pki.dir().to_owned(),
        ..server
    };
    assert!(matches!(
        server_config(&directory, &accepted(&pki, &["client"])),
        Err(TlsError::KeyOwner { path }) if path == pki.dir()
    ));
    assert!(matches!(
        client_config(&directory, &pki.ca()),
        Err(TlsError::KeyOwner { .. })
    ));
}

#[test]
fn a_key_error_names_its_file_and_mode_and_nothing_of_the_key() {
    use std::os::unix::fs::PermissionsExt as _;
    let pki = Pki::new("ca");
    let client = identity(&pki.client("client"));
    std::fs::set_permissions(&client.key, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    let refused = client_config(&client, &pki.ca()).expect_err("refused");
    let key = std::fs::read_to_string(&client.key).expect("the key reads");
    let secret = key.lines().nth(1).expect("the key has a body");
    let said = format!("{refused} {refused:?}");
    assert!(!said.contains(secret));
    assert!(said.contains("644"));
}

#[test]
fn only_a_host_named_to_the_connector_is_accepted() {
    let pki = Pki::new("ca");
    let server = identity(&pki.server("server", &["localhost"]));
    let handshakes = |host: &Files, hosts: &[&str]| {
        let server = server_config(&server, &accepted(&pki, hosts)).expect("the server builds");
        let client = client_config(&identity(host), &pki.ca()).expect("the client builds");
        handshake(client, server, "localhost").map(|_| ())
    };
    let refused = Err(rustls::Error::InvalidCertificate(
        rustls::CertificateError::ApplicationVerificationFailure,
    ));
    let loader = pki.client("loader.example");
    assert_eq!(handshakes(&loader, &["loader.example"]), Ok(()));
    assert_eq!(handshakes(&loader, &["other", "LOADER.Example"]), Ok(()));
    assert_eq!(handshakes(&loader, &["other.example"]), refused);
    assert_eq!(handshakes(&loader, &["loader"]), refused);
    assert_eq!(handshakes(&loader, &["loader.example.org"]), refused);
    let spiffe = pki.client_uri("workload", "spiffe://example.org/loader");
    assert_eq!(
        handshakes(&spiffe, &["spiffe://example.org/loader"]),
        Ok(())
    );
    assert_eq!(
        handshakes(&spiffe, &["spiffe://example.org/Loader"]),
        refused
    );
    assert_eq!(handshakes(&spiffe, &["workload"]), refused);
    let unnamed = pki.client_unnamed("unnamed");
    assert_eq!(handshakes(&unnamed, &["unnamed"]), refused);
}

#[test]
fn a_list_of_hosts_names_at_least_one_and_none_emptily() {
    assert_eq!(Hosts::new(Vec::<String>::new()), Err(NoHosts));
    assert_eq!(Hosts::new(["host", ""]), Err(NoHosts));
    assert!(Hosts::new(["host"]).is_ok());
}

#[test]
fn the_listed_name_a_certificate_carries_is_its_hosts_identity() {
    let pki = Pki::new("ca");
    let hosts = Hosts::new(["b.example", "A.example", "spiffe://x/y"]).expect("hosts are named");
    let named = |files: &Files| {
        let chain = super::certificates(&files.cert).expect("the certificate reads");
        hosts.named(&chain[0]).map(str::to_owned)
    };
    assert_eq!(
        named(&pki.client("a.example")).as_deref(),
        Some("A.example")
    );
    assert_eq!(
        named(&pki.client("b.example")).as_deref(),
        Some("b.example")
    );
    assert_eq!(
        named(&pki.client_uri("w", "spiffe://x/y")).as_deref(),
        Some("spiffe://x/y")
    );
    assert_eq!(named(&pki.client("c.example")), None);
    let garbage = rustls::pki_types::CertificateDer::from(vec![0_u8; 8]);
    assert_eq!(hosts.named(&garbage), None);
}

#[test]
fn an_expired_host_certificate_is_refused() {
    let pki = Pki::new("ca");
    let server = server_config(
        &identity(&pki.server("server", &["localhost"])),
        &accepted(&pki, &["client"]),
    )
    .expect("the server's configuration builds");
    let client = client_config(&identity(&pki.client_expired("client")), &pki.ca())
        .expect("the client's configuration builds");
    assert!(matches!(
        handshake(client, server, "localhost"),
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::Expired | rustls::CertificateError::ExpiredContext { .. }
        ))
    ));
}

#[test]
fn a_revoked_host_is_refused_and_another_accepted() {
    let pki = Pki::new("ca");
    let (stolen, kept) = (pki.client("stolen"), pki.client("kept"));
    let server = identity(&pki.server("server", &["localhost"]));
    let handshakes = |host: &Files, crl: Option<std::path::PathBuf>| {
        let accepted = Accepted {
            crl,
            ..accepted(&pki, &["stolen", "kept"])
        };
        let server = server_config(&server, &accepted).expect("the server builds");
        let client = client_config(&identity(host), &pki.ca()).expect("the client builds");
        handshake(client, server, "localhost").map(|_| ())
    };
    let list = pki.revoking("revoked", &[&stolen]);
    assert_eq!(handshakes(&stolen, None), Ok(()));
    assert_eq!(
        handshakes(&stolen, Some(list.clone())),
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::Revoked
        ))
    );
    assert_eq!(handshakes(&kept, Some(list)), Ok(()));
    // A list past its next update says nothing current: every host is refused.
    let stale = pki.revoking_stale("stale", &[&stolen]);
    assert!(matches!(
        handshakes(&kept, Some(stale)),
        Err(rustls::Error::InvalidCertificate(_))
    ));
}

#[test]
fn a_revocation_list_file_without_a_list_is_refused_by_name() {
    let pki = Pki::new("ca");
    let server = identity(&pki.server("server", &["localhost"]));
    let empty = pki.dir().join("empty.crl");
    std::fs::write(&empty, "").expect("the file is written");
    let missing = pki.dir().join("missing.crl");
    let refused = |crl: &std::path::Path| {
        let accepted = Accepted {
            crl: Some(crl.to_owned()),
            ..accepted(&pki, &["client"])
        };
        server_config(&server, &accepted).expect_err("refused")
    };
    assert!(matches!(refused(&empty), TlsError::NoRevocationList { path } if path == empty));
    assert!(matches!(refused(&missing), TlsError::Pem { path, .. } if path == missing));
}
