use std::sync::Arc;

use rdlt_testkit::tls::{Files, Pki};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection};

use super::{
    ALPN, Accepted, Hosts, Identity, InvalidHosts, TlsError, client_config, server_config,
};

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
fn both_ends_agree_on_tls_1_3_with_a_post_quantum_key_exchange() {
    let pki = Pki::new("ca");
    let server = server_config(
        &identity(&pki.server("server", &["localhost"])),
        &accepted(&pki, &["client"]),
    )
    .expect("the server's configuration builds");
    let client = client_config(&identity(&pki.client("client")), &pki.ca())
        .expect("the client's configuration builds");
    let (client, server) = handshake(client, server, "localhost").expect("they handshake");
    let ends: [&rustls::CommonState; 2] = [&client, &server];
    for end in ends {
        assert_eq!(
            end.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_3)
        );
        assert_eq!(
            end.negotiated_cipher_suite().map(|suite| suite.suite()),
            Some(rustls::CipherSuite::TLS13_AES_256_GCM_SHA384)
        );
        assert_eq!(
            end.negotiated_key_exchange_group()
                .map(rustls::crypto::SupportedKxGroup::name),
            Some(rustls::NamedGroup::X25519MLKEM768)
        );
    }
}

/// The CA bundle of `pki`, as roots a client trusts.
fn trusted(pki: &Pki) -> rustls::RootCertStore {
    super::roots(&pki.ca()).expect("the CA reads")
}

/// A client presenting `files` that speaks `versions` with `provider`.
fn speaking(
    provider: rustls::crypto::CryptoProvider,
    versions: &[&'static rustls::SupportedProtocolVersion],
    pki: &Pki,
    files: &Files,
) -> ClientConfig {
    let mut config = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(versions)
        .expect("the versions are the provider's")
        .with_root_certificates(trusted(pki))
        .with_client_auth_cert(
            super::certificates(&files.cert).expect("the chain reads"),
            super::key(&files.key).expect("the key reads"),
        )
        .expect("the client's configuration builds");
    config.alpn_protocols = vec![ALPN.to_vec()];
    config
}

#[test]
fn neither_end_speaks_tls_1_2() {
    let pki = Pki::new("ca");
    let (server_files, client_files) = (pki.server("server", &["localhost"]), pki.client("client"));
    let server = server_config(&identity(&server_files), &accepted(&pki, &["client"]))
        .expect("the server's configuration builds");
    let old_host = speaking(
        Arc::unwrap_or_clone(super::provider()),
        &[&rustls::version::TLS12],
        &pki,
        &client_files,
    );
    assert!(matches!(
        handshake(old_host, server, "localhost"),
        Err(rustls::Error::PeerIncompatible(_))
    ));
    let old_connector = ServerConfig::builder_with_provider(super::provider())
        .with_protocol_versions(&[&rustls::version::TLS12])
        .expect("TLS 1.2")
        .with_no_client_auth()
        .with_single_cert(
            super::certificates(&server_files.cert).expect("the chain reads"),
            super::key(&server_files.key).expect("the key reads"),
        )
        .expect("the server's configuration builds");
    let host = client_config(&identity(&client_files), &pki.ca())
        .expect("the client's configuration builds");
    assert!(matches!(
        handshake(host, old_connector, "localhost"),
        Err(rustls::Error::PeerIncompatible(_))
    ));
}

#[test]
fn a_host_offering_only_a_suite_the_connector_lacks_is_refused() {
    let pki = Pki::new("ca");
    let server = server_config(
        &identity(&pki.server("server", &["localhost"])),
        &accepted(&pki, &["client"]),
    )
    .expect("the server's configuration builds");
    let mut provider = Arc::unwrap_or_clone(super::provider());
    let gcm = provider
        .cipher_suites
        .iter()
        .find_map(|suite| match suite {
            rustls::SupportedCipherSuite::Tls13(suite)
                if suite.common.suite == rustls::CipherSuite::TLS13_AES_128_GCM_SHA256 =>
            {
                Some(*suite)
            }
            _ => None,
        })
        .expect("the provider has AES-128-GCM");
    // A real TLS 1.3 suite that neither end implements, offered under that name alone.
    let ccm: &'static rustls::Tls13CipherSuite = Box::leak(Box::new(rustls::Tls13CipherSuite {
        common: rustls::crypto::CipherSuiteCommon {
            suite: rustls::CipherSuite::TLS13_AES_128_CCM_SHA256,
            ..gcm.common
        },
        ..*gcm
    }));
    provider.cipher_suites = vec![rustls::SupportedCipherSuite::Tls13(ccm)];
    let host = speaking(
        provider,
        &[&rustls::version::TLS13],
        &pki,
        &pki.client("client"),
    );
    assert!(matches!(
        handshake(host, server, "localhost"),
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::NoCipherSuitesInCommon
        ))
    ));
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
    let mut anonymous = ClientConfig::builder_with_provider(super::provider())
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
        let listening = server_config(&server, &accepted(&pki, &["client"]));
        let dialed = client_config(&client, &pki.ca());
        if bit >= 0o100 {
            assert!(listening.is_ok() && dialed.is_ok(), "mode {mode:o}");
        } else {
            assert!(
                matches!(listening, Err(TlsError::KeyMode { mode: found, .. }) if found == mode),
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
fn a_list_of_hosts_names_at_least_one_and_only_names_a_certificate_can_carry() {
    assert_eq!(Hosts::new(Vec::<String>::new()), Err(InvalidHosts::None));
    assert_eq!(Hosts::new(["host"]).map(|hosts| hosts.count()), Ok(1));
    assert_eq!(
        Hosts::new(["b", "a", "b"]).map(|hosts| hosts.count()),
        Ok(2)
    );
    let named = [
        "host",
        "loader.example",
        "LOADER-1.Example.ORG",
        "xn--bcher-kva.example",
        "a_b.example",
        "spiffe://example.org/loader",
        "urn:example:loader",
        "https://loader.example/path?query#fragment",
    ];
    for name in named {
        assert!(Hosts::new([name]).is_ok(), "{name}");
    }
    // What no certificate's name can equal names no host: it would be refused for ever, unseen.
    let never = [
        "",
        " ",
        "host ",
        " host",
        "host.",
        ".host",
        "a..b",
        "bücher.example",
        "*.example.com",
        "*",
        "192.0.2.7",
        "::1",
        "host:7443",
        "spiffe://example.org/a loader",
        "spiffe://bücher.example/loader",
        "://loader",
        "1scheme://loader",
        "line\nbreak",
    ];
    for name in never {
        let refused = Hosts::new(["host", name]);
        assert_eq!(
            refused,
            Err(InvalidHosts::Name(name.to_owned())),
            "{name:?}"
        );
    }
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

#[test]
fn naming_hosts_changes_nothing_else_of_how_a_certificate_is_verified() {
    use rustls::server::WebPkiClientVerifier;
    use rustls::server::danger::ClientCertVerifier as _;
    let pki = Pki::new("ca");
    let roots = Arc::new(super::roots(&pki.ca()).expect("the CA reads"));
    let inner = WebPkiClientVerifier::builder_with_provider(roots, super::provider())
        .build()
        .expect("a verifier");
    let hosts = Hosts::new(["client"]).expect("a host is named");
    let named = super::hosts::Named::new(Arc::clone(&inner), hosts);
    assert!(!inner.root_hint_subjects().is_empty());
    let subjects = |verifier: &dyn rustls::server::danger::ClientCertVerifier| -> Vec<Vec<u8>> {
        let hints = verifier.root_hint_subjects();
        hints
            .iter()
            .map(|subject| subject.as_ref().to_vec())
            .collect()
    };
    assert_eq!(subjects(&named), subjects(inner.as_ref()));
    assert!(!inner.supported_verify_schemes().is_empty());
    assert_eq!(
        named.supported_verify_schemes(),
        inner.supported_verify_schemes()
    );
    assert!(named.offer_client_auth() && named.client_auth_mandatory());
}

#[test]
fn a_file_that_is_no_regular_file_is_refused_without_waiting_for_it() {
    let pki = Pki::new("ca");
    let server = identity(&pki.server("server", &["localhost"]));
    // A pipe nothing writes to: opening it to read would wait for a writer for ever.
    let pipe = pki.dir().join("pipe");
    nix::unistd::mkfifo(
        &pipe,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .expect("a pipe is made");
    let (answered, answers) = std::sync::mpsc::channel();
    let (accepted, key_in_pipe) = (accepted(&pki, &["client"]), pipe.clone());
    std::thread::spawn(move || {
        let keyed = Identity {
            key: key_in_pipe.clone(),
            ..server.clone()
        };
        let certified = Identity {
            cert: key_in_pipe.clone(),
            ..server.clone()
        };
        let trusting = Accepted {
            ca: key_in_pipe.clone(),
            ..accepted.clone()
        };
        let revoking = Accepted {
            crl: Some(key_in_pipe),
            ..accepted.clone()
        };
        let refused = [
            server_config(&keyed, &accepted).err(),
            server_config(&certified, &accepted).err(),
            server_config(&server, &trusting).err(),
            server_config(&server, &revoking).err(),
            client_config(&keyed, &accepted.ca).err(),
        ];
        answered.send(refused).ok();
    });
    let refused = answers
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("each configuration is refused, not waited for");
    let [key, certificate, ca, crl, client] = refused;
    assert!(matches!(key, Some(TlsError::KeyOwner { path }) if path == pipe));
    assert!(matches!(client, Some(TlsError::KeyOwner { path }) if path == pipe));
    for other in [certificate, ca, crl] {
        assert!(
            matches!(other, Some(TlsError::NotAFile { ref path }) if *path == pipe),
            "{other:?}"
        );
    }
}

#[test]
fn naming_hosts_demands_a_certificate_though_what_it_wraps_would_take_none() {
    use rustls::server::WebPkiClientVerifier;
    use rustls::server::danger::ClientCertVerifier as _;
    let pki = Pki::new("ca");
    let roots = Arc::new(super::roots(&pki.ca()).expect("the CA reads"));
    let lenient = WebPkiClientVerifier::builder_with_provider(roots, super::provider())
        .allow_unauthenticated()
        .build()
        .expect("a verifier");
    assert!(!lenient.client_auth_mandatory());
    let hosts = Hosts::new(["client"]).expect("a host is named");
    let named = super::hosts::Named::new(lenient, hosts);
    assert!(named.offer_client_auth());
    assert!(named.client_auth_mandatory());
}
