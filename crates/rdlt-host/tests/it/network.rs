//! Connectors listening on the network, reached over mutual TLS: placed at their endpoints,
//! refused when either end's certificate does not hold.

use std::process::Stdio;
use std::time::Duration;

use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Identity, Placement, Provider as _, ProviderError, Remote};
use rdlt_testkit::tls::{Files, Pki};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::{Child, Command};

use crate::process::example;

pub(crate) fn identity(files: &Files) -> Identity {
    Identity {
        cert: files.cert.clone(),
        key: files.key.clone(),
    }
}

/// The hosts a connector of `pki` accepts in these tests: a single host, named `host`.
pub(crate) fn accepted(pki: &Pki) -> rdlt_wire::tls::Accepted {
    rdlt_wire::tls::Accepted {
        ca: pki.ca(),
        hosts: rdlt_wire::tls::Hosts::new(["host"]).expect("a host is named"),
        crl: None,
    }
}

/// The scripted connector, listening at `address` over mutual TLS with `server`'s certificate
/// and `pki`'s CA; its process, and the address it announced.
pub(crate) async fn listening(pki: &Pki, server: &Files, address: &str) -> (Child, String) {
    let mut child = Command::new(example("scripted_connector"))
        .args(["--listen", address])
        .arg("--tls-cert")
        .arg(&server.cert)
        .arg("--tls-key")
        .arg(&server.key)
        .arg("--tls-client-ca")
        .arg(pki.ca())
        .args(["--tls-allow-host", "host"])
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("the connector starts");
    let stdout = child.stdout.take().expect("its output is piped");
    let line = BufReader::new(stdout)
        .lines()
        .next_line()
        .await
        .expect("its output reads")
        .expect("it announces its address");
    let address = line
        .strip_prefix("listening on ")
        .expect("the announcement names the address")
        .to_owned();
    (child, address)
}

pub(crate) fn scripted(endpoint: &str) -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse("test.scripted").expect("a valid id")).endpoint(endpoint)
}

pub(crate) fn port(address: &str) -> u16 {
    address
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .expect("an address and port")
}

#[tokio::test]
async fn a_connector_listening_over_mutual_tls_is_placed_at_its_endpoint() {
    let pki = Pki::new("ca");
    let (_connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let placed = remote
        .source(&scripted(&endpoint), &serde_json::json!({}))
        .await
        .expect("the connector is placed");
    assert_eq!(placed.placement, Placement::Remote { endpoint });
    assert_eq!(
        (placed.spec.id.as_str(), placed.digest),
        ("test.scripted", None)
    );
    placed.connector.check().await.expect("the check passes");
}

#[tokio::test]
async fn a_connector_whose_certificate_does_not_name_the_endpoint_is_refused() {
    let pki = Pki::new("ca");
    let (_connector, address) =
        listening(&pki, &pki.server("server", &["elsewhere"]), "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let refused = remote
        .source(&scripted(&endpoint), &serde_json::json!({}))
        .await
        .err()
        .expect("refused");
    assert!(matches!(refused, ProviderError::Tls { .. }), "{refused}");
}

#[tokio::test]
async fn a_host_whose_certificate_the_connector_does_not_trust_is_refused() {
    let (pki, other) = (Pki::new("ca"), Pki::new("other"));
    let (_connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    // The host trusts the connector, but its own certificate comes from another CA.
    let remote = Remote::new(identity(&other.client("host")), pki.ca());
    let refused = remote
        .source(&scripted(&endpoint), &serde_json::json!({}))
        .await
        .err()
        .expect("refused");
    // In TLS 1.3 the host learns of the refusal from the first bytes after its handshake: the
    // refusal is still the TLS's, not a transport failure retrying could mend.
    let ProviderError::Tls { source, .. } = &refused else {
        panic!("{refused}");
    };
    let alert = source.to_string();
    assert!(alert.contains("alert"), "{alert}");
}

#[tokio::test]
async fn a_host_the_connector_was_not_told_to_accept_is_refused() {
    let pki = Pki::new("ca");
    let (_connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    // Its certificate comes from the connector's CA, and names a host the connector was not told.
    let remote = Remote::new(identity(&pki.client("another-host")), pki.ca());
    let refused = remote
        .source(&scripted(&endpoint), &serde_json::json!({}))
        .await
        .err()
        .expect("refused");
    assert!(matches!(refused, ProviderError::Tls { .. }), "{refused}");
}

#[tokio::test]
async fn a_listening_connector_says_once_which_host_a_session_serves() {
    let pki = Pki::new("ca");
    let (mut connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let placed = remote
        .source(&scripted(&endpoint), &serde_json::json!({}))
        .await
        .expect("the connector is placed");
    for _ in 0..3 {
        placed.connector.check().await.expect("the check passes");
    }
    stop(&connector);
    let mut stderr = String::new();
    connector
        .stderr
        .take()
        .expect("its errors are piped")
        .read_to_string(&mut stderr)
        .await
        .expect("its errors read");
    let named = stderr.lines().filter(|line| line.contains("`host`"));
    assert_eq!(named.count(), 1, "{stderr}");
}

#[tokio::test]
async fn a_listening_connector_speaks_no_plaintext_and_counts_whom_it_refused_in_one_line() {
    let pki = Pki::new("ca");
    let (mut connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let mut peers = Vec::new();
    for _ in 0..32 {
        let mut stream = tokio::net::TcpStream::connect(&address)
            .await
            .expect("the connector accepts");
        peers.push(stream.local_addr().expect("a local address"));
        stream
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .expect("the preface is written");
        let mut answer = Vec::new();
        let read =
            tokio::time::timeout(Duration::from_secs(60), stream.read_to_end(&mut answer)).await;
        // It closes the connection, answering at most a TLS alert, and never HTTP/2.
        assert!(read.is_ok(), "the connection stays open");
        assert!(!answer.starts_with(b"\0"), "{answer:?}");
    }
    stop(&connector);
    let mut stderr = String::new();
    connector
        .stderr
        .take()
        .expect("its errors are piped")
        .read_to_string(&mut stderr)
        .await
        .expect("its errors read");
    // A line an interval for all of them, which names none: a peer decides how many are
    // refused. A slow run may cross an interval, and then they are counted over two lines.
    let lines: Vec<String> = stderr.lines().map(str::to_owned).collect();
    assert!((1..=2).contains(&lines.len()), "{stderr}");
    let counted: u64 = lines
        .iter()
        .filter_map(|line| line.split(' ').nth(1)?.parse::<u64>().ok())
        .sum();
    assert_eq!(counted, 32, "{stderr}");
    for peer in peers {
        assert!(!stderr.contains(&peer.to_string()), "{stderr}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn peers_that_never_handshake_do_not_keep_a_host_out() {
    let pki = Pki::new("ca");
    let (_connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    // More idle peers than the connector holds unauthenticated, none of them authenticated; few
    // enough that the test's own process may hold them, where it may open few files.
    let mut idle = Vec::new();
    for _ in 0..100 {
        idle.push(
            tokio::net::TcpStream::connect(&address)
                .await
                .expect("the connector accepts"),
        );
    }
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let placed = tokio::time::timeout(
        Duration::from_secs(60),
        remote.source(&scripted(&endpoint), &serde_json::json!({})),
    )
    .await
    .expect("the host is served while the idle peers wait")
    .expect("the connector is placed");
    placed.connector.check().await.expect("the check passes");
    drop(idle);
}

/// Asks the listening `connector` to stop, as its operator would.
pub(crate) fn stop(connector: &Child) {
    let pid = connector
        .id()
        .and_then(|pid| i32::try_from(pid).ok())
        .expect("a process id");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("the signal is sent");
}

#[tokio::test]
async fn a_connector_that_drops_the_connection_after_its_handshake_is_unreachable() {
    for reset in [true, false] {
        dropped_after_handshake(reset).await;
    }
}

/// A connector that closes the connection after its handshake, reset when `reset`, is
/// unreachable: no certificate was refused.
async fn dropped_after_handshake(reset: bool) {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["localhost"]);
    let config = rdlt_wire::tls::server_config(&identity(&server), &accepted(&pki))
        .expect("the server's configuration builds");
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port is free");
    let port = listener.local_addr().expect("a local address").port();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("the host connects");
        let tls = acceptor.accept(stream).await.expect("the host handshakes");
        // Reset, the host reads an error; closed, the end of the stream.
        if reset {
            tls.get_ref().0.set_zero_linger().ok();
        }
        drop(tls);
    });
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let endpoint = format!("grpcs://localhost:{port}");
    let refused = remote
        .source(&scripted(&endpoint), &serde_json::json!({}))
        .await
        .err()
        .expect("refused");
    let ProviderError::Unreachable { source, .. } = &refused else {
        panic!("reset {reset}: {refused}");
    };
    let expected = if reset {
        std::io::ErrorKind::ConnectionReset
    } else {
        std::io::ErrorKind::UnexpectedEof
    };
    assert_eq!(source.kind(), expected, "reset {reset}: {source}");
}

#[test]
fn a_remote_provider_debugs_what_it_holds() {
    let pki = Pki::new("ca");
    let remote =
        Remote::new(identity(&pki.client("host")), pki.ca()).fallback(rdlt_host::Local::new());
    let shown = format!("{remote:?}");
    assert!(shown.starts_with("Remote { identity: Identity"), "{shown}");
    assert!(shown.ends_with("fallback: true }"), "{shown}");
}

#[tokio::test]
async fn an_endpoint_nothing_listens_on_is_unreachable_and_what_is_no_endpoint_is_refused() {
    let pki = Pki::new("ca");
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let source = |endpoint: &'static str| {
        let remote = remote.clone();
        async move {
            let (reference, config) = (scripted(endpoint), serde_json::json!({}));
            let placed = remote.source(&reference, &config).await;
            placed.err().expect("refused")
        }
    };
    let unreachable = source("grpcs://127.0.0.1:1").await;
    assert!(
        matches!(unreachable, ProviderError::Unreachable { .. }),
        "{unreachable}"
    );
    let refused = source("https://127.0.0.1:1").await;
    assert!(
        matches!(
            refused,
            ProviderError::Endpoint {
                source: rdlt_host::EndpointError::Scheme,
                ..
            }
        ),
        "{refused}"
    );
    let destination = remote
        .destination(&scripted("grpcs://127.0.0.1:1/x"), &serde_json::json!({}))
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(
            destination,
            ProviderError::Endpoint {
                source: rdlt_host::EndpointError::Path,
                ..
            }
        ),
        "{destination}"
    );
}

#[tokio::test]
async fn a_reference_without_an_endpoint_goes_to_the_fallback() {
    let pki = Pki::new("ca");
    let id = ConnectorId::parse("test.scripted").expect("a valid id");
    let local = ConnectorRef::new(id).path(example("scripted_connector"));
    let alone = Remote::new(identity(&pki.client("host")), pki.ca());
    let refused = alone
        .source(&local, &serde_json::json!({}))
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, ProviderError::NotFound { .. }),
        "{refused}"
    );
    let spawning = Remote::new(identity(&pki.client("host")), pki.ca())
        .fallback(rdlt_host::Local::new().env_passthrough("LLVM_PROFILE_FILE"));
    let placed = spawning
        .source(&local, &serde_json::json!({}))
        .await
        .expect("the fallback spawns it");
    assert!(matches!(placed.placement, Placement::Process { .. }));
}

#[tokio::test]
async fn listening_without_mutual_tls_is_refused_and_a_stop_ends_it_cleanly() {
    let output = Command::new(example("scripted_connector"))
        .args(["--listen", "127.0.0.1:0"])
        .output()
        .await
        .expect("the connector runs");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("mutual TLS only"));
    let pki = Pki::new("ca");
    let (mut connector, _) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let pid = connector
        .id()
        .and_then(|pid| i32::try_from(pid).ok())
        .expect("a process id");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGTERM,
    )
    .expect("the signal is sent");
    let status = tokio::time::timeout(Duration::from_secs(10), connector.wait())
        .await
        .expect("it stops")
        .expect("its status reads");
    assert!(status.success(), "{status}");
}

/// Everything `error` says of itself, its causes and its fields.
fn said(error: &dyn std::error::Error) -> String {
    use std::fmt::Write as _;
    let mut said = format!("{error} {error:?}");
    let mut source = error.source();
    while let Some(cause) = source {
        write!(said, " {cause} {cause:?}").expect("a string is written");
        source = cause.source();
    }
    said
}

#[tokio::test]
async fn an_endpoint_with_more_than_a_host_and_a_port_is_refused_without_repeating_it() {
    let pki = Pki::new("ca");
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let endpoints = [
        "grpcs://svc:hunter2@connector:7443",
        "grpcs://hunter2@connector:7443",
        "grpcs://connector:7443/hunter2",
        "grpcs://connector:7443/?token=hunter2",
        "grpcs://connector:7443?token=hunter2",
        "grpcs://connector:7443#hunter2",
        "grpcs://connector:hunter2",
        "grpcs://hunter2 connector:7443",
        "https://hunter2:7443",
    ];
    for endpoint in endpoints {
        let refused = remote
            .source(&scripted(endpoint), &serde_json::json!({}))
            .await
            .err()
            .expect("refused");
        assert!(
            matches!(refused, ProviderError::Endpoint { .. }),
            "{endpoint}"
        );
        assert!(!said(&refused).contains("hunter2"), "{endpoint}");
        let Err(wired) = remote.wire(&scripted(endpoint)).await else {
            panic!("{endpoint} is dialed");
        };
        assert!(!said(&wired).contains("hunter2"), "{endpoint}");
    }
}

#[tokio::test]
async fn an_error_about_an_endpoint_names_its_host_and_port_alone() {
    let pki = Pki::new("ca");
    // Nothing listens there, and the trailing slash is no part of where.
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let unreachable = remote
        .source(&scripted("grpcs://127.0.0.1:1/"), &serde_json::json!({}))
        .await
        .err()
        .expect("unreachable");
    let ProviderError::Unreachable { endpoint, .. } = &unreachable else {
        panic!("{unreachable}");
    };
    assert_eq!(endpoint, "127.0.0.1:1");
    // A host whose key cannot be used fails in its TLS, at the same place.
    let keyless = Identity {
        key: pki.dir().join("missing.key"),
        ..identity(&pki.client("keyless"))
    };
    let untrusted = Remote::new(keyless, pki.ca())
        .source(&scripted("grpcs://[::1]:7443"), &serde_json::json!({}))
        .await
        .err()
        .expect("refused");
    let ProviderError::Tls { endpoint, .. } = &untrusted else {
        panic!("{untrusted}");
    };
    assert_eq!(endpoint, "[::1]:7443");
}

/// The scripted connector, asked to listen with `more` arguments in a process that may open
/// `descriptors` files: its output, once it has announced its address and been stopped, or
/// ended by itself.
async fn limited(descriptors: u32, more: &[&str]) -> std::process::Output {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["localhost"]);
    let mut child = Command::new("sh")
        .args(["-c", "ulimit -n \"$0\" && exec \"$@\""])
        .arg(descriptors.to_string())
        .arg(example("scripted_connector"))
        .args(["--listen", "127.0.0.1:0"])
        .arg("--tls-cert")
        .arg(&server.cert)
        .arg("--tls-key")
        .arg(&server.key)
        .arg("--tls-client-ca")
        .arg(pki.ca())
        .args(["--tls-allow-host", "host"])
        .args(more)
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("the connector starts");
    let stdout = child.stdout.take().expect("its output is piped");
    let announced = BufReader::new(stdout).lines().next_line().await;
    let announced = announced.expect("its output reads");
    if announced.is_some() {
        stop(&child);
    }
    let mut output = tokio::time::timeout(Duration::from_secs(120), child.wait_with_output())
        .await
        .expect("the connector ends")
        .expect("its output reads");
    output.stdout = announced.unwrap_or_default().into_bytes();
    output
}

#[tokio::test]
async fn a_connector_that_may_open_too_few_descriptors_refuses_to_listen() {
    let refused = limited(256, &[]).await;
    assert!(!refused.status.success());
    assert!(refused.stdout.is_empty(), "it announced an address");
    // It says how many it may open, and how many it needs.
    let said = String::from_utf8_lossy(&refused.stderr);
    let needed = rdlt_connector::limits::ListenLimits::default().descriptors();
    assert!(
        said.contains("256") && said.contains(&needed.to_string()),
        "{said}"
    );
}

#[tokio::test]
async fn a_connector_serving_fewer_sessions_listens_within_fewer_descriptors() {
    let listening = limited(256, &["--max-sessions", "8"]).await;
    assert!(listening.status.success(), "{listening:?}");
    assert!(listening.stdout.starts_with(b"listening on "));
}

#[tokio::test]
async fn a_connector_whose_hosts_could_leave_one_no_session_refuses_to_listen() {
    // Two hosts and one session: whichever holds it leaves the other none.
    let unfair = limited(4096, &["--tls-allow-host", "other", "--max-sessions", "1"]).await;
    assert!(!unfair.status.success());
    assert!(unfair.stdout.is_empty(), "it announced an address");
    let greedy = ["--tls-allow-host", "other", "--max-host-sessions", "256"];
    let greedy = limited(4096, &greedy).await;
    assert!(!greedy.status.success());
    assert!(greedy.stdout.is_empty(), "it announced an address");
    let fair = ["--tls-allow-host", "other", "--max-host-sessions", "100"];
    let fair = limited(4096, &fair).await;
    assert!(fair.status.success(), "{fair:?}");
    assert!(fair.stdout.starts_with(b"listening on "));
}

/// Opens silent connections to `address` from `from` at `rate` a second, holding the newest
/// hundred, until `stopped`; answers how many it opened.
async fn flood(
    address: String,
    from: std::net::IpAddr,
    rate: u64,
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> u64 {
    use std::sync::atomic::Ordering;
    let address: std::net::SocketAddr = address.parse().expect("an address");
    let mut held = std::collections::VecDeque::new();
    let started = std::time::Instant::now();
    let mut opened = 0_u64;
    while !stopped.load(Ordering::Relaxed) {
        let due = started.elapsed().as_millis() * u128::from(rate) / 1000;
        if u128::from(opened) >= due {
            tokio::time::sleep(Duration::from_micros(200)).await;
            continue;
        }
        let socket = tokio::net::TcpSocket::new_v4().expect("a socket");
        socket
            .bind(std::net::SocketAddr::new(from, 0))
            .expect("the address is this machine's");
        if let Ok(stream) = socket.connect(address).await {
            held.push_back(stream);
            opened += 1;
        }
        if held.len() > 100 {
            held.pop_front();
        }
    }
    opened
}

/// A measurement, not a check: how many of a host's dials complete while silent peers connect
/// at a rate, from the host's own address and from another.
///
/// Run by name on Linux, whose loopback answers at every `127.0.0.0/8` address:
/// `cargo nextest run -p rdlt-host --all-features --run-ignored ignored-only -E
/// 'test(dials_under_a_flood)' --no-capture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement, run by name"]
async fn dials_under_a_flood_of_silent_peers_measured() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let pki = Pki::new("ca");
    let (_connector, address) =
        listening(&pki, &pki.server("server", &["localhost"]), "127.0.0.1:0").await;
    let endpoint = format!("grpcs://localhost:{}", port(&address));
    let remote = Remote::new(identity(&pki.client("host")), pki.ca());
    let reference = scripted(&endpoint);
    let (own, other) = ([127, 0, 0, 1].into(), [127, 0, 0, 2].into());
    for (from, rate) in [(other, 0), (other, 5_000), (other, 20_000), (own, 5_000)] {
        let stopped = Arc::new(AtomicBool::new(false));
        let floods: Vec<_> = (0..4)
            .filter(|_| rate > 0)
            .map(|_| tokio::spawn(flood(address.clone(), from, rate / 4, Arc::clone(&stopped))))
            .collect();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let began = std::time::Instant::now();
        let (mut served, dials) = (0, 200);
        for _ in 0..dials {
            let wire = tokio::time::timeout(Duration::from_secs(3), remote.wire(&reference));
            if matches!(wire.await, Ok(Ok(_))) {
                served += 1;
            }
        }
        let took = began.elapsed();
        stopped.store(true, Ordering::Relaxed);
        let mut opened = 0;
        for flood in floods {
            opened += flood.await.expect("the flood ends");
        }
        eprintln!(
            "flood from {from} asked {rate}/s, opened {opened}: {served}/{dials} dials in {took:?}"
        );
        tokio::time::sleep(Duration::from_secs(6)).await;
    }
}
