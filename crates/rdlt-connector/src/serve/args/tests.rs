use rdlt_wire::tls::Hosts;

use super::{Args, Failure, Listen, parse};

fn parsed(args: &[&str]) -> Result<Args, Failure> {
    parse(args.iter().map(|arg| (*arg).to_owned()))
}

#[test]
fn the_hosts_socket_is_named_either_way() {
    assert_eq!(parsed(&["--rdlt-fd", "3"]), Ok(Args::Inherited { fd: 3 }));
    assert_eq!(parsed(&["--rdlt-fd=7"]), Ok(Args::Inherited { fd: 7 }));
}

#[test]
fn a_listening_connector_names_its_address_and_its_tls() {
    let listen = parsed(&[
        "--listen=127.0.0.1:0",
        "--tls-cert",
        "server.pem",
        "--tls-key=server.key",
        "--tls-client-ca",
        "ca.pem",
        "--tls-allow-host",
        "loader.example",
        "--tls-allow-host=spiffe://example.org/loader",
    ]);
    let Ok(Args::Listen(Listen {
        address,
        identity,
        accepted,
        sessions,
        host_sessions,
    })) = listen
    else {
        panic!("{listen:?}");
    };
    assert_eq!(address.to_string(), "127.0.0.1:0");
    assert_eq!(
        (
            identity.cert.to_str(),
            identity.key.to_str(),
            accepted.ca.to_str()
        ),
        (Some("server.pem"), Some("server.key"), Some("ca.pem"))
    );
    let hosts = Hosts::new(["loader.example", "spiffe://example.org/loader"]);
    assert_eq!(Ok(accepted.hosts), hosts);
    assert_eq!((accepted.crl, sessions, host_sessions), (None, None, None));
}

#[test]
fn a_listening_connector_names_its_revocation_lists_and_its_sessions() {
    let ipv6 = parsed(&[
        "--listen",
        "[::1]:7443",
        "--tls-cert",
        "c",
        "--tls-key",
        "k",
        "--tls-client-ca",
        "a",
        "--tls-allow-host",
        "h",
        "--tls-client-crl",
        "revoked.crl",
        "--max-sessions",
        "8",
        "--max-host-sessions=3",
    ]);
    let Ok(Args::Listen(Listen {
        address,
        accepted,
        sessions,
        host_sessions,
        ..
    })) = ipv6
    else {
        panic!("{ipv6:?}");
    };
    assert_eq!(address.port(), 7443);
    let counts = [sessions, host_sessions].map(|count| count.map(std::num::NonZeroUsize::get));
    assert_eq!(counts, [Some(8), Some(3)]);
    assert_eq!(
        accepted.crl.as_deref(),
        Some(std::path::Path::new("revoked.crl"))
    );
}

/// Asserts each of `cases` is refused with a reason that says its `why`.
fn refused(cases: Vec<(Vec<&str>, &str)>) {
    for (args, why) in cases {
        let refused = parsed(&args).unwrap_err().to_string();
        assert!(refused.contains(why), "{args:?}: {refused}");
    }
}

const TLS: [&str; 6] = ["--tls-cert", "c", "--tls-key", "k", "--tls-client-ca", "a"];

/// A listening connector's arguments with `more`.
fn listening<'a>(more: &[&'a str]) -> Vec<&'a str> {
    let listen = ["--listen", "0.0.0.0:1", "--tls-allow-host", "h"];
    [listen.as_slice(), TLS.as_slice(), more].concat()
}

#[test]
fn anything_else_is_refused_with_why() {
    refused(vec![
        (vec![], "serves its host's socket"),
        (vec!["--rdlt-fd"], "needs a value"),
        (vec!["--rdlt-fd", "three"], "is not a file descriptor"),
        (vec!["--rdlt-fd=3", "--rdlt-fd=4"], "given twice"),
        (vec!["--rdlt-fdx", "3"], "unknown argument `--rdlt-fdx`"),
        (vec!["serve"], "unknown argument `serve`"),
        (vec!["--listen", "0.0.0.0:1"], "mutual TLS only"),
        (
            vec!["--listen", "0.0.0.0:1", "--tls-cert", "c", "--tls-key", "k"],
            "mutual TLS only",
        ),
        (vec!["--listen", "localhost"], "is not an address and port"),
        (
            vec!["--rdlt-fd", "3", "--listen", "0.0.0.0:1"],
            "exclude each other",
        ),
    ]);
}

#[test]
fn what_goes_with_listening_is_refused_with_the_hosts_socket() {
    let mut cases = vec![
        (
            [&["--rdlt-fd", "3"], TLS.as_slice()].concat(),
            "go with `--listen`",
        ),
        (
            vec!["--rdlt-fd", "3", "--tls-allow-host", "h"],
            "go with `--listen`",
        ),
        (
            vec!["--rdlt-fd", "3", "--max-sessions", "4"],
            "go with `--listen`",
        ),
        (
            vec!["--rdlt-fd", "3", "--max-host-sessions", "4"],
            "go with `--listen`",
        ),
    ];
    for option in [
        "--tls-cert",
        "--tls-key",
        "--tls-client-ca",
        "--tls-client-crl",
    ] {
        cases.push((vec!["--rdlt-fd", "3", option, "x"], "go with `--listen`"));
    }
    refused(cases);
}

#[test]
fn a_listening_connector_must_name_its_hosts_and_count_its_sessions() {
    refused(vec![
        (
            [&["--listen", "0.0.0.0:1"], TLS.as_slice()].concat(),
            "the hosts named to it",
        ),
        (
            [
                &["--listen", "0.0.0.0:1", "--tls-allow-host="],
                TLS.as_slice(),
            ]
            .concat(),
            "names no host",
        ),
        (
            listening(&["--tls-allow-host", "*.example.com"]),
            "names no host",
        ),
        (listening(&["--tls-allow-host", "host."]), "names no host"),
        (
            listening(&["--tls-client-crl", "a", "--tls-client-crl", "b"]),
            "given twice",
        ),
        (
            listening(&["--max-sessions", "0"]),
            "is not a count of sessions",
        ),
        (
            listening(&["--max-sessions", "many"]),
            "is not a count of sessions",
        ),
    ]);
    assert!(parsed(&listening(&[])).is_ok());
}
