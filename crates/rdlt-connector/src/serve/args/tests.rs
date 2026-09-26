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
    ]);
    let Ok(Args::Listen(Listen {
        address,
        identity,
        client_ca,
    })) = listen
    else {
        panic!("{listen:?}");
    };
    assert_eq!(address.to_string(), "127.0.0.1:0");
    assert_eq!(
        (
            identity.cert.to_str(),
            identity.key.to_str(),
            client_ca.to_str()
        ),
        (Some("server.pem"), Some("server.key"), Some("ca.pem"))
    );
    let ipv6 = parsed(&[
        "--listen",
        "[::1]:7443",
        "--tls-cert",
        "c",
        "--tls-key",
        "k",
        "--tls-client-ca",
        "a",
    ]);
    assert!(matches!(ipv6, Ok(Args::Listen(Listen { address, .. })) if address.port() == 7443));
}

#[test]
fn anything_else_is_refused_with_why() {
    let tls = ["--tls-cert", "c", "--tls-key", "k", "--tls-client-ca", "a"];
    let cases: Vec<(Vec<&str>, &str)> = vec![
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
        (
            [vec!["--rdlt-fd", "3"], tls.to_vec()].concat(),
            "go with `--listen`",
        ),
    ];
    for (args, why) in cases {
        let refused = parsed(&args).unwrap_err().to_string();
        assert!(refused.contains(why), "{args:?}: {refused}");
    }
}
