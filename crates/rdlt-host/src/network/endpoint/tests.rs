use super::{Endpoint, EndpointError};

#[test]
fn an_endpoint_is_a_host_and_a_port() {
    let cases = [
        (
            "grpcs://connector.example:7443",
            "connector.example",
            7443,
            "connector.example:7443",
        ),
        ("grpcs://127.0.0.1:1/", "127.0.0.1", 1, "127.0.0.1:1"),
        ("grpcs://[::1]:7443", "::1", 7443, "[::1]:7443"),
        (
            "grpcs://localhost:65535",
            "localhost",
            65535,
            "localhost:65535",
        ),
    ];
    for (endpoint, host, port, shown) in cases {
        let parsed = Endpoint::parse(endpoint).expect("a valid endpoint");
        assert_eq!((parsed.host(), parsed.port()), (host, port), "{endpoint}");
        assert_eq!(parsed.to_string(), shown, "{endpoint}");
    }
}

#[test]
fn anything_else_is_refused_by_what_is_wrong_and_never_repeated() {
    use EndpointError::{Credentials, Fragment, Host, Path, Port, Query, Scheme};
    let cases = [
        ("https://connector.example:7443", Scheme),
        ("grpc://connector.example:7443", Scheme),
        ("connector.example:7443", Scheme),
        ("grpcs://connector.example", Port),
        ("grpcs://connector.example:port", Port),
        ("grpcs://connector.example:70000", Port),
        ("grpcs://connector.example:-1", Port),
        ("grpcs://connector.example:", Port),
        ("grpcs://:7443", Host),
        ("grpcs://user@connector.example:7443", Credentials),
        ("grpcs://user:secret@connector.example:7443", Credentials),
        ("grpcs://@connector.example:7443", Credentials),
        ("grpcs://connector.example:7443/path", Path),
        ("grpcs://connector.example:7443//", Path),
        ("grpcs://connector.example/secret:7443", Path),
        ("grpcs://connector.example:7443?token=secret", Query),
        ("grpcs://connector.example:7443/?token=secret", Query),
        ("grpcs://user:secret@connector.example:7443?x", Query),
        ("grpcs://connector.example:7443#secret", Fragment),
        ("grpcs://connector.example:7443/?a#secret", Fragment),
        ("grpcs://::1:7443", Host),
        ("grpcs://fe80::1", Host),
        ("grpcs://[::1:7443", Host),
        ("grpcs://[connector.example]:7443", Host),
        ("grpcs://[]:7443", Host),
        ("grpcs://connector example:7443", Host),
        ("grpcs://connector_example!:7443", Host),
    ];
    for (endpoint, wrong) in cases {
        let refused = Endpoint::parse(endpoint).expect_err("refused");
        assert_eq!(refused, wrong, "{endpoint}");
        let said = format!("{refused} {refused:?}");
        for part in ["connector", "secret", "token", "user", "7443"] {
            assert!(!said.contains(part), "{endpoint}: {said}");
        }
    }
}
