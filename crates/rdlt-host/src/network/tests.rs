use super::Endpoint;

#[test]
fn an_endpoint_is_a_host_and_a_port() {
    let cases = [
        ("grpcs://connector.example:7443", "connector.example", 7443),
        ("grpcs://127.0.0.1:1/", "127.0.0.1", 1),
        ("grpcs://[::1]:7443", "::1", 7443),
    ];
    for (endpoint, host, port) in cases {
        let parsed = Endpoint::parse(endpoint).expect("a valid endpoint");
        assert_eq!(
            (parsed.host.as_str(), parsed.port),
            (host, port),
            "{endpoint}"
        );
    }
}

#[test]
fn anything_else_is_refused_with_why() {
    let cases = [
        ("https://connector.example:7443", "is not grpcs"),
        ("grpc://connector.example:7443", "is not grpcs"),
        ("grpcs://connector.example", "has no port"),
        ("grpcs://connector.example:port", "has no valid port"),
        ("grpcs://connector.example:70000", "has no valid port"),
        ("grpcs://:7443", "has no host"),
        (
            "grpcs://user@connector.example:7443",
            "more than a host and a port",
        ),
        (
            "grpcs://connector.example:7443/path",
            "more than a host and a port",
        ),
        ("grpcs://::1:7443", "IPv6 address outside brackets"),
        ("grpcs://fe80::1", "IPv6 address outside brackets"),
        ("grpcs://[::1:7443", "unclosed bracket"),
        (
            "grpcs://[connector.example]:7443",
            "no IPv6 address in brackets",
        ),
        ("grpcs://[]:7443", "no IPv6 address in brackets"),
    ];
    for (endpoint, why) in cases {
        let refused = Endpoint::parse(endpoint).expect_err("refused");
        assert_eq!(
            refused.kind(),
            std::io::ErrorKind::InvalidInput,
            "{endpoint}"
        );
        assert!(refused.to_string().contains(why), "{endpoint}: {refused}");
    }
}
