//! The libpq conn-string translation pins, lifted from the module's former
//! inline tests: the parse gate, TLS-trio extraction, and rejection texts.

#[cfg(test)]
mod tests {
    use rdlt_connector_postgres::tls::{PemSource, TlsMode, TlsPolicy, parse_conn};

    #[test]
    fn malformed_percent_escapes_are_typed_errors() {
        // Never a silent literal passthrough.
        for bad in [
            "postgresql://u@h/db?sslrootcert=%2",
            "postgresql://u@h/db?sslkey=bad%zz&sslcert=/c.pem",
        ] {
            let err = parse_conn(bad, None).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("percent-escape"), "{bad}: {msg}");
        }
        // Valid escapes still decode.
        parse_conn("postgresql://u@h/db?sslrootcert=%2Fca.pem", None).expect("valid escape");
    }

    #[test]
    fn tls_trio_extracts_from_both_libpq_forms() {
        // URL form, percent-encoded path.
        let parsed = parse_conn(
            "postgresql://u@h:5432/db?sslmode=verify-full&sslrootcert=%2Fetc%2Fca.pem",
            None,
        )
        .expect("url form");
        assert_eq!(
            parsed.policy.root_cert,
            Some(PemSource("/etc/ca.pem".into()))
        );
        // libpq's verify-full spelling (which the driver itself rejects)
        // translates into the policy mode.
        assert_eq!(parsed.policy.mode, TlsMode::VerifyFull);
        // …and a weaker block mode cannot silently reverse it.
        let weaker = TlsPolicy {
            mode: TlsMode::Require,
            ..TlsPolicy::default()
        };
        assert!(
            parse_conn(
                "postgresql://u@h/db?sslmode=verify-full&sslrootcert=/ca.pem",
                Some(&weaker),
            )
            .is_err(),
            "block must not weaken conn verify-full"
        );

        // key=value form with spaces and a quoted path; full trio.
        let parsed = parse_conn(
            "host=h user=u sslrootcert = '/my ca/ca.pem' sslcert=/c.pem sslkey=/k.pem",
            None,
        )
        .expect("kv form");
        assert_eq!(
            parsed.policy.root_cert,
            Some(PemSource("/my ca/ca.pem".into()))
        );
        assert_eq!(parsed.policy.client_cert, Some(PemSource("/c.pem".into())));
        assert_eq!(parsed.policy.client_key, Some(PemSource("/k.pem".into())));
        // The remainder reached the driver intact.
        assert_eq!(parsed.pg.get_user(), Some("u"));

        // `system` selects the platform store (no explicit root).
        let parsed = parse_conn("host=h sslrootcert=system", None).expect("system");
        assert_eq!(parsed.policy.root_cert, None);
    }

    #[test]
    fn conn_and_block_values_must_agree() {
        let block = TlsPolicy {
            root_cert: Some(PemSource("/etc/other.pem".into())),
            ..TlsPolicy::default()
        };
        // Disagreement: typed, names both sides.
        let err = parse_conn("host=h sslrootcert=/etc/ca.pem", Some(&block)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("sslrootcert") && msg.contains("/etc/other.pem"),
            "{msg}"
        );
        // Agreement: accepted.
        parse_conn("host=h sslrootcert=/etc/other.pem", Some(&block)).expect("agreeing dup");
        // Split credential across sources: cert in string, key in block.
        let block = TlsPolicy {
            client_key: Some(PemSource("/k.pem".into())),
            ..TlsPolicy::default()
        };
        let parsed = parse_conn("host=h sslcert=/c.pem", Some(&block)).expect("split");
        assert_eq!(parsed.policy.client_cert, Some(PemSource("/c.pem".into())));
        // …and the both-or-neither rule still bites across sources.
        let err = parse_conn("host=h sslcert=/c.pem", None).unwrap_err();
        assert!(err.to_string().contains("client_key is missing"), "{err}");
    }

    #[test]
    fn sslrootcert_system_selects_the_platform_store() {
        // libpq 16+ `sslrootcert=system` = platform trust store: the policy
        // resolves to a verifying mode with NO explicit root.
        let parsed = parse_conn(
            "postgresql://u@h/d?sslmode=verify-full&sslrootcert=system",
            None,
        )
        .expect("system root parses");
        assert_eq!(parsed.policy.mode, TlsMode::VerifyFull);
        assert!(
            parsed.policy.root_cert.is_none(),
            "system = platform store = no explicit root in the policy"
        );
    }

    #[test]
    fn rejected_parameters_are_named_never_bare() {
        // Every unsupported parameter is NAMED, with a hint where one exists.
        for (conn, param, hint_word) in [
            ("host=h sslpassword=secret", "sslpassword", "encrypted"),
            ("host=h gssencmode=require", "gssencmode", "GSS"),
            ("host=h requiressl=1", "requiressl", "sslmode"),
            ("host=h sslcrl=/crl.pem", "sslcrl", "revocation"),
            (
                "postgresql://u@h/db?connect_timeout=5&some_future_param=1",
                "some_future_param",
                "no rdlt equivalent",
            ),
        ] {
            let err = parse_conn(conn, None).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains(param) && msg.contains(hint_word),
                "{conn}: {msg}"
            );
        }
        // Pure syntax garbage still gets the driver's description, prefixed.
        let err = parse_conn("host=h port=notaport", None).unwrap_err();
        assert!(err.to_string().contains("does not parse"), "{err}");
    }

    #[test]
    fn application_name_defaults_and_yields() {
        let parsed = parse_conn("host=h user=u", None).expect("parses");
        assert_eq!(parsed.pg.get_application_name(), Some("rdlt"));
        let parsed = parse_conn("host=h user=u application_name=mine", None).expect("parses");
        assert_eq!(parsed.pg.get_application_name(), Some("mine"));
    }
}
