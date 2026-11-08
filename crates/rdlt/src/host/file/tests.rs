use super::{HostFile, RemoteFiles};

#[tokio::test]
async fn a_host_file_reads_in_the_operator_s_words() {
    let file = HostFile::parse(
        "connectors: [/opt/rdlt/bin]\nwritable: [/data]\nreadable: [/in]\nalways_readable: [/etc/ssl]\n\
         allow_env: [DB_PASSWORD]\nallow_files: [/run/secrets]\ntrust_binaries: true\n",
    )
    .unwrap();
    assert_eq!(
        file.connectors,
        ["/opt/rdlt/bin"].map(std::path::PathBuf::from)
    );
    assert_eq!(file.writable, ["/data"].map(std::path::PathBuf::from));
    assert_eq!(file.readable, ["/in"].map(std::path::PathBuf::from));
    assert_eq!(
        file.always_readable,
        ["/etc/ssl"].map(std::path::PathBuf::from)
    );
    assert_eq!(file.allow_env, ["DB_PASSWORD"]);
    assert_eq!(
        file.allow_files,
        ["/run/secrets"].map(std::path::PathBuf::from)
    );
    assert!(file.trust_binaries);
    assert_eq!(file.remote, None);
    // Its placement settings need `local`, and a build without it refuses them.
    let built = file.host().await;
    assert_eq!(built.is_ok(), cfg!(feature = "local"), "{:?}", built.err());
    let unknown = HostFile::parse("conectors: [/opt]\n").unwrap_err();
    assert!(
        unknown.to_string().contains("did you mean `connectors`?"),
        "{unknown}"
    );
    assert_eq!(
        unknown.code().map(rdlt_engine::Code::as_str),
        Some("document_unknown_key")
    );
    assert!(
        !HostFile::parse("trust_binaries: false\n")
            .unwrap()
            .trust_binaries
    );
    let refused = HostFile::parse("trust_binaries: 31337424\n").unwrap_err();
    assert!(!refused.to_string().contains("31337424"), "{refused}");
}

#[tokio::test]
#[cfg(feature = "local")]
async fn a_host_file_allows_the_network_only_where_it_says_so() {
    use crate::{Access, Connector, Side};
    let side = Side::new(Connector::new("a.b")).access(Access::new().network());
    let allowing = HostFile::parse("allow_network: true\n")
        .unwrap()
        .host()
        .await
        .unwrap();
    allowing
        .admits_network(&side, rdlt_connector::Role::Source)
        .expect("the file allows it");
    for text in ["", "allow_network: false\n"] {
        let host = HostFile::parse(text).unwrap().host().await.unwrap();
        let refused = host
            .admits_network(&side, rdlt_connector::Role::Source)
            .expect_err("the file does not allow it");
        assert!(refused.to_string().contains("allow_network"), "{refused}");
    }
}

#[test]
fn a_setting_of_a_feature_this_build_lacks_is_refused_by_name_and_never_ignored() {
    for (text, setting, feature) in [
        ("connectors: [/opt]\n", "connectors", "local"),
        ("trust_binaries: true\n", "trust_binaries", "local"),
        ("writable: [/data]\n", "writable", "local"),
        ("readable: [/in]\n", "readable", "local"),
        ("always_readable: [/etc/ssl]\n", "always_readable", "local"),
        ("allow_network: true\n", "allow_network", "local"),
        (
            "remote: { cert: /a, key: /b, ca: /c }\n",
            "remote",
            "remote",
        ),
    ] {
        let file = HostFile::parse(text).unwrap();
        let refused = file.refuse_unbuilt(false, false).expect_err(text);
        assert_eq!(
            refused.code().map(rdlt_engine::Code::as_str),
            Some("host_setting_unavailable"),
            "{text}"
        );
        let said = refused.to_string();
        assert!(said.contains(&format!("`{setting}`")), "{said}");
        assert!(said.contains(&format!("`{feature}` feature")), "{said}");
        file.refuse_unbuilt(true, true)
            .unwrap_or_else(|refused| panic!("{text}: {refused}"));
    }
    let plain = HostFile::parse("allow_env: [A]\nallow_files: [/run/s]\n").unwrap();
    plain
        .refuse_unbuilt(false, false)
        .expect("no feature is needed");
    let remote = HostFile::parse("remote: { cert: /a, key: /b, ca: /c }\n").unwrap();
    remote.refuse_unbuilt(false, true).expect("remote is built");
}

#[test]
fn a_host_file_s_environment_names_are_text_and_nothing_else() {
    for text in ["allow_env: [31337424]\n", "allow_env: [true]\n"] {
        let refused = HostFile::parse(text).expect_err(text);
        assert_eq!(
            refused.code().map(rdlt_engine::Code::as_str),
            Some("document_invalid"),
            "{refused}"
        );
        assert!(!refused.to_string().contains("31337424"), "{refused}");
    }
    let names = HostFile::parse("allow_env: [\"31337424\", DB_PASSWORD]\n").unwrap();
    assert_eq!(names.allow_env, ["31337424", "DB_PASSWORD"]);
}

#[test]
fn a_host_file_s_paths_are_text_and_nothing_else() {
    for field in [
        "connectors",
        "writable",
        "readable",
        "always_readable",
        "allow_files",
    ] {
        let text = format!("{field}: [31337424]\n");
        let refused = HostFile::parse(&text).expect_err(&text);
        assert_eq!(
            refused.code().map(rdlt_engine::Code::as_str),
            Some("document_invalid"),
            "{refused}"
        );
        assert!(!refused.to_string().contains("31337424"), "{refused}");
    }
    let parsed = HostFile::parse("writable: [\"31337424\", /tmp/out]\n").unwrap();
    assert_eq!(
        parsed.writable,
        ["31337424", "/tmp/out"].map(std::path::PathBuf::from)
    );
}

#[test]
fn a_host_file_left_empty_or_commented_out_allows_nothing() {
    for text in ["", "\n", "# nothing allowed yet\n", "  # a\n  # b"] {
        assert_eq!(
            HostFile::parse(text).unwrap(),
            HostFile::default(),
            "{text:?}"
        );
    }
    let scalar = HostFile::parse("CANARY-value-9f3").unwrap_err();
    assert_eq!(
        scalar.code().map(rdlt_engine::Code::as_str),
        Some("document_invalid")
    );
    assert!(!scalar.to_string().contains("CANARY"), "{scalar}");
}

/// A file left empty reads as the default; one that fails to scan at its first token holds no
/// node either, and is a syntax error all the same, at its place, for a host file and a document.
#[test]
fn a_text_that_fails_to_scan_at_its_first_token_is_a_syntax_error() {
    for text in ["@a", "\"a", "]", "%", "@connectors: [/opt]\n"] {
        let refusals = [
            HostFile::parse(text).map(drop).unwrap_err(),
            crate::Document::parse(text).map(drop).unwrap_err(),
        ];
        for refused in refusals {
            assert_eq!(
                refused.code().map(rdlt_engine::Code::as_str),
                Some("document_syntax"),
                "{text:?}: {refused}"
            );
            let place = refused.to_string();
            let (line, _) = place.split_once(':').expect("a place");
            assert!(line.parse::<u32>().is_ok(), "{text:?}: {refused}");
        }
    }
}

#[test]
fn a_host_file_is_refused_in_its_own_words_and_quotes_nothing() {
    let large = format!("# {}\n", "x".repeat(crate::limits::DOCUMENT_BYTES));
    let error = HostFile::parse(&large).unwrap_err();
    assert_eq!(
        error.code().map(rdlt_engine::Code::as_str),
        Some("document_too_large")
    );
    assert!(
        error.to_string().contains("the host file is larger"),
        "{error}"
    );
    let deep = format!("writable: {}/a{}\n", "[".repeat(40), "]".repeat(40));
    let error = HostFile::parse(&deep).unwrap_err();
    assert_eq!(
        error.code().map(rdlt_engine::Code::as_str),
        Some("document_too_deep")
    );
    assert!(
        error.to_string().contains("the host file nests deeper"),
        "{error}"
    );
    for refused in [
        "connectors: CANARY-a\n",
        "connectors: [[CANARY-h]]\n",
        "writable: 31337424\n",
        "readable: CANARY-b\n",
        "always_readable: [[CANARY-j]]\n",
        "allow_env: 31337424\n",
        "allow_env: [{ a: CANARY-c }]\n",
        "allow_files: CANARY-d\n",
        "trust_binaries: CANARY-e\n",
        "remote: CANARY-f\n",
        "remote: { cert: [CANARY-i], key: /a, ca: /b }\n",
        "remote: { cert: /a, key: { a: CANARY-g }, ca: /b }\n",
    ] {
        let error = HostFile::parse(refused).expect_err(refused);
        let said = crate::document::tests::said(&error);
        for canary in ["CANARY", "31337424"] {
            assert!(!said.contains(canary), "{said} for {refused}");
        }
    }
}

#[test]
fn a_host_file_round_trips_and_its_remote_files_read() {
    let mut file = HostFile {
        allow_env: vec!["A".to_owned()],
        connectors: vec!["/bin".into()],
        ..HostFile::default()
    };
    file.remote = Some(RemoteFiles::new(
        "/tls/host.pem",
        "/tls/host.key",
        "/tls/ca.pem",
    ));
    let written = serde_saphyr::to_string(&file).unwrap();
    assert_eq!(HostFile::parse(&written).unwrap(), file);
}

#[tokio::test]
#[cfg(feature = "remote")]
async fn a_host_file_names_its_remote_files_and_a_missing_one_fails_the_host() {
    let file = HostFile::parse(
        "remote: { cert: /absent/host.pem, key: /absent/host.key, ca: /absent/ca.pem }\n",
    )
    .unwrap();
    let error = file.host().await.expect_err("a missing TLS file");
    assert_eq!(
        error.code().map(rdlt_engine::Code::as_str),
        Some("tls_unreadable")
    );
    assert!(HostFile::parse("remote: { cert: /a }\n").is_err());
}

/// Run by `just test` without the facade's default features: what a file sets that this build
/// cannot do is refused, not ignored.
#[cfg(not(any(feature = "local", feature = "remote")))]
#[tokio::test]
async fn a_host_file_setting_of_a_feature_not_built_is_refused() {
    let remote = "remote: { cert: /a, key: /b, ca: /c }\n";
    let refused = HostFile::parse(remote).unwrap().host().await.unwrap_err();
    assert!(refused.to_string().contains("`remote`"), "{refused}");
    let local = HostFile::parse("connectors: [/opt]\n").unwrap();
    let refused = local.host().await.unwrap_err();
    assert!(refused.to_string().contains("`local`"), "{refused}");
    let plain = HostFile::parse("allow_env: [A]\n").unwrap();
    drop(plain.host().await.expect("nothing needs a feature"));
}
