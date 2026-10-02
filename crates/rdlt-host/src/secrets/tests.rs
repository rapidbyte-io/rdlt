use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::fs::PermissionsExt;

use rdlt_connector::{BoxFuture, Secret};
use serde_json::json;

use super::reference::{Piece, pieces};
use super::{
    Config, EnvSecrets, FileSecrets, Redactions, ReferenceFault, SecretError, SecretFault,
    SecretKind, SecretReference, SecretResolver, Secrets,
};
use crate::limits::{CONFIG_BYTES, SECRET_BYTES, SECRET_NAME_BYTES, SECRET_REFERENCES};

/// Secrets by the name each kind knows them by.
#[derive(Debug, Default)]
struct Vault(BTreeMap<(&'static str, &'static str), &'static str>);

impl SecretResolver for Vault {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            let kind = match reference.kind {
                SecretKind::Env => "env",
                SecretKind::File => "file",
                SecretKind::Named => "secret",
            };
            let held = self
                .0
                .iter()
                .find(|((k, name), _)| *k == kind && *name == reference.name);
            let (_, secret) = held.ok_or(SecretFault::Missing)?;
            Ok(Secret::new((*secret).to_owned()))
        })
    }
}

fn vault() -> Vault {
    Vault(BTreeMap::from([
        (("env", "DB_PASSWORD"), "hunter2"),
        (("file", "/run/secrets/token"), "tok\"en\\9"),
        (("secret", "api-key"), "k-12345"),
        (("secret", "empty"), ""),
    ]))
}

async fn resolved(document: &serde_json::Value) -> (Result<String, SecretError>, Redactions) {
    let redactions = Redactions::new();
    let config = Config::from(document);
    let sent = config.resolved(&vault(), &redactions).await;
    (sent.map(|json| json.as_str().to_owned()), redactions)
}

#[tokio::test]
async fn a_reference_in_any_text_value_is_replaced_by_its_secret_as_the_connector_is_sent_it() {
    let document = json!({
        "dsn": "postgres://app:${env:DB_PASSWORD}@db/prod",
        "auth": { "token": "${file:/run/secrets/token}", "keys": ["${secret:api-key}", "plain"] },
        "both": "${secret:api-key}${env:DB_PASSWORD}",
        "nothing": "${secret:empty}",
        "port": 5432,
        "tls": true,
        "none": null,
    });
    let (sent, redactions) = resolved(&document).await;
    let sent: serde_json::Value = serde_json::from_str(&sent.expect("it resolves")).expect("JSON");
    assert_eq!(
        sent,
        json!({
            "dsn": "postgres://app:hunter2@db/prod",
            "auth": { "token": "tok\"en\\9", "keys": ["k-12345", "plain"] },
            "both": "k-12345hunter2",
            "nothing": "",
            "port": 5432,
            "tls": true,
            "none": null,
        })
    );
    // Each secret, in each form a connector may say it back in, is what gets scrubbed.
    let said = r#"dsn hunter2, token tok"en\9 or "tok\"en\\9", key k-12345, port 5432"#;
    assert_eq!(
        redactions.scrubbed(said.to_owned()),
        r#"dsn ***, token *** or "***", key ***, port 5432"#
    );
}

#[tokio::test]
async fn a_configuration_without_references_is_sent_as_it_is() {
    let document = json!({ "path": "/data/db.sqlite", "price": "$5 and ${", "n": [1, "two"] });
    for document in [
        document,
        json!("text"),
        json!(7),
        json!(null),
        json!([]),
        json!({}),
    ] {
        let (sent, redactions) = resolved(&document).await;
        if document.get("price").is_some() {
            // An opened reference that is never closed is refused, not sent.
            assert!(matches!(sent, Err(SecretError::Reference { .. })));
            continue;
        }
        let sent: serde_json::Value = serde_json::from_str(&sent.expect("sent")).expect("JSON");
        assert_eq!(sent, document);
        assert_eq!(redactions.scrubbed("two text".to_owned()), "two text");
    }
}

#[tokio::test]
async fn a_literal_dollar_brace_is_written_doubled_and_sent_single() {
    let (sent, _) = resolved(&json!({ "template": "$${name} costs $5, $${env:X}" })).await;
    let sent: serde_json::Value = serde_json::from_str(&sent.expect("sent")).expect("JSON");
    assert_eq!(sent, json!({ "template": "${name} costs $5, ${env:X}" }));
}

#[test]
fn a_text_splits_into_its_literal_pieces_and_its_references() {
    let reference = |kind, name: &str| {
        Piece::Reference(SecretReference {
            kind,
            name: name.to_owned(),
        })
    };
    let text = |text: &str| Piece::Text(text.to_owned());
    assert_eq!(pieces(""), Ok(vec![]));
    assert_eq!(pieces("plain $ { }"), Ok(vec![text("plain $ { }")]));
    assert_eq!(
        pieces("a${env:B}c${file:/d e}${secret:f:g}"),
        Ok(vec![
            text("a"),
            reference(SecretKind::Env, "B"),
            text("c"),
            reference(SecretKind::File, "/d e"),
            reference(SecretKind::Named, "f:g"),
        ])
    );
    assert_eq!(pieces("$${env:B}"), Ok(vec![text("${env:B}")]));
    assert_eq!(pieces("$$${env:B}"), Ok(vec![text("$${env:B}")]));
    let longest = "n".repeat(SECRET_NAME_BYTES);
    assert!(pieces(&format!("${{env:{longest}}}")).is_ok());
    for (malformed, fault) in [
        ("${env:B", ReferenceFault::Unclosed),
        ("${", ReferenceFault::Unclosed),
        ("${}", ReferenceFault::Kind),
        ("${B}", ReferenceFault::Kind),
        ("${vault:B}", ReferenceFault::Kind),
        ("${ENV:B}", ReferenceFault::Kind),
        ("${env:}", ReferenceFault::Name),
        ("${env:a\nb}", ReferenceFault::Name),
        (&format!("${{env:{longest}n}}"), ReferenceFault::Name),
    ] {
        assert_eq!(pieces(malformed), Err(fault), "{malformed}");
    }
}

#[tokio::test]
async fn an_error_names_the_field_and_the_fault_and_never_what_the_field_holds() {
    let cases = [
        (
            json!({ "a": { "b": ["x", "hunter2 ${env:MISSING} canary"] } }),
            "a.b[1]",
            "secret_unresolved",
        ),
        (
            json!({ "a": "hunter2 ${vault:canary}" }),
            "a",
            "secret_reference",
        ),
        (
            json!({ "a": "hunter2 ${env:canary" }),
            "a",
            "secret_reference",
        ),
        (json!("hunter2 ${canary}"), ".", "secret_reference"),
        (
            json!(["${file:/hunter2/canary}"]),
            "[0]",
            "secret_unresolved",
        ),
    ];
    for (document, field, code) in cases {
        let (sent, _) = resolved(&document).await;
        let error = sent.expect_err("it is refused");
        assert_eq!(error.code(), code, "{document}");
        let mut said = format!("{error} {error:?}");
        if let Some(source) = std::error::Error::source(&error) {
            said.push_str(&source.to_string());
        }
        assert!(said.contains(&format!("config field {field}: ")), "{said}");
        assert!(
            !said.contains("hunter2") && !said.contains("canary"),
            "{said}"
        );
        assert!(!said.contains("MISSING"), "{said}");
    }
}

#[tokio::test]
async fn a_field_path_through_hostile_keys_is_shown_and_bounded() {
    let key = format!("\u{1b}[2J\u{202e}{}", "k".repeat(4096));
    let (sent, _) = resolved(&json!({ key: "${env:MISSING}" })).await;
    let said = sent.expect_err("refused").to_string();
    assert!(
        said.is_ascii() && !said.chars().any(char::is_control),
        "{said:?}"
    );
    assert!(said.len() < 512, "{}", said.len());
}

#[tokio::test]
async fn a_configuration_holds_a_bounded_number_of_references_and_bytes() {
    let references = |count: usize| json!(vec!["${secret:api-key}"; count]);
    let (within, _) = resolved(&references(SECRET_REFERENCES)).await;
    assert!(within.is_ok());
    let (beyond, _) = resolved(&references(SECRET_REFERENCES + 1)).await;
    assert!(matches!(beyond, Err(SecretError::TooMany { limit }) if limit == SECRET_REFERENCES));
    // One text of many references counts each.
    let (beyond, _) = resolved(&json!("${secret:api-key}".repeat(SECRET_REFERENCES + 1))).await;
    assert_eq!(beyond.expect_err("refused").code(), "config_invalid");
    let large = "x".repeat(CONFIG_BYTES);
    assert!(matches!(Config::parse(large), Err(SecretError::NotJson)));
    let large = format!("\"{}\"", "x".repeat(CONFIG_BYTES - 1));
    assert!(
        matches!(Config::parse(large), Err(SecretError::TooLarge { limit }) if limit == CONFIG_BYTES)
    );
    let fits = format!("\"{}\"", "x".repeat(CONFIG_BYTES - 2));
    assert!(Config::parse(fits).is_ok());
}

#[test]
fn a_configuration_shows_nothing_of_itself_and_is_copied_only_on_purpose() {
    let config = Config::parse(r#"{"password":"hunter2"}"#).expect("JSON");
    assert_eq!(format!("{config:?}"), "Config(***)");
    let copy = config.duplicate();
    drop(config);
    assert_eq!(format!("{copy:?}"), "Config(***)");
    for refused in ["", "{", "hunter2", r#"{"password":"hunter2""#] {
        let error = Config::parse(refused).expect_err("no JSON");
        assert_eq!(error.code(), "config_invalid");
        assert!(!format!("{error} {error:?}").contains("hunter2"));
    }
}

#[test]
fn redactions_replace_every_secret_the_longest_first_and_show_none() {
    let redactions = Redactions::new();
    assert_eq!(redactions.scrubbed("nothing yet".to_owned()), "nothing yet");
    for secret in ["pass", "password-long", "", "x"] {
        redactions.add(secret);
    }
    redactions.add("pass");
    assert_eq!(
        redactions.scrubbed("password-long pass passpass ax".to_owned()),
        "*** *** ****** a***"
    );
    assert_eq!(format!("{redactions:?}"), "Redactions(3)");
    // A secret that is written otherwise in JSON, by `Debug` or once shown has those forms.
    let quoted = Redactions::new();
    quoted.add("a\"b\u{1b}  c");
    assert_eq!(format!("{quoted:?}"), "Redactions(4)");
    for said in [
        "a\"b\u{1b}  c",
        r#"a\"b\u001b  c"#,
        r#"a\"b\u{1b}  c"#,
        r#"a"b\u{1b} c"#,
    ] {
        assert_eq!(quoted.scrubbed(format!("<{said}>")), "<***>", "{said}");
    }
    // A copy shares what is added after it was made.
    let shared = redactions.clone();
    redactions.add("later");
    assert_eq!(shared.scrubbed("later".to_owned()), "***");
}

fn variables(name: &OsStr) -> Option<OsString> {
    use std::os::unix::ffi::OsStringExt as _;
    match name.to_str()? {
        "DB_PASSWORD" => Some("hunter2".into()),
        "RDLT_SECRET_API_KEY_2" => Some("k-12345".into()),
        "NOT_TEXT" => Some(OsString::from_vec(vec![0xff, 0xfe])),
        "LONGEST" => Some("x".repeat(usize::try_from(SECRET_BYTES).ok()?).into()),
        "TOO_LONG" => Some("x".repeat(usize::try_from(SECRET_BYTES).ok()? + 1).into()),
        _ => None,
    }
}

async fn resolve(
    resolver: &dyn SecretResolver,
    kind: SecretKind,
    name: &str,
) -> Result<String, SecretFault> {
    let name = name.to_owned();
    let secret = resolver.resolve(&SecretReference { kind, name }).await?;
    Ok(secret.expose().clone())
}

#[tokio::test]
async fn the_environment_resolves_only_the_variables_its_operator_lists() {
    let listed = EnvSecrets::allowing(["DB_PASSWORD", "LONGEST", "NOT_TEXT", "TOO_LONG", "UNSET"])
        .reading(variables);
    let prefixed = EnvSecrets::prefixed("DB_").reading(variables);
    let found = resolve(&listed, SecretKind::Env, "DB_PASSWORD").await;
    assert_eq!(found.expect("listed"), "hunter2");
    let found = resolve(&prefixed, SecretKind::Env, "DB_PASSWORD").await;
    assert_eq!(found.expect("under the prefix"), "hunter2");
    let longest = resolve(&listed, SecretKind::Env, "LONGEST").await;
    assert_eq!(
        longest.expect("set").len(),
        usize::try_from(SECRET_BYTES).expect("fits")
    );
    for (resolver, name) in [
        (&listed, "HOME"),
        (&listed, "DB_PASSWORDX"),
        (&prefixed, "HOME"),
    ] {
        let fault = resolve(resolver, SecretKind::Env, name)
            .await
            .expect_err("not listed");
        assert!(matches!(fault, SecretFault::Refused), "{name}: {fault:?}");
    }
    // An empty prefix lists nothing.
    let empty = EnvSecrets::prefixed("").reading(variables);
    let fault = resolve(&empty, SecretKind::Env, "DB_PASSWORD")
        .await
        .expect_err("nothing");
    assert!(matches!(fault, SecretFault::Refused));
    let fault = resolve(&listed, SecretKind::Env, "UNSET")
        .await
        .expect_err("unset");
    assert!(matches!(fault, SecretFault::Missing));
    let fault = resolve(&listed, SecretKind::Env, "NOT_TEXT")
        .await
        .expect_err("no text");
    assert!(matches!(fault, SecretFault::NotText), "{fault:?}");
    let fault = resolve(&listed, SecretKind::Env, "TOO_LONG")
        .await
        .expect_err("too long");
    assert!(matches!(fault, SecretFault::TooLong { limit } if limit == SECRET_BYTES));
    for kind in [SecretKind::File, SecretKind::Named] {
        let fault = resolve(&listed, kind, "DB_PASSWORD")
            .await
            .expect_err("no such kind");
        assert!(matches!(fault, SecretFault::Refused));
    }
    // The host's own environment is what is read where nothing else is said.
    let own = EnvSecrets::allowing(["CARGO_PKG_NAME"]);
    let own = resolve(&own, SecretKind::Env, "CARGO_PKG_NAME").await;
    assert_eq!(own.expect("cargo sets it"), "rdlt-host");
}

#[tokio::test]
async fn named_secrets_come_from_the_store_the_operator_gives_and_from_nothing_else() {
    let by_prefix = Secrets::new().named(EnvSecrets::named("RDLT_SECRET_").reading(variables));
    let found = resolve(&by_prefix, SecretKind::Named, "api-key.2").await;
    assert_eq!(found.expect("set"), "k-12345");
    let fault = resolve(&by_prefix, SecretKind::Env, "DB_PASSWORD")
        .await
        .expect_err("no env");
    assert!(matches!(fault, SecretFault::Refused));
    // An empty prefix reaches no variable, as `DB_PASSWORD` would be reached by `db_password`.
    let unprefixed = Secrets::new().named(EnvSecrets::named("").reading(variables));
    let fault = resolve(&unprefixed, SecretKind::Named, "db_password")
        .await
        .expect_err("no prefix, nothing");
    assert!(matches!(fault, SecretFault::Refused), "{fault:?}");
    let vaulted = Secrets::new().named(vault());
    let found = resolve(&vaulted, SecretKind::Named, "api-key").await;
    assert_eq!(found.expect("the vault holds it"), "k-12345");
    // Not read from the environment once a store is given.
    let fault = resolve(&vaulted, SecretKind::Named, "api-key.2")
        .await
        .expect_err("unset");
    assert!(matches!(fault, SecretFault::Missing));
    let shown = format!("{vaulted:?}");
    assert!(
        shown.contains("named: true") && shown.contains("env: None"),
        "{shown}"
    );
}

#[tokio::test]
async fn by_default_no_reference_of_any_kind_resolves() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let private = file(directory.path(), "secret", b"top-secret", 0o600);
    for (kind, name) in [
        (SecretKind::Env, "HOME"),
        (SecretKind::Env, "CARGO_PKG_NAME"),
        (SecretKind::File, private.as_str()),
        (SecretKind::Named, "anything"),
    ] {
        let fault = resolve(&Secrets::new(), kind, name)
            .await
            .expect_err("refused");
        assert!(matches!(fault, SecretFault::Refused), "{kind}: {fault:?}");
    }
}

#[tokio::test]
async fn a_configuration_author_reaches_no_secret_of_the_host_the_operator_did_not_list() {
    let outside = tempfile::tempdir().expect("a temporary directory");
    let listed = tempfile::tempdir().expect("a temporary directory");
    let private = file(outside.path(), "secret", b"top-secret", 0o600);
    file(listed.path(), "token", b"t-123", 0o600);
    let document = serde_json::json!({
        "password": format!("${{file:{private}}}"),
        "home": "${env:HOME}",
    });
    let scoped = Secrets::new()
        .env(EnvSecrets::allowing(["PGPASSWORD"]))
        .files(FileSecrets::within([listed.path()]));
    for secrets in [Secrets::new(), scoped.clone()] {
        let redactions = Redactions::new();
        let sent = Config::from(&document)
            .resolved(&secrets, &redactions)
            .await;
        let error = sent.expect_err("refused");
        assert_eq!(error.code(), "secret_refused");
        let said = format!("{error} {error:?}");
        assert!(said.contains("config field "), "{said}");
        assert!(
            !said.contains("top-secret") && !said.contains(&private) && !said.contains("HOME"),
            "{said}"
        );
    }
    let token = format!("${{file:{}}}", listed.path().join("token").display());
    let sent = Config::from(&serde_json::json!({ "token": token }))
        .resolved(&scoped, &Redactions::new())
        .await;
    assert_eq!(sent.expect("listed").as_str(), r#"{"token":"t-123"}"#);
}

#[tokio::test]
async fn a_file_reference_reaches_only_what_lies_beneath_a_listed_directory_through_no_link() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let listed = root.path().join("listed");
    std::fs::create_dir_all(listed.join("sub")).expect("directories");
    file(&listed.join("sub"), "token", b"t-123", 0o600);
    file(root.path(), "beside", b"top-secret", 0o600);
    std::os::unix::fs::symlink(root.path(), listed.join("up")).expect("a link");
    let files = FileSecrets::within([&listed]);
    let reach = |path: std::path::PathBuf| {
        let files = files.clone();
        async move { resolve(&files, SecretKind::File, path.to_str().expect("text")).await }
    };
    assert_eq!(
        reach(listed.join("sub/token")).await.expect("beneath"),
        "t-123"
    );
    for (path, refused) in [
        (listed.join("../beside"), true),
        (listed.join("sub/../../beside"), true),
        (root.path().join("beside"), true),
        (listed.clone(), true),
        (listed.join("up/beside"), false),
    ] {
        let fault = reach(path.clone()).await.expect_err("not reached");
        if refused {
            assert!(
                matches!(fault, SecretFault::Refused),
                "{}: {fault:?}",
                path.display()
            );
        } else {
            assert!(
                matches!(fault, SecretFault::NotRegular),
                "{}: {fault:?}",
                path.display()
            );
        }
    }
    // A listed directory another user may write is no place for secrets.
    std::fs::set_permissions(&listed, PermissionsExt::from_mode(0o777)).expect("its mode is set");
    let fault = reach(listed.join("sub/token")).await.expect_err("shared");
    assert!(matches!(fault, SecretFault::Shared { .. }), "{fault:?}");
}

/// A file in `directory` holding `text`, with `mode`.
fn file(directory: &std::path::Path, name: &str, text: &[u8], mode: u32) -> String {
    let path = directory.join(name);
    std::fs::write(&path, text).expect("the file writes");
    std::fs::set_permissions(&path, PermissionsExt::from_mode(mode)).expect("its mode is set");
    path.to_str().expect("a path of text").to_owned()
}

#[tokio::test]
async fn a_private_file_resolves_to_its_text_without_the_line_end_at_its_end() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let at = |name, text: &[u8]| file(directory.path(), name, text, 0o600);
    for (name, text, secret) in [
        ("plain", &b"hunter2"[..], "hunter2"),
        ("line", b"hunter2\n", "hunter2"),
        ("crlf", b"hunter2\r\n", "hunter2"),
        ("lines", b"hunter\n2\n\n", "hunter\n2\n"),
        ("empty", b"", ""),
    ] {
        let files = FileSecrets::within([directory.path()]);
        let secrets = Secrets::new().files(files.clone());
        for resolver in [&files as &dyn SecretResolver, &secrets] {
            let found = resolve(resolver, SecretKind::File, &at(name, text)).await;
            assert_eq!(found.expect("it reads"), secret, "{name}");
        }
    }
    let read_only = file(directory.path(), "read-only", b"hunter2", 0o400);
    assert_eq!(
        resolve(
            &FileSecrets::within([directory.path()]),
            SecretKind::File,
            &read_only
        )
        .await
        .expect("it reads"),
        "hunter2"
    );
    let longest = usize::try_from(SECRET_BYTES).expect("fits");
    let files = FileSecrets::within([directory.path()]);
    let found = resolve(
        &files,
        SecretKind::File,
        &at("longest", &vec![b'x'; longest]),
    )
    .await;
    assert_eq!(found.expect("it reads").len(), longest);
    let found = resolve(
        &files,
        SecretKind::File,
        &at("ended", &[vec![b'x'; longest], b"\r\n".to_vec()].concat()),
    )
    .await;
    assert_eq!(found.expect("it reads").len(), longest);
}

#[tokio::test]
async fn a_file_that_is_not_private_regular_text_within_the_limit_is_refused() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let private = file(directory.path(), "private", b"hunter2", 0o600);
    let files = FileSecrets::within([directory.path()]);
    let fault = |path: String| {
        let files = files.clone();
        async move {
            resolve(&files, SecretKind::File, &path)
                .await
                .expect_err("it is refused")
        }
    };
    for mode in [
        0o640, 0o604, 0o620, 0o602, 0o610, 0o601, 0o644, 0o666, 0o4640,
    ] {
        let shared = file(directory.path(), "shared", b"hunter2", mode);
        let refused = fault(shared).await;
        assert!(
            matches!(refused, SecretFault::Shared { mode: found, .. } if found == mode),
            "{mode:o}: {refused:?}"
        );
    }
    let linked = directory.path().join("link");
    std::os::unix::fs::symlink(&private, &linked).expect("a link");
    let linked = linked.to_str().expect("text").to_owned();
    assert!(matches!(fault(linked).await, SecretFault::NotRegular));
    std::fs::create_dir(directory.path().join("inside")).expect("a directory");
    let inside = directory
        .path()
        .join("inside")
        .to_str()
        .expect("text")
        .to_owned();
    assert!(matches!(fault(inside).await, SecretFault::NotRegular));
    let pipe = directory.path().join("pipe");
    let made = std::process::Command::new("mkfifo").arg(&pipe).status();
    assert!(made.expect("mkfifo runs").success());
    let pipe = pipe.to_str().expect("text").to_owned();
    // Refused at once: nothing waits for a writer.
    assert!(matches!(fault(pipe).await, SecretFault::NotRegular));
    assert!(matches!(
        fault("relative/path".to_owned()).await,
        SecretFault::Relative
    ));
    let absent = format!("{}/absent", directory.path().display());
    assert!(matches!(fault(absent).await, SecretFault::Missing));
}

#[tokio::test]
async fn a_private_file_that_holds_no_text_or_too_much_is_refused_and_no_fault_says_what_is_there()
{
    let directory = tempfile::tempdir().expect("a temporary directory");
    let files = FileSecrets::within([directory.path()]);
    let fault = |path: String| {
        let files = files.clone();
        async move {
            resolve(&files, SecretKind::File, &path)
                .await
                .expect_err("it is refused")
        }
    };
    let binary = file(directory.path(), "binary", &[0xff, 0xfe], 0o600);
    assert!(matches!(fault(binary).await, SecretFault::NotText));
    let longest = usize::try_from(SECRET_BYTES).expect("fits");
    for excess in [1, 2, 3, 4096] {
        let long = file(
            directory.path(),
            "long",
            &vec![b'x'; longest + excess],
            0o600,
        );
        let refused = fault(long).await;
        assert!(
            matches!(refused, SecretFault::TooLong { limit } if limit == SECRET_BYTES),
            "{excess}"
        );
    }
    let unsupported = resolve(&files, SecretKind::Env, "HOME")
        .await
        .expect_err("no env");
    assert!(matches!(unsupported, SecretFault::Refused));
    // No fault says what a file holds or where it is.
    let refused = fault(file(directory.path(), "shared", b"hunter2", 0o644)).await;
    let said = format!("{refused} {refused:?}");
    assert!(
        !said.contains("hunter2") && !said.contains("shared"),
        "{said}"
    );
}

#[test]
fn the_part_of_a_secret_left_where_a_text_was_cut_is_scrubbed_too() {
    use rdlt_connector::text::{CUT, shown};
    let redactions = Redactions::new();
    redactions.add("hunter2");
    redactions.add("pä55wörd");
    // Cut at every point within each secret, as a text bounded where it is shown is.
    for secret in ["hunter2", "pä55wörd"] {
        for kept in 0..=secret.len() {
            let said = format!("the password is {secret} and more follows it");
            let cut = shown(&said, "the password is ".len() + kept + CUT.len());
            let scrubbed = redactions.scrubbed(cut.clone());
            let part = &secret[..secret.floor_char_boundary(kept)];
            assert!(scrubbed.ends_with(CUT), "{cut}");
            if part.is_empty() {
                assert_eq!(scrubbed, cut);
            } else {
                assert_eq!(scrubbed, format!("the password is ***{CUT}"), "{cut}");
            }
        }
    }
    // Each cut of a text of many is scrubbed, and what no cut follows is left as it is.
    let report = format!("a hunt{CUT})\nb pä5{CUT})\nc hunt and pä5\nd{CUT}");
    assert_eq!(
        redactions.scrubbed(report),
        format!("a ***{CUT})\nb ***{CUT})\nc hunt and pä5\nd{CUT}")
    );
}

#[test]
fn the_part_of_a_secret_left_where_a_beginning_was_dropped_is_scrubbed_too() {
    let redactions = Redactions::new();
    redactions.add("hunter2");
    for dropped in 0.."hunter2".len() {
        let left = &"hunter2"[dropped..];
        let scrubbed = redactions.scrubbed_end(format!("{left} was the password"), true);
        assert_eq!(scrubbed, "*** was the password", "{left}");
        let marked = redactions.scrubbed(format!("[cut] {left} was the password"));
        assert_eq!(marked, "[cut] *** was the password", "{left}");
    }
    // A text whose beginning was not dropped starts with what it starts with.
    let whole = redactions.scrubbed_end("ter2 was said".to_owned(), false);
    assert_eq!(whole, "ter2 was said");
    let unrelated = redactions.scrubbed_end("was the password hunter2".to_owned(), true);
    assert_eq!(unrelated, "was the password ***");
}

/// A store that reads secrets from `directory`, as a store of files does.
#[derive(Debug)]
struct Filed(std::path::PathBuf);

impl SecretResolver for Filed {
    fn resolve<'a>(
        &'a self,
        _reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async { Err(SecretFault::Missing) })
    }

    fn directories(&self) -> Vec<std::path::PathBuf> {
        vec![self.0.clone()]
    }
}

#[test]
fn the_resolvers_list_every_directory_they_read_secrets_from() {
    let (listed, stored) = (std::path::PathBuf::from("/files"), "/stored".into());
    let both = Secrets::new()
        .env(EnvSecrets::allowing(["HOME"]))
        .files(FileSecrets::within([listed.clone()]))
        .named(Filed(std::path::PathBuf::from("/stored")));
    assert_eq!(both.directories(), [listed.clone(), stored]);
    assert_eq!(
        FileSecrets::within([listed.clone()]).directories(),
        [listed]
    );
    assert!(Secrets::new().directories().is_empty());
    assert!(EnvSecrets::prefixed("RDLT_").directories().is_empty());
    assert!(vault().directories().is_empty());
}
