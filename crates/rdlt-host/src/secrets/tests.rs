use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::fs::PermissionsExt;

use proptest::prelude::*;
use rdlt_connector::{BoxFuture, Secret};
use serde_json::json;

use super::reference::{Piece, pieces};
use super::{
    Config, EnvSecrets, FileSecrets, Redactions, ReferenceFault, SecretError, SecretFault,
    SecretKind, SecretReference, SecretResolver, Secrets, Wiping, wipe,
};
use crate::limits::{SECRET_BYTES, SECRET_NAME_BYTES, SECRET_REFERENCES};
use rdlt_wire::limits::CONFIG_BYTES;

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

/// A secret longer than a reference to it.
const LONG: &str = "a secret much longer than the reference that names it, sixty-four";

fn vault() -> Vault {
    Vault(BTreeMap::from([
        (("env", "DB_PASSWORD"), "hunter2"),
        (("file", "/run/secrets/token"), "tok\"en\\9"),
        (("secret", "api-key"), "k-12345"),
        (("secret", "empty"), ""),
        (("secret", "long"), LONG),
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

#[tokio::test]
async fn a_text_is_rewritten_only_where_a_reference_or_a_doubled_literal_changes_it() {
    let (vault, redactions) = (vault(), Redactions::new());
    for (text, sent) in [
        ("", None),
        ("plain $ { }", None),
        ("$${env:X}", Some("${env:X}")),
        ("key ${secret:api-key}, $${", Some("key k-12345, ${")),
    ] {
        let pieces = pieces(text).expect("well formed");
        let resolved = super::reference::resolved(&pieces, text, ".", &vault, &redactions).await;
        let resolved = resolved.map(|resolved| resolved.expect("it resolves").as_str().to_owned());
        assert_eq!(resolved.as_deref(), sent, "{text}");
    }
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
    let bytes = usize::try_from(CONFIG_BYTES).unwrap();
    let large = "x".repeat(bytes);
    assert!(matches!(Config::parse(large), Err(SecretError::NotJson)));
    let large = format!("\"{}\"", "x".repeat(bytes - 1));
    assert!(
        matches!(Config::parse(large), Err(SecretError::TooLarge { limit }) if limit == CONFIG_BYTES)
    );
    let fits = format!("\"{}\"", "x".repeat(bytes - 2));
    assert!(Config::parse(fits).is_ok());
    // A configuration within the bound whose secrets take it beyond is refused as it is sent:
    // a text of its quotes, the secret and the rest, in exactly the bound and one beyond.
    let around = |rest: usize| json!(format!("{}${{secret:long}}", "x".repeat(rest)));
    let exactly = bytes - 2 - LONG.len();
    let (sent, _) = resolved(&around(exactly)).await;
    assert_eq!(sent.expect("within the bound").len(), bytes);
    let (beyond, _) = resolved(&around(exactly + 1)).await;
    assert!(matches!(beyond, Err(SecretError::TooLarge { limit }) if limit == CONFIG_BYTES));
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
fn a_line_of_a_secret_is_found_on_its_own_from_eight_characters_other_than_whitespace() {
    let whole = "seven-7\n  eight-88  \n   seven-7 \nshort";
    let redactions = Redactions::new();
    redactions.add(whole);
    assert_eq!(
        redactions.scrubbed("seven-7|  eight-88  |   seven-7 |short".to_owned()),
        "seven-7|***|   seven-7 |short"
    );
    // The whole secret is found wherever it is.
    assert_eq!(redactions.scrubbed(format!("<{whole}>")), "<***>");
}

#[test]
fn redactions_replace_every_secret_whole_and_show_none() {
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
    // As JSON writes one that ends in a quote, it ends in the quote's escape.
    let ending = Redactions::new();
    ending.add("a\u{1b}b\"");
    assert_eq!(ending.scrubbed(r#"<a\u001bb\">"#.to_owned()), "<***>");
    // A copy shares what is added after it was made.
    let shared = redactions.clone();
    redactions.add("later");
    assert_eq!(shared.scrubbed("later".to_owned()), "***");
}

#[test]
fn a_secret_that_holds_the_mask_is_not_made_again_by_its_own_replacement() {
    use super::redact::{STEPS, WITHHELD};
    let redactions = Redactions::new();
    redactions.add("a***b");
    // Each step takes one depth away, and the last finds nothing.
    for depth in 1..=STEPS + 1 {
        let said = format!("<{}***{}>", "a".repeat(depth), "b".repeat(depth));
        let scrubbed = if depth < STEPS { "<***>" } else { WITHHELD };
        assert_eq!(redactions.scrubbed(said.clone()), scrubbed, "{said}");
    }
}

#[test]
fn a_replacement_at_a_cut_that_leaves_the_start_of_another_secret_is_replaced_in_turn() {
    use rdlt_connector::text::CUT;
    let redactions = Redactions::new();
    for secret in ["xfoo", "ab***c"] {
        redactions.add(secret);
    }
    // `x` is the start of `xfoo`, and its `***` makes `ab***`, the start of `ab***c`.
    assert_eq!(
        redactions.scrubbed(format!("<abx{CUT}")),
        format!("<***{CUT}")
    );
    // `o` is the end of `xfoo`, and its `***` makes `***c`, the end of `ab***c`.
    let dropped = redactions.scrubbed_end("oc>".to_owned(), true);
    assert_eq!(dropped, "***>");
}

#[test]
fn a_text_that_a_few_steps_do_not_scrub_is_withheld_whole() {
    use super::redact::{STEPS, WITHHELD};
    let redactions = Redactions::new();
    for secret in ["a*", "b"] {
        redactions.add(secret);
    }
    // `b` becomes `***`, and then each step makes one more `a*` of the `a` before it.
    let said = |many: usize| format!("{}b", "a".repeat(many));
    let all = "*".repeat(2 * STEPS - 1);
    assert_eq!(redactions.scrubbed(said(STEPS - 2)), all);
    assert_eq!(redactions.scrubbed(said(STEPS - 1)), WITHHELD);
    assert_eq!(redactions.scrubbed(said(10_000)), WITHHELD);
    // What a cut was made to keep is withheld too.
    let marked = redactions.scrubbed_end(said(10_000), true);
    assert_eq!(marked, WITHHELD);
}

#[test]
fn a_replacement_that_completes_another_secret_is_replaced_in_turn() {
    use rdlt_connector::text::CUT;
    let redactions = Redactions::new();
    for secret in ["hunter2", "a*", "x"] {
        redactions.add(secret);
    }
    assert_eq!(redactions.scrubbed("<ax>".to_owned()), "<*****>");
    // What replaces the part of a secret left at a cut may complete one too.
    assert_eq!(
        redactions.scrubbed(format!("<ahun{CUT}")),
        format!("<*****{CUT}")
    );
}

#[test]
fn a_secret_or_a_line_of_nothing_but_the_mask_and_whitespace_is_never_found() {
    let redactions = Redactions::new();
    redactions.add("hunter-canary\n********\n ** ** ** ** ");
    for said in ["<********>", "< ** ** ** ** >", "<***>"] {
        assert_eq!(redactions.scrubbed(said.to_owned()), said);
    }
    assert_eq!(redactions.scrubbed("<hunter-canary>".to_owned()), "<***>");
    let alone = Redactions::new();
    for secret in ["*", "***", " * * ", "  "] {
        alone.add(secret);
    }
    assert_eq!(format!("{alone:?}"), "Redactions(0)");
}

#[tokio::test]
async fn a_secret_of_nothing_but_the_mask_and_whitespace_is_refused_as_it_resolves() {
    let document = json!({ "a": { "b": "${secret:it}" } });
    for secret in ["*", "***", "* *\n**", " \t "] {
        let vault = Vault(BTreeMap::from([(("secret", "it"), secret)]));
        let redactions = Redactions::new();
        let sent = Config::from(&document).resolved(&vault, &redactions).await;
        let error = sent.expect_err("refused");
        assert_eq!(error.code(), "secret_unredactable", "{secret:?}");
        let refused = matches!(&error, SecretError::Unredactable { field, kind }
            if field == "a.b" && *kind == SecretKind::Named);
        assert!(refused, "{error:?}");
        assert_eq!(format!("{redactions:?}"), "Redactions(0)", "{secret:?}");
    }
    // One with a character besides is sent, and scrubbed.
    let vault = Vault(BTreeMap::from([(("secret", "it"), "p**s *")]));
    let redactions = Redactions::new();
    let sent = Config::from(&document).resolved(&vault, &redactions).await;
    assert_eq!(sent.expect("sent").as_str(), r#"{"a":{"b":"p**s *"}}"#);
    assert_eq!(redactions.scrubbed("<p**s *>".to_owned()), "<***>");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// However a text is made of the pieces of secrets that hold the mask's `*`, the mask
    /// and the marks of a cut, no secret, and no part of one at a mark of a cut, is left in it
    /// once it is scrubbed, or it is withheld whole.
    #[test]
    fn a_scrubbed_text_holds_no_secret(
        secrets in prop::collection::vec(
            "[ab*]{1,5}".prop_filter("a character besides the mask's", |secret| {
                secret.contains(['a', 'b'])
            }),
            1..4,
        ),
        picks in prop::collection::vec((any::<prop::sample::Index>(), 0..4_u8), 0..12),
        dropped in any::<bool>(),
    ) {
        use super::redact::maskable;
        use rdlt_connector::text::CUT;
        let mut pieces: Vec<&str> = vec!["***", "*", "a", "b", "x", CUT, super::DROPPED];
        for secret in &secrets {
            for at in 0..=secret.len() {
                pieces.extend([&secret[..at], &secret[at..]]);
            }
        }
        // A piece is followed by a mark of a cut half the time.
        let marks = [CUT, super::DROPPED, "", ""];
        let text: String = picks
            .iter()
            .flat_map(|(pick, mark)| [*pick.get(&pieces), marks[usize::from(*mark)]])
            .collect();
        let redactions = Redactions::new();
        for secret in &secrets {
            redactions.add(secret);
        }
        let scrubbed = redactions.scrubbed_end(text.clone(), dropped);
        if scrubbed != super::redact::WITHHELD {
            let marked = if dropped { format!("{}{scrubbed}", super::DROPPED) } else { scrubbed };
            for secret in &secrets {
                prop_assert!(
                    !marked.contains(secret.as_str()),
                    "{secret:?} in {marked:?} from {text:?}"
                );
                for at in 1..secret.len() {
                    let (start, end) = secret.split_at(at);
                    let cut = format!("{start}{CUT}");
                    let begun = format!("{}{end}", super::DROPPED);
                    for part in [(start, &cut), (end, &begun)] {
                        prop_assert!(
                            !maskable(part.0) || !marked.contains(part.1.as_str()),
                            "{:?} at a cut in {marked:?} from {text:?}", part.0
                        );
                    }
                }
            }
        }
    }
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
async fn a_pipe_on_the_way_to_a_secret_is_refused_at_once() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let pipe = directory.path().join("pipe");
    let made = std::process::Command::new("mkfifo").arg(&pipe).status();
    assert!(made.expect("mkfifo runs").success());
    let files = FileSecrets::within([directory.path()]);
    let beyond = pipe.join("token").to_str().expect("text").to_owned();
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        resolve(&files, SecretKind::File, &beyond),
    )
    .await;
    if read.is_err() {
        // An open that waits for a writer is given one, so the test ends.
        drop(std::fs::OpenOptions::new().write(true).open(&pipe));
    }
    let refused = read.expect("refused at once").expect_err("refused");
    assert!(matches!(refused, SecretFault::NotRegular), "{refused:?}");
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

#[test]
fn a_secret_and_the_part_of_one_at_a_cut_that_overlap_are_hidden_whole() {
    use rdlt_connector::text::CUT;
    let redactions = Redactions::new();
    for secret in ["ab\nfirst-line-0\nyz", "abcdef12", "cdef12gh"] {
        redactions.add(secret);
    }
    let cases = [
        // A line of a secret, a secret of its own, within the part of it a cut left.
        (format!(r"<ab\nfirst-line-0\ny{CUT}"), format!("<***{CUT}")),
        (
            r"[cut] b\nfirst-line-0\nyz>".to_owned(),
            "[cut] ***>".to_owned(),
        ),
        // The part of a secret at a cut within another secret.
        (format!("<abcdef12{CUT}"), format!("<***{CUT}")),
        ("[cut] cdef12gh>".to_owned(), "[cut] ***>".to_owned()),
    ];
    for (said, scrubbed) in cases {
        assert_eq!(redactions.scrubbed(said.clone()), scrubbed, "{said}");
    }
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

#[test]
fn a_text_that_is_one_reference_and_nothing_else_parses_as_it() {
    let named = |kind, name: &str| SecretReference {
        kind,
        name: name.to_owned(),
    };
    for (text, reference) in [
        ("${env:S3_KEY}", named(SecretKind::Env, "S3_KEY")),
        (
            "${file:/run/secrets/s3}",
            named(SecretKind::File, "/run/secrets/s3"),
        ),
        ("${secret:s3.key}", named(SecretKind::Named, "s3.key")),
    ] {
        assert_eq!(SecretReference::parse(text), Ok(reference), "{text}");
    }
    for (text, fault) in [
        ("AKIAEXAMPLE", ReferenceFault::NotOne),
        ("", ReferenceFault::NotOne),
        ("key-${env:S3_KEY}", ReferenceFault::NotOne),
        ("${env:S3_KEY}${env:OTHER}", ReferenceFault::NotOne),
        ("${env:S3_KEY} ", ReferenceFault::NotOne),
        ("$${env:S3_KEY}", ReferenceFault::NotOne),
        ("${env:S3_KEY", ReferenceFault::Unclosed),
        ("${vault:S3_KEY}", ReferenceFault::Kind),
        ("${env:}", ReferenceFault::Name),
    ] {
        assert_eq!(SecretReference::parse(text), Err(fault), "{text}");
    }
}

#[test]
fn a_kind_shows_as_the_word_a_reference_names_it_by() {
    for kind in [SecretKind::Env, SecretKind::File, SecretKind::Named] {
        let reference = SecretReference::parse(&format!("${{{kind}:name}}"));
        assert_eq!(reference.map(|reference| reference.kind), Ok(kind));
    }
}

#[test]
fn wiping_a_document_empties_every_text_it_holds_at_every_depth() {
    let mut document = json!({
        "token": "s3cret",
        "nested": ["one", {"deeper": "two", "count": 3}],
        "flag": true,
        "none": null,
    });
    wipe(&mut document);
    assert_eq!(
        document,
        json!({
            "token": "",
            "nested": ["", {"deeper": "", "count": 3}],
            "flag": true,
            "none": null,
        })
    );
}

#[test]
fn a_document_under_resolution_is_wiped_however_its_resolution_ends() {
    let mut document = json!({ "token": "s3cret", "nested": ["one", {"deeper": "two"}] });
    drop(Wiping(&mut document));
    assert_eq!(
        document,
        json!({ "token": "", "nested": ["", {"deeper": ""}] })
    );
}
