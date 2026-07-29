//! No secret reaches anything a human or a log can read.
//!
//! Every credential this connector accepts is checked on every channel that
//! renders a value: `Debug`, serialization, and the text of an error. Errors
//! matter most and are the easiest to forget — a config never leaves the
//! process, while an error is printed, logged, and pasted into an issue.

use rdlt_connector_snowflake::dest::{Auth, KeyPair, Password, Snowflake, SnowflakeConfig};
use serde_json::json;

/// A marker per credential field, so a leak names which one leaked.
const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nLEAK-KEY-MATERIAL\n";
const PASSPHRASE: &str = "LEAK-PASSPHRASE";
const PASSWORD: &str = "LEAK-PASSWORD";
const PASSCODE: &str = "LEAK-PASSCODE";
const OAUTH: &str = "LEAK-OAUTH-TOKEN";
const PAT: &str = "LEAK-PAT";

const EVERY_SECRET: &[&str] = &[
    "LEAK-KEY-MATERIAL",
    PASSPHRASE,
    PASSWORD,
    PASSCODE,
    OAUTH,
    PAT,
];

fn config_with(auth: serde_json::Value) -> SnowflakeConfig {
    SnowflakeConfig::from_value(json!({
        "account": "MYORG-MYACCT",
        "user": "LOADER",
        "database": "D",
        "schema": "S",
        "auth": auth,
    }))
    .expect("valid config")
}

fn every_auth() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        (
            "key_pair",
            json!({"key_pair": {"private_key": KEY_PEM, "passphrase": PASSPHRASE}}),
        ),
        (
            "password",
            json!({"password": {"password": PASSWORD, "passcode": PASSCODE}}),
        ),
        ("oauth_token", json!({"oauth_token": OAUTH})),
        ("pat", json!({"pat": PAT})),
    ]
}

fn assert_clean(channel: &str, method: &str, rendered: &str) {
    for secret in EVERY_SECRET {
        assert!(
            !rendered.contains(secret),
            "`{secret}` leaked through {channel} for the {method} method:\n{rendered}"
        );
    }
}

#[test]
fn no_secret_survives_debug_rendering() {
    for (method, auth) in every_auth() {
        let config = config_with(auth);
        assert_clean("Debug", method, &format!("{config:?}"));
        // And through the destination that wraps it — a type that holds a
        // config derives its own Debug, and deriving it is exactly how a
        // redacted field gets un-redacted.
        let dest = Snowflake::new(config).expect("valid config");
        assert_clean("the destination's Debug", method, &format!("{dest:?}"));
    }
}

/// Serialization is the DOCUMENT, and it round-trips faithfully.
///
/// The one channel that deliberately keeps the values, which is worth stating
/// rather than leaving as an omission. Serde is how a pipeline document is
/// read and re-emitted; a config that redacted on the way out could be parsed
/// once and never written back, and the "redacted" placeholder would then be
/// loaded as the credential.
///
/// That is precisely why `Debug` — not serde — is the guarded channel: an
/// error report, a panic and a log line all reach for `Debug`, and none of
/// them is a document.
#[test]
fn serialization_keeps_the_values_because_it_is_the_document() {
    for (_, auth) in every_auth() {
        let config = config_with(auth);
        let json = serde_json::to_string(&config).expect("serializes");
        let reparsed = SnowflakeConfig::from_json(&json).expect("round-trips");
        assert_eq!(
            reparsed, config,
            "a config must survive being written and read back"
        );
    }
}

#[test]
fn no_secret_survives_a_validation_failure() {
    // A refused document is rendered somewhere by definition — that is what a
    // config error is FOR — so the refusal path needs the same guarantee as
    // the success path.
    for (method, auth) in every_auth() {
        let doc = json!({
            "account": "https://not-an-identifier.example",
            "user": "LOADER",
            "database": "D",
            "schema": "S",
            "auth": auth,
        });
        let err = SnowflakeConfig::from_value(doc).expect_err("a URL is not an identifier");
        assert_clean("a validation error", method, &err);
    }
}

#[test]
fn no_secret_survives_an_unknown_field_error() {
    // serde's own error text quotes the document it was reading, which is a
    // channel this crate does not write and must still not leak through.
    for (method, auth) in every_auth() {
        let doc = format!(
            r#"{{"account":"A","user":"U","database":"D","schema":"S","typo":1,"auth":{auth}}}"#
        );
        let err = SnowflakeConfig::from_json(&doc).expect_err("the typo is refused");
        assert_clean("a serde error", method, &err);
    }
}

#[test]
fn the_programmatic_constructors_redact_as_the_parsed_ones_do() {
    // An embedding application builds these directly rather than through a
    // document, and a constructor that stored a plain String would be a leak
    // reachable only from the library API — where no document test would look.
    let built = [
        format!(
            "{:?}",
            Auth::key_pair(KeyPair::new(KEY_PEM).with_passphrase(PASSPHRASE))
        ),
        format!(
            "{:?}",
            Auth::password(Password::new(PASSWORD).with_passcode(PASSCODE))
        ),
        format!("{:?}", Auth::oauth_token(OAUTH)),
        format!("{:?}", Auth::pat(PAT)),
    ];
    for rendered in built {
        assert_clean("a programmatic constructor", "library API", &rendered);
    }
}
