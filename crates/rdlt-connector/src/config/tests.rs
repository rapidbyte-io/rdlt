use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::{parse, schema};
use crate::error::ConnectorErrorKind;

#[derive(Debug, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
struct Config {
    host: String,
    tls: Tls,
}

#[derive(Debug, Deserialize, JsonSchema, PartialEq)]
struct Tls {
    verify: bool,
}

#[derive(Debug, Deserialize)]
struct Token {
    #[expect(dead_code, reason = "only parsing is under test")]
    token: crate::Secret<u64>,
}

#[test]
fn invalid_secret_values_are_not_quoted_in_errors() {
    let error = parse::<Token>(&json!({"token": "hunter2"})).unwrap_err();
    assert_eq!(error.code(), Some("config_invalid"));
    assert!(error.to_string().contains("token"), "{error}");
    assert!(!error.to_string().contains("hunter2"), "{error}");
}

#[test]
fn valid_configuration_parses() {
    let config: Config = parse(&json!({"host": "db", "tls": {"verify": true}})).unwrap();
    assert_eq!(
        config,
        Config {
            host: "db".to_owned(),
            tls: Tls { verify: true }
        }
    );
}

#[test]
fn errors_name_the_offending_field() {
    let cases = [
        (
            json!({"host": "db", "tls": {"verify": "yes"}}),
            "tls.verify",
        ),
        (json!({"host": 5, "tls": {"verify": true}}), "host"),
        (
            json!({"host": "db", "tls": {"verify": true}, "port": 1}),
            "port",
        ),
    ];
    for (config, field) in cases {
        let error = parse::<Config>(&config).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Config);
        assert_eq!(error.code(), Some("config_invalid"));
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[test]
fn the_schema_describes_the_configuration() {
    let schema = schema::<Config>();
    assert_eq!(schema["properties"]["host"]["type"], "string");
    assert_eq!(schema["required"], json!(["host", "tls"]));
}

mod quoting {
    use std::collections::BTreeMap;

    use serde::Deserialize;
    use serde_json::json;

    use super::super::parse;
    use crate::Secret;

    /// What no error may hold.
    const CANARY: &str = "hunter2-canary";

    #[derive(Debug, Deserialize)]
    #[expect(dead_code, reason = "only parsing is under test")]
    struct Auth {
        token: Secret<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case")]
    #[expect(dead_code, reason = "only parsing is under test")]
    enum Choice {
        Token(Secret<String>),
        Basic {
            user: String,
            password: Secret<String>,
        },
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case")]
    #[expect(dead_code, reason = "only parsing is under test")]
    enum Tagged {
        Token { token: Secret<String> },
    }

    /// A type whose own error quotes what it was given, as a driver's may.
    #[derive(Debug)]
    struct Url;

    impl<'de> Deserialize<'de> for Url {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            let given = String::deserialize(deserializer)?;
            Err(serde::de::Error::custom(format!("`{given}` is no URL")))
        }
    }

    #[derive(Debug, Default, Deserialize)]
    #[serde(default, deny_unknown_fields)]
    struct Config {
        port: Option<u16>,
        flag: Option<bool>,
        auth: Option<Auth>,
        choice: Option<Choice>,
        tagged: Option<Tagged>,
        tokens: Option<Vec<Secret<String>>>,
        headers: Option<BTreeMap<String, Secret<String>>>,
        pair: Option<(u8, u8)>,
        url: Option<Url>,
        password: Option<Secret<String>>,
        plain: Option<String>,
        letter: Option<char>,
    }

    #[derive(Debug, Deserialize)]
    #[expect(dead_code, reason = "only parsing is under test")]
    struct Flattened {
        #[serde(flatten)]
        auth: Auth,
    }

    #[test]
    fn no_error_quotes_a_value_of_any_field_whatever_is_wrong_with_its_shape() {
        let canary = json!(CANARY);
        let wrong = [
            // A value where a container is expected, one level above a secret.
            json!({ "auth": canary }),
            json!({ "tokens": canary }),
            json!({ "headers": canary }),
            json!({ "choice": canary }),
            json!({ "choice": { "token": [CANARY] } }),
            json!({ "choice": { "basic": canary } }),
            json!({ "choice": { CANARY: 1 } }),
            json!({ "tagged": { "kind": CANARY } }),
            json!({ "tagged": { "kind": "token", "token": [CANARY] } }),
            json!({ "tagged": canary }),
            // Fields that are no secrets to their connector.
            json!({ "port": canary }),
            json!({ "flag": canary }),
            json!({ "pair": [CANARY, 1] }),
            json!({ "pair": [1, 2, CANARY] }),
            json!({ "letter": canary }),
            json!({ "url": canary }),
            json!({ "plain": [CANARY] }),
            // A value of the wrong kind in a secret's own place.
            json!({ "password": [CANARY] }),
            json!({ "tokens": [[CANARY]] }),
            json!({ "headers": { "a": { "b": CANARY } } }),
            // The whole configuration given as one value.
            canary.clone(),
            json!([CANARY]),
            json!(format!("postgres://app:{CANARY}@db/prod")),
        ];
        for config in wrong {
            let error = parse::<Config>(&config).expect_err("it is refused");
            assert_eq!(error.code(), Some("config_invalid"), "{config}");
            assert!(std::error::Error::source(&error).is_none());
            let said = error.to_string();
            assert!(
                !said.contains("hunter2") && !said.contains("canary"),
                "{config}: {said}"
            );
            assert!(said.starts_with("config field "), "{said}");
        }
        let flattened = parse::<Flattened>(&json!({ "token": [CANARY] })).expect_err("refused");
        assert!(!flattened.to_string().contains("hunter2"), "{flattened}");
        let whole = parse::<Flattened>(&canary).expect_err("refused");
        assert!(!whole.to_string().contains("hunter2"), "{whole}");
    }

    #[test]
    fn an_error_says_which_field_what_is_wrong_and_the_kind_of_value_it_holds() {
        // The configuration, the field the error names, its fault, and the kind it holds.
        let cases = [
            (json!({ "port": "x" }), "port", "wrong type", "text"),
            (
                json!({ "port": 70000 }),
                "port",
                "not one the connector accepts",
                "a number",
            ),
            (json!({ "auth": [1] }), "auth[0]", "not valid", "a number"),
            // A secret that is missing says no more than one that is wrong.
            (json!({ "auth": {} }), "auth", "not valid", "an object"),
            (
                json!({ "choice": "x" }),
                "choice",
                "names no choice",
                "text",
            ),
            (
                json!({ "tokens": [true] }),
                "tokens[0]",
                "not valid",
                "a boolean",
            ),
            (
                json!({ "port": null, "flag": {} }),
                "flag",
                "wrong type",
                "an object",
            ),
            (json!({ "url": "x" }), "url", "not valid", "text"),
            (
                json!({ "pair": [1] }),
                "pair",
                "wrong number of items",
                "a list",
            ),
            (json!(null), ".", "wrong type", "null"),
        ];
        for (config, field, fault, holds) in cases {
            let said = parse::<Config>(&config).expect_err("refused").to_string();
            let start = format!("config field {field}: ");
            assert!(said.starts_with(&start) && said.contains(fault), "{said}");
            assert!(said.ends_with(&format!("(it holds {holds})")), "{said}");
        }
    }

    #[test]
    fn an_unknown_field_is_named_by_its_path_and_a_missing_one_by_the_connectors_name_for_it() {
        let said = |config| parse::<Config>(&config).expect_err("refused").to_string();
        let unknown = said(json!({ "extra": 1 }));
        assert!(unknown.starts_with("config field extra: the connector knows no such field"));
        let missing = said(json!({ "choice": { "basic": { "password": "x" } } }));
        assert!(
            missing.contains("lacks a required field, `user`"),
            "{missing}"
        );
    }

    #[test]
    fn a_name_is_read_from_a_message_only_when_it_reads_as_a_field() {
        use super::super::named;
        assert_eq!(named("`token`").as_deref(), Some("token"));
        assert_eq!(named("`a-b.c_9`, expected x").as_deref(), Some("a-b.c_9"));
        let long = format!("`{}`", "x".repeat(65));
        for unread in [
            "token",
            "``",
            "`hunter2 canary`",
            "`a/b`",
            "`é`",
            long.as_str(),
        ] {
            assert_eq!(named(unread), None, "{unread}");
        }
        assert_eq!(
            named(&format!("`{}`", "x".repeat(64))),
            Some("x".repeat(64))
        );
    }

    #[test]
    fn a_path_that_runs_through_hostile_keys_is_shown_and_bounded() {
        let key = format!("\u{1b}[2J\u{202e}{}", "k".repeat(4096));
        let said = parse::<Config>(&json!({ "headers": { key: [1] } })).expect_err("refused");
        let said = said.to_string();
        assert!(
            said.is_ascii() && !said.chars().any(char::is_control),
            "{said:?}"
        );
        assert!(said.len() < 512, "{}", said.len());
    }
}
