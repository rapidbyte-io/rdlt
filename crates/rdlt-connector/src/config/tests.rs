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

#[test]
fn valid_configuration_parses() {
    let config: Config = parse(json!({"host": "db", "tls": {"verify": true}})).unwrap();
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
        let error = parse::<Config>(config).unwrap_err();
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
