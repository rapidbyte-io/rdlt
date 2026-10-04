use std::sync::Arc;

use rdlt_engine::SystemClock;
use rdlt_host::Secrets;
use serde_json::{Value, json};

use super::{LogStoreConfig, S3Config};
use crate::LogStoreErrorKind;

/// An S3 configuration of every required field, `changes` set over it.
fn s3(changes: &Value) -> Value {
    let mut config = json!({
        "bucket": "rdlt-logs",
        "prefix": "pipelines/logs",
        "region": "eu-west-1",
        "access_key_id": "${secret:s3_key_id}",
        "secret_access_key": "${file:/run/secrets/s3}",
    });
    for (key, value) in changes.as_object().expect("an object") {
        config[key] = value.clone();
    }
    json!({ "s3": config })
}

fn parsed(document: &Value) -> S3Config {
    match LogStoreConfig::parse(document).expect("parses") {
        LogStoreConfig::S3(config) => config,
        LogStoreConfig::Local { .. } => panic!("not an S3 configuration"),
    }
}

#[test]
fn a_configuration_names_a_local_directory_or_an_s3_bucket() {
    let local = LogStoreConfig::parse(&json!({ "local": { "base": "/var/lib/rdlt/logs" } }));
    assert_eq!(
        local.expect("parses"),
        LogStoreConfig::Local {
            base: "/var/lib/rdlt/logs".into()
        }
    );
    let config = parsed(&s3(&json!({
        "endpoint": "https://s3.example.com:9000",
        "path_style": true,
        "session_token": "${env:S3_TOKEN}",
        "part_bytes": 16 << 20,
    })));
    assert_eq!(
        config.endpoint.as_deref(),
        Some("https://s3.example.com:9000")
    );
    assert!(config.path_style);
    let checked = config.checked().expect("valid");
    assert!(!checked.endpoint.expect("an endpoint").plaintext);
    assert!(checked.credentials.token.is_some());
    assert_eq!(
        checked.part_bytes.map(std::num::NonZero::get),
        Some(16 << 20)
    );
    let plain = parsed(&s3(&json!({}))).checked().expect("valid");
    assert!(plain.endpoint.is_none() && plain.part_bytes.is_none());
}

#[test]
fn a_document_that_is_no_log_store_s_is_refused_without_its_values() {
    for document in [
        json!({ "s3": { "bucket": "AKIASECRETVALUE" } }),
        s3(&json!({ "path_style": "AKIASECRETVALUE" })),
        s3(&json!({ "unknown": "AKIASECRETVALUE" })),
        json!({ "ftp": { "host": "AKIASECRETVALUE" } }),
        json!("AKIASECRETVALUE"),
    ] {
        let error = LogStoreConfig::parse(&document).expect_err("refused");
        assert_eq!(error.code(), "config_invalid", "{document}");
        assert_eq!(error.kind(), LogStoreErrorKind::Config);
        let shown = format!("{error} {error:?}");
        assert!(!shown.contains("AKIASECRETVALUE"), "{shown}");
    }
}

/// Changes over [`s3`] a bucket cannot take, each with the field it names.
fn refusals() -> Vec<(Value, &'static str)> {
    let long = "a".repeat(64);
    vec![
        (json!({ "bucket": "ab" }), "bucket"),
        (json!({ "bucket": long }), "bucket"),
        (json!({ "bucket": "Rdlt-logs" }), "bucket"),
        (json!({ "bucket": "-rdlt" }), "bucket"),
        (json!({ "bucket": "rdlt-" }), "bucket"),
        (json!({ "bucket": "rdlt..logs" }), "bucket"),
        (json!({ "bucket": "rdlt_logs" }), "bucket"),
        (json!({ "bucket": "192.168.1.1" }), "bucket"),
        (json!({ "region": "" }), "region"),
        (json!({ "region": "EU-west-1" }), "region"),
        (json!({ "region": "eu west" }), "region"),
        (json!({ "region": "a".repeat(65) }), "region"),
        (json!({ "endpoint": "not an address" }), "endpoint"),
        (json!({ "endpoint": "ftp://s3.example.com" }), "endpoint"),
        (json!({ "endpoint": "http://s3.example.com" }), "endpoint"),
        (json!({ "endpoint": "http://10.0.0.1:9000" }), "endpoint"),
        (json!({ "endpoint": "http://localhost:9000" }), "endpoint"),
        (
            json!({ "endpoint": "https://user:pw@s3.example.com" }),
            "endpoint",
        ),
        (
            json!({ "endpoint": "https://s3.example.com/bucket" }),
            "endpoint",
        ),
        (
            json!({ "endpoint": "https://s3.example.com?x=1" }),
            "endpoint",
        ),
        (
            json!({ "endpoint": "https://s3.example.com#x" }),
            "endpoint",
        ),
        (
            json!({ "access_key_id": "AKIASECRETVALUE" }),
            "access_key_id",
        ),
        (
            json!({ "secret_access_key": "AKIASECRETVALUE" }),
            "secret_access_key",
        ),
        (
            json!({ "secret_access_key": "a${env:X}" }),
            "secret_access_key",
        ),
        (
            json!({ "session_token": "AKIASECRETVALUE" }),
            "session_token",
        ),
        (json!({ "part_bytes": (5 << 20) - 1 }), "part_bytes"),
        (json!({ "part_bytes": (5_u64 << 30) + 1 }), "part_bytes"),
    ]
}

#[test]
fn every_field_a_bucket_cannot_take_is_named_and_its_value_never_shown() {
    for (changes, field) in refusals() {
        let error = parsed(&s3(&changes)).checked().expect_err("refused");
        assert_eq!(error.code(), "log_store_config", "{changes}");
        assert!(
            matches!(error, crate::LogStoreError::Config { field: named, .. } if named == field),
            "{changes}: {error:?}"
        );
        assert!(!format!("{error} {error:?}").contains("AKIASECRETVALUE"));
    }
}

#[test]
fn only_an_endpoint_at_a_loopback_ip_address_is_reached_without_tls() {
    for (changes, plaintext) in [
        (
            json!({ "endpoint": "http://127.0.0.1:9000", "path_style": true }),
            true,
        ),
        (
            json!({ "endpoint": "http://127.8.0.1", "path_style": true }),
            true,
        ),
        (
            json!({ "endpoint": "http://[::1]:9000", "path_style": true }),
            true,
        ),
        (
            json!({ "endpoint": "https://10.0.0.1:9000/", "path_style": true }),
            false,
        ),
        (json!({ "part_bytes": 5 << 20 }), false),
        (json!({ "part_bytes": 5_u64 << 30 }), false),
        (json!({ "bucket": "a.b-c9" }), false),
    ] {
        let checked = parsed(&s3(&changes)).checked().expect("valid");
        let reached = checked.endpoint.is_some_and(|endpoint| endpoint.plaintext);
        assert_eq!(reached, plaintext, "{changes}");
    }
}

#[tokio::test]
async fn a_local_configuration_opens_its_directory() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let config = LogStoreConfig::Local {
        base: base.path().join("logs"),
    };
    let store = config
        .open(Arc::new(Secrets::new()), Arc::new(SystemClock))
        .await
        .expect("opens");
    let pipeline = rdlt_connector::PipelineId::parse("local").expect("a valid pipeline");
    assert_eq!(store.loads(&pipeline).await.expect("lists"), []);
    assert!(store.chunk_bytes().is_none());
}

#[test]
fn a_bucket_not_named_in_the_path_is_named_in_the_endpoint_s_host() {
    for (changes, url) in [
        (
            json!({ "endpoint": "https://s3.example.com:9000" }),
            "https://rdlt-logs.s3.example.com:9000",
        ),
        (
            json!({ "endpoint": "https://nyc3.digitaloceanspaces.com/" }),
            "https://rdlt-logs.nyc3.digitaloceanspaces.com",
        ),
        (
            json!({ "endpoint": "https://s3.example.com:9000", "path_style": true }),
            "https://s3.example.com:9000",
        ),
    ] {
        let checked = parsed(&s3(&changes)).checked().expect("valid");
        assert_eq!(checked.endpoint.expect("an endpoint").url, url, "{changes}");
    }
}
