//! The quickstart's pipeline document, parsed.
//!
//! Documentation drifts silently: the shipped shape changes, the walkthrough
//! keeps the old one, and the first person to follow it hits an error the
//! project believes cannot happen. The quickstart carried exactly that — a
//! flat `private_key:` where the shipped vocabulary nests it under `auth:`,
//! and a `path:` prefix that never existed — so the document is now compiled.

use rdlt_connector_snowflake::dest::SnowflakeConfig;

/// Lifted verbatim from the `destination:` block of
/// `specs/023-snowflake-put/quickstart.md`.
const QUICKSTART_DESTINATION: &str = r#"
account: MYORG-MYACCT
user: LOADER
auth:
  key_pair:
    private_key: /etc/rdlt/snowflake.p8
    passphrase: ${SNOWFLAKE_KEY_PASSPHRASE}
database: ANALYTICS
schema: RAW
warehouse: LOADING_WH
role: LOADER_ROLE
"#;

/// The block a previous document carried. Kept verbatim as the thing that must now
/// be REFUSED — a removal nobody can test is a removal that quietly becomes
/// acceptance again.
const QUICKSTART_WITH_STAGE: &str = r#"
account: "MYORG-MYACCT"
user: "MY_LOADER"
auth:
  key_pair:
    private_key: "/home/you/.config/rdlt/snowflake/rdlt_key.p8"
    passphrase: "your-passphrase"
database: "ANALYTICS"
schema: "RAW"
merge_strategy: upsert
stage:
  s3:
    bucket: my-staging-bucket
    access_key: "..."
    secret_key: "..."
"#;

#[test]
fn the_quickstart_document_is_a_valid_configuration() {
    let config = SnowflakeConfig::from_yaml(QUICKSTART_DESTINATION)
        .expect("the documented document must parse");
    assert_eq!(config.host(), "myorg-myacct.snowflakecomputing.com");
    assert!(config.auth.key_pair.is_some());
    assert_eq!(config.warehouse.as_deref(), Some("LOADING_WH"));
    assert_eq!(config.role.as_deref(), Some("LOADER_ROLE"));
    // The walkthrough shows the WHOLE configuration and claims there is no
    // ingestion setting in it. Asserted rather than trusted: a field that
    // reappeared would make the sentence beside this document false.
    let rendered = serde_json::to_value(&config).expect("the config serializes");
    let fields: Vec<&String> = rendered
        .as_object()
        .expect("a document")
        .keys()
        .filter(|k| rendered[*k] != serde_json::Value::Null)
        .collect();
    assert!(
        !fields.iter().any(|k| k.contains("stage")),
        "no ingestion vocabulary survives: {fields:?}"
    );
    // The documented key value is a PATH, not inline material — the comment
    // beside it in the walkthrough says so, and this is what proves it.
    assert!(
        !config
            .auth
            .key_pair
            .expect("key pair")
            .private_key
            .is_inline()
    );
}

/// A document still carrying the removed storage block is REFUSED, by name.
///
/// The point of US2, and the reason the removal is not merely a deletion:
/// accepting the block and ignoring it would leave a user believing their data
/// travels through a bucket they configured, when it no longer does. The
/// configuration already refuses unknown fields, so the deletion alone
/// produces the refusal — no tombstone field is kept, which would put the
/// removed vocabulary back into the generated schema.
#[test]
fn a_document_carrying_the_removed_storage_block_is_refused_by_name() {
    let err = SnowflakeConfig::from_yaml(QUICKSTART_WITH_STAGE)
        .expect_err("the storage block no longer exists");
    assert!(
        err.contains("stage"),
        "the refusal must NAME the field, or a reader cannot tell which line to \
         delete: {err}"
    );
}

/// And the generated schema carries none of that vocabulary either.
#[test]
fn the_generated_schema_has_no_storage_vocabulary_left() {
    let rendered = serde_json::to_string(&rdlt_connector_snowflake::dest::config_schema())
        .expect("the schema renders");
    for gone in [
        "stage",
        "bucket",
        "access_key",
        "secret_key",
        "storage_integration",
    ] {
        assert!(
            !rendered.contains(gone),
            "`{gone}` still appears in the published configuration schema"
        );
    }
}
