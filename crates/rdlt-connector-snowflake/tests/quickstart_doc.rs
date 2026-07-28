//! The quickstart's pipeline document, parsed.
//!
//! Documentation drifts silently: the shipped shape changes, the walkthrough
//! keeps the old one, and the first person to follow it hits an error the
//! project believes cannot happen. The quickstart carried exactly that — a
//! flat `private_key:` where the shipped vocabulary nests it under `auth:`,
//! and a `path:` prefix that never existed — so the document is now compiled.

use rdlt_connector_snowflake::dest::SnowflakeConfig;

/// Lifted verbatim from `specs/022-snowflake-dest/quickstart.md`.
const QUICKSTART_DESTINATION: &str = r#"
account: "MYORG-MYACCT"
user: "MY_LOADER"
auth:
  key_pair:
    private_key: "/home/you/.config/rdlt/snowflake/rdlt_key.p8"
    passphrase: "your-passphrase"
database: "ANALYTICS"
schema: "RAW"
merge_strategy: upsert
"#;

/// And the optional bulk-path block the same section adds.
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
    bucket: "my-staging-bucket"
    prefix: "rdlt/parts"
    region: "eu-west-2"
    access_key: "AKIA..."
    secret_key: "..."
"#;

#[test]
fn the_quickstart_document_is_a_valid_configuration() {
    let config = SnowflakeConfig::from_yaml(QUICKSTART_DESTINATION)
        .expect("the documented document must parse");
    assert_eq!(config.host(), "myorg-myacct.snowflakecomputing.com");
    assert!(config.auth.key_pair.is_some());
    assert!(config.options.merge_strategy.is_some());
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

#[test]
fn the_documented_bulk_path_block_is_a_valid_configuration() {
    let config =
        SnowflakeConfig::from_yaml(QUICKSTART_WITH_STAGE).expect("the stage block must parse");
    assert!(config.stage.is_some(), "the bulk path is configured");
}
