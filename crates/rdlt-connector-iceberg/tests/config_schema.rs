//! Feature 016 T002: the generated config schema and the parser cannot
//! drift — round-trip corpus, unknown-field rejection, the validation
//! matrix, and the Secret grep-proof (ID6).

use jsonschema::validator_for;
use rdlt_connector_iceberg::{IcebergConfig, config_schema};
use serde_json::json;

fn corpus() -> Vec<serde_json::Value> {
    vec![
        // Minimal oauth2 (the Polaris/Snowflake shape).
        json!({
            "catalog": {
                "uri": "http://127.0.0.1:8181/api/catalog",
                "warehouse": "rdlt",
                "auth": {"oauth2_client_credentials": {
                    "client_id": "root", "client_secret": "s3cr3t",
                    "scopes": ["PRINCIPAL_ROLE:ALL"]}}
            },
            "namespace": "raw"
        }),
        // Bearer + create_namespace + storage override + tables/partitions.
        json!({
            "catalog": {
                "uri": "https://uc.example.com/api/2.1/unity-catalog/iceberg",
                "warehouse": "unity",
                "auth": {"bearer": {"token": "pat-123"}},
                "props": {"io-impl": "whatever"}
            },
            "namespace": "raw.orders",
            "create_namespace": true,
            "storage": {"s3": {"endpoint": "http://127.0.0.1:9000",
                                 "region": "us-east-1",
                                 "access_key": "k", "secret_key": "s"}},
            "tables": {
                "orders": {"name": "orders_v2",
                            "partition_by": [
                                {"column": "created_at", "transform": "day"},
                                {"column": "region", "transform": "identity"}]}
            }
        }),
    ]
}

#[test]
fn schema_valid_corpus_parses() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    for config in corpus() {
        assert!(
            validator.is_valid(&config),
            "corpus entry invalid: {config}"
        );
        IcebergConfig::from_value(config.clone())
            .unwrap_or_else(|e| panic!("schema-valid config failed to parse: {config}: {e}"));
    }
}

#[test]
fn unknown_fields_fail_schema_and_parser_identically() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let mut bad = corpus().remove(0);
    bad["catalog"]["warehous"] = json!("typo"); // typo for warehouse
    assert!(!validator.is_valid(&bad));
    assert!(IcebergConfig::from_value(bad).is_err());
}

#[test]
fn validation_matrix() {
    let base = corpus().remove(0);
    let with = |mutate: &dyn Fn(&mut serde_json::Value)| {
        let mut v = base.clone();
        mutate(&mut v);
        IcebergConfig::from_value(v)
            .expect_err("invalid")
            .to_string()
    };
    let err = with(&|v| v["catalog"]["uri"] = json!("not-a-url"));
    assert!(err.contains("http(s)"), "{err}");
    let err = with(&|v| v["catalog"]["warehouse"] = json!(""));
    assert!(err.contains("warehouse"), "{err}");
    let err = with(&|v| {
        v["catalog"]["auth"] = json!({});
    });
    assert!(err.contains("no scheme"), "{err}");
    let err = with(&|v| {
        v["catalog"]["auth"]["bearer"] = json!({"token": "t"});
    });
    assert!(err.contains("BOTH"), "{err}");
    let err = with(&|v| v["namespace"] = json!("raw..orders"));
    assert!(err.contains("namespace"), "{err}");
    let err = with(&|v| v["storage"] = json!({}));
    assert!(err.contains("no kind"), "{err}");
    let err = with(&|v| {
        v["tables"] = json!({"a": {"partition_by": [{"column": "", "transform": "day"}]}});
    });
    assert!(err.contains("names no column"), "{err}");
    // Unknown transform spelling rejected at parse (closed enum).
    let err = with(&|v| {
        v["tables"] = json!({"a": {"partition_by": [{"column": "x", "transform": "zorp"}]}});
    });
    assert!(err.contains("unknown variant"), "{err}");
    // Parameterized transforms need their parameter (`{bucket: N}`).
    let err = with(&|v| {
        v["tables"] = json!({"a": {"partition_by": [{"column": "x", "transform": "bucket"}]}});
    });
    assert!(err.contains("newtype variant"), "{err}");
}

/// ID6 grep-proof: no credential value ever renders from Debug.
#[test]
fn secrets_never_render() {
    let config = IcebergConfig::from_value(json!({
        "catalog": {
            "uri": "http://x:8181/api/catalog",
            "warehouse": "w",
            "auth": {"oauth2_client_credentials": {
                "client_id": "cid", "client_secret": "hunter2-oauth"}}
        },
        "namespace": "raw",
        "storage": {"s3": {"access_key": "hunter2-ak", "secret_key": "hunter2-sk"}}
    }))
    .expect("parses");
    let rendered = format!("{config:?}");
    for secret in ["hunter2-oauth", "hunter2-ak", "hunter2-sk"] {
        assert!(!rendered.contains(secret), "leaked `{secret}`: {rendered}");
    }
    assert!(rendered.contains("***"));

    // The bearer arm redacts too (T011).
    let bearer = IcebergConfig::from_value(json!({
        "catalog": {
            "uri": "http://x:8181/api/catalog",
            "warehouse": "w",
            "auth": {"bearer": {"token": "hunter2-bearer"}}
        },
        "namespace": "raw"
    }))
    .expect("parses");
    let rendered = format!("{bearer:?}");
    assert!(!rendered.contains("hunter2-bearer"), "leaked: {rendered}");
}

/// Helper surface: names/levels/partition lookups.
#[test]
fn helper_lookups() {
    let config = IcebergConfig::from_value(corpus().remove(1)).expect("parses");
    assert_eq!(config.namespace_levels(), vec!["raw", "orders"]);
    assert_eq!(config.table_name("orders"), "orders_v2");
    assert_eq!(config.table_name("other"), "other");
    assert_eq!(config.partition_fields("orders").len(), 2);
    assert!(config.partition_fields("other").is_empty());
}

/// Parameterized transforms (parity D2 closed): the map spellings
/// The YAML spellings the config documents: unit transforms as plain
/// strings, parameterized ones as single-key maps. YAML is what CLI
/// users write, and its enum handling differs from JSON's — the JSON
/// twin below passing does NOT prove these parse.
#[test]
fn yaml_transform_spellings_parse() {
    let config = IcebergConfig::from_yaml(
        "catalog:\n  uri: http://x:8181/api/catalog\n  warehouse: w\n  auth:\n    bearer:\n      token: t\nnamespace: raw\ntables:\n  events:\n    partition_by:\n      - column: id\n        transform:\n          bucket: 16\n      - column: region\n        transform:\n          truncate: 2\n      - column: created_at\n        transform: day\n",
    )
    .expect("documented YAML spellings parse");
    let fields = config.partition_fields("events");
    assert_eq!(
        fields[0].transform,
        rdlt_connector_iceberg::PartitionTransform::Bucket(16)
    );
    assert_eq!(
        fields[1].transform,
        rdlt_connector_iceberg::PartitionTransform::Truncate(2)
    );
    assert_eq!(
        fields[2].transform,
        rdlt_connector_iceberg::PartitionTransform::Day
    );
}

/// round-trip schema AND parser; zero parameters are typed eagerly.
#[test]
fn bucket_and_truncate_spellings_and_validation() {
    let config = IcebergConfig::from_value(json!({
        "catalog": {
            "uri": "http://x:8181/api/catalog",
            "warehouse": "w",
            "auth": {"bearer": {"token": "t"}}
        },
        "namespace": "raw",
        "tables": {"events": {"partition_by": [
            {"column": "id", "transform": {"bucket": 16}},
            {"column": "region", "transform": {"truncate": 2}},
            {"column": "created_at", "transform": "day"}
        ]}}
    }))
    .expect("parses");
    let fields = config.partition_fields("events");
    assert_eq!(fields.len(), 3);
    assert_eq!(
        fields[0].transform,
        rdlt_connector_iceberg::PartitionTransform::Bucket(16)
    );
    assert_eq!(
        fields[1].transform,
        rdlt_connector_iceberg::PartitionTransform::Truncate(2)
    );
    // The generated schema accepts what the parser accepts.
    let schema = jsonschema::validator_for(&config_schema()).expect("schema compiles");
    let doc = serde_json::to_value(&config).expect("serializes");
    assert!(schema.is_valid(&doc), "round-trips the generated schema");

    for (transform, subject) in [
        (json!({"bucket": 0}), "bucket"),
        (json!({"truncate": 0}), "truncate"),
    ] {
        let err = IcebergConfig::from_value(json!({
            "catalog": {
                "uri": "http://x:8181/api/catalog",
                "warehouse": "w",
                "auth": {"bearer": {"token": "t"}}
            },
            "namespace": "raw",
            "tables": {"events": {"partition_by": [
                {"column": "id", "transform": transform}
            ]}}
        }))
        .expect_err("zero parameter must be typed");
        assert!(format!("{err}").contains(subject), "{err}");
    }
}

/// Destination construction surface (T017): from_yaml/from_config
/// validation, spec, capabilities — the container-free dest.rs paths.
#[test]
fn dest_construction_spec_and_capabilities() {
    use rdlt_connector::Destination;
    use rdlt_connector_iceberg::IcebergDest;

    let yaml = r#"
catalog:
  uri: https://polaris.example/api/catalog
  warehouse: rdlt
  auth:
    bearer: {token: t}
namespace: raw
"#;
    let dest = IcebergDest::from_yaml(yaml).expect("valid yaml");
    let spec = dest.spec();
    assert_eq!(spec.name, "iceberg");
    assert!(spec.config_schema.is_some());
    let caps = dest.capabilities();
    assert!(!caps.merge, "append-only this release");
    assert!(caps.structs && caps.scalar_lists && caps.decimal);
    assert!(!caps.json_type, "Json lands as string (closed table)");

    assert!(
        IcebergDest::from_yaml("namespace: x\n").is_err(),
        "missing catalog typed"
    );
    let invalid = IcebergDest::from_config(IcebergConfig::new(
        "ftp://nope",
        "w",
        rdlt_connector_iceberg::AuthOptions::bearer("t"),
        "raw",
    ));
    assert!(invalid.is_err(), "validation runs at construction");
}
