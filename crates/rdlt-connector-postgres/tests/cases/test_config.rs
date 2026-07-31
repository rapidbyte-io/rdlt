//! The config vocabulary pins, lifted from the module's former inline tests:
//! parsing, validation, and rejection texts through the public entry points.

use rdlt_connector_postgres::source::config::*;

const MINIMAL: &str = r#"
conn: "postgresql://u:p@localhost:5432/db"
"#;

#[test]
fn minimal_defaults() {
    let c = PostgresConfig::from_yaml(MINIMAL).expect("minimal config");
    assert_eq!(c.schema, "public");
    assert!(!c.include_views);
    assert!(c.tables.is_none());
    assert_eq!(c.batch_target_bytes, 8 << 20);
    assert_eq!(c.batch_max_rows, 65_536);
}

#[test]
fn unknown_fields_rejected() {
    let err = PostgresConfig::from_yaml("conn: host=localhost\nfrobnicate: true\n").unwrap_err();
    assert!(matches!(err, ConfigError::Yaml(_)), "{err}");
}

#[test]
fn qualified_table_name_rejected() {
    let err = PostgresConfig::from_yaml("conn: host=localhost\ntables:\n  - name: sales.orders\n")
        .unwrap_err();
    assert!(err.to_string().contains("schema-qualified"), "{err}");
}

#[test]
fn include_exclude_mutually_exclusive() {
    let err = PostgresConfig::from_yaml(
        "conn: host=localhost\ntables:\n  - name: t\n    included_columns: [a]\n    excluded_columns: [b]\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"), "{err}");
}

/// An empty table list means "no tables" — the only way to say "deliver the
/// declared queries and nothing else". Absent, by contrast, still discovers
/// every table, so a queries-only pipeline that omits the field receives the
/// whole schema alongside its queries.
#[test]
fn empty_table_list_selects_no_tables_and_keeps_queries() {
    let c = PostgresConfig::from_yaml(
        "conn: host=localhost\ntables: []\nqueries:\n  - name: q\n    sql: SELECT 1\n",
    )
    .expect("empty list alongside a query is a valid selection");
    assert_eq!(c.tables.as_deref(), Some(&[][..]));
    assert_eq!(c.queries.len(), 1);
}

/// Selecting nothing at all would move zero rows and report success, so it
/// is refused where it is knowable — at parse time, from the document alone.
#[test]
fn selecting_no_streams_at_all_is_rejected_naming_both_remedies() {
    let err = PostgresConfig::from_yaml("conn: host=localhost\ntables: []\n")
        .expect_err("no tables and no queries selects nothing");
    let msg = err.to_string();
    assert!(msg.contains("no streams selected"), "{msg}");
    assert!(msg.contains("queries") && msg.contains("omit"), "{msg}");
}

/// Change data capture reads the configured tables; configuring none would
/// leave the slot un-preflighted and never advanced, i.e. the block would
/// silently behave as if it were absent.
#[test]
fn cdc_with_an_empty_table_list_is_rejected() {
    let err = PostgresConfig::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s1\n  publication: p1\ntables: []\n\
         queries:\n  - name: q\n    sql: SELECT 1\n",
    )
    .expect_err("cdc captures the configured tables; none means nothing");
    assert!(err.to_string().contains("captures nothing"), "{err}");
}

#[test]
fn empty_selections_rejected() {
    for doc in [
        "conn: host=localhost\ntables: []\n",
        "conn: host=localhost\ntables:\n  - name: t\n    included_columns: []\n",
        "conn: host=localhost\ntables:\n  - name: t\n    primary_key: []\n",
        "conn: \"\"\n",
        "conn: host=localhost\nbatch_max_rows: 0\n",
    ] {
        assert!(
            PostgresConfig::from_yaml(doc).is_err(),
            "should reject: {doc}"
        );
    }
}

#[test]
fn cdc_block_parses_with_defaults() {
    let c = PostgresConfig::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s1\n  publication: p1\ntables:\n  - name: orders\n",
    )
    .expect("cdc config");
    let cdc = c.cdc.expect("cdc");
    assert_eq!(cdc.slot, "s1");
    assert_eq!(cdc.publication, "p1");
    assert!(!cdc.create_if_missing);
    assert_eq!(cdc.mode, CdcMode::Catchup);
    assert_eq!(cdc.idle_wait, Wait { seconds: 1 });
    assert_eq!(cdc.flag_column, "_rdlt_deleted");
    assert_eq!(cdc.ack, AckMode::Auto);
}

#[test]
fn cdc_validation_matrix() {
    // cursor + cdc on the same table — typed, names the table.
    let err = PostgresConfig::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
         tables:\n  - name: orders\n    cursor:\n      column: id\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("`orders`"), "{err}");
    assert!(err.to_string().contains("mutually exclusive"), "{err}");
    // Required + non-empty names.
    for doc in [
        "conn: host=localhost\ncdc:\n  publication: p\n",
        "conn: host=localhost\ncdc:\n  slot: s\n",
        "conn: host=localhost\ncdc:\n  slot: \"\"\n  publication: p\n",
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  flag_column: \"\"\n",
        // unknown fields fail; idle_wait rejects magnitudes.
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  drop_slot: true\n",
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  idle_wait: \"5\"\n",
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  mode: streaming\n",
    ] {
        assert!(
            PostgresConfig::from_yaml(doc).is_err(),
            "should reject: {doc}"
        );
    }
    // Tail mode + duration idle_wait + ack off parse.
    let c = PostgresConfig::from_yaml(
        "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
         \x20 mode: tail\n  idle_wait: \"5m\"\n  ack: off\n",
    )
    .expect("tail config");
    let cdc = c.cdc.expect("cdc");
    assert_eq!(cdc.mode, CdcMode::Tail);
    assert_eq!(cdc.idle_wait, Wait { seconds: 300 });
    assert_eq!(cdc.ack, AckMode::Off);
}

#[test]
fn json_and_value_entry_points_share_validation() {
    let json =
        r#"{"conn": "host=localhost", "tables": [{"name": "t", "cursor": {"column": "id"}}]}"#;
    let from_json = PostgresConfig::from_json(json).expect("json");
    let from_yaml = PostgresConfig::from_yaml(
        "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: id\n",
    )
    .expect("yaml");
    assert_eq!(from_json, from_yaml, "one document shape, two syntaxes");
    let value: serde_json::Value = serde_json::from_str(json).expect("value");
    assert_eq!(PostgresConfig::from_value(value).expect("value"), from_json);
    // Validation is shared: the parse gate fires on every entry point.
    assert!(PostgresConfig::from_json(r#"{"conn": "not a conn"}"#).is_err());
    assert!(
        PostgresConfig::from_value(serde_json::json!({"conn": "x", "unknown": 1})).is_err(),
        "deny_unknown_fields holds for Value too"
    );
}

#[test]
fn conn_parse_gate_and_tls_policy() {
    // Contract rule 1: parse failure = typed CONFIG error, up front.
    let err = PostgresConfig::from_yaml("conn: not-a-conn-string\n").unwrap_err();
    assert!(err.to_string().contains("does not parse"), "{err}");
    // TLS is wired — every conn-string sslmode level now passes config
    // validation (incl. the spaced keyword form).
    for conn in [
        "postgresql://u:p@h/db?sslmode=require",
        "host=h sslmode=require",
        "host=h sslmode = require",
        "host=h sslmode=prefer",
        "host=h sslmode=disable",
    ] {
        assert!(
            PostgresConfig::from_yaml(&format!("conn: \"{conn}\"\n")).is_ok(),
            "{conn} must validate"
        );
    }
    // Contradiction rule (tls-policy.md): explicit conn sslmode reversed
    // by the block = typed config error; refinement is allowed.
    let err =
        PostgresConfig::from_yaml("conn: \"host=h sslmode=disable\"\ntls:\n  mode: verify_full\n")
            .unwrap_err();
    assert!(err.to_string().contains("contradicts"), "{err}");
    assert!(
        PostgresConfig::from_yaml("conn: \"host=h sslmode=require\"\ntls:\n  mode: verify_full\n")
            .is_ok(),
        "require -> verify_full is refinement, not contradiction"
    );
}

#[test]
fn duplicate_tables_rejected() {
    let err =
        PostgresConfig::from_yaml("conn: host=localhost\ntables:\n  - name: t\n  - name: t\n")
            .unwrap_err();
    assert!(err.to_string().contains("listed twice"), "{err}");
}

#[test]
fn lag_with_open_boundary_dies_at_config_parse() {
    let err = PostgresConfig::from_yaml(
        "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: ts\n      boundary: exclusive\n      lag: \"5m\"\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("INCLUSIVE boundary"), "{err}");
    // Closed (default) parses fine.
    PostgresConfig::from_yaml(
        "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: ts\n      lag: \"5m\"\n",
    )
    .expect("closed + lag parses");
}

#[test]
fn tls_contradiction_rejected_at_config_validation() {
    // sslmode=require is ACCEPTED (TLS is wired); the config-level
    // rejection is now the contradiction rule.
    assert!(
        PostgresConfig::from_yaml("conn: \"postgresql://u:p@localhost/db?sslmode=require\"\n")
            .is_ok(),
        "require now validates — TLS is wired"
    );
    let err = PostgresConfig::from_yaml(
        "conn: \"postgresql://u:p@localhost/db?sslmode=require\"\ntls:\n  mode: disable\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("contradicts"), "{err}");
}
