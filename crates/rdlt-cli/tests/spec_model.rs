//! The spec-model pins that lived inside the old single-file CLI:
//! the shared parity fixture and the per-destination spec arms. Moved
//! VERBATIM (module wrapper included — the fixtures' indentation is
//! load-bearing) in the 036 module split; the subject is
//! `rdlt::pipeline_spec`, not the binary.

mod tests {
    use rdlt::pipeline_spec::{ConfigSpec, DestSpec, SourceSpec, Spec, WriteModeSpec};
    use serde::Deserialize;

    fn spec(yaml: &str) -> Spec {
        serde_yaml::from_str(yaml).expect("spec parses")
    }

    /// Every document in the shared parity fixture must parse as a Spec.
    /// The bench harness reaches the same shared model INDIRECTLY (it
    /// execs `rdlt run <spec> --report`), so this pin is the one place
    /// the fixture — where a destination or source kind is added first —
    /// is held against the model directly.
    #[test]
    fn shared_parity_specs_all_parse() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../benches/parity_specs.yaml"
        ));
        let mut parsed = 0usize;
        for document in serde_yaml::Deserializer::from_str(raw) {
            let parsed_spec = Spec::deserialize(document).expect("parity spec parses in the CLI");
            assert!(!parsed_spec.pipeline.is_empty());
            parsed += 1;
        }
        assert_eq!(parsed, 6, "fixture covers every destination kind");
    }

    /// The iceberg destination block parses the crate's full vocabulary from
    /// the pipeline YAML with zero CLI code — and validation errors are typed
    /// at spec load.
    #[test]
    fn iceberg_spec_parses_from_the_yaml() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  iceberg:
    catalog:
      uri: https://polaris.example/api/catalog
      warehouse: rdlt
      auth:
        oauth2_client_credentials: {client_id: cid, client_secret: hunter2-cli}
    namespace: raw.orders
    create_namespace: true
    tables:
      events:
        partition_by: [{column: region, transform: identity}]
"#,
        );
        let DestSpec::Iceberg(config) = parsed.destination else {
            panic!("expected iceberg dest");
        };
        assert_eq!(config.namespace_levels(), vec!["raw", "orders"]);
        assert_eq!(config.partition_fields("events").len(), 1);
        assert!(config.validate().is_ok());
        assert!(
            !format!("{config:?}").contains("hunter2-cli"),
            "secret redacted"
        );
    }

    /// The snowflake destination block parses the crate's full vocabulary
    /// from the pipeline YAML with zero CLI code — including the staging
    /// bucket and the shared merge options — and no credential renders.
    #[test]
    fn snowflake_spec_parses_from_the_yaml() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  snowflake:
    account: MYORG-MYACCT
    user: LOADER
    auth:
      key_pair: {private_key: /k.p8, passphrase: hunter2-sf}
    database: ANALYTICS
    schema: RAW
    warehouse: WH
    table_type: transient
    merge_strategy: upsert
"#,
        );
        let DestSpec::Snowflake(config) = parsed.destination else {
            panic!("expected snowflake dest");
        };
        assert_eq!(config.host(), "myorg-myacct.snowflakecomputing.com");
        assert!(config.auth.key_pair.is_some());
        assert!(config.options.merge_strategy.is_some());
        // No storage vocabulary remains to configure: rows travel through
        // storage the service provides, and a document still carrying the old
        // block is refused rather than quietly ignored.
        assert!(config.validate().is_ok());
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("hunter2-sf"),
            "secret rendered: {rendered}"
        );
    }

    /// A destination document whose account is a pasted console URL is refused
    /// at spec load, where the user can still act on it — not at connect time
    /// with a name that resolves to nothing.
    #[test]
    fn an_invalid_snowflake_document_is_typed_at_spec_load() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  snowflake:
    account: https://myorg-myacct.snowflakecomputing.com
    user: LOADER
    auth: {pat: tok}
    database: D
    schema: S
"#,
        );
        let DestSpec::Snowflake(config) = parsed.destination else {
            panic!("expected snowflake dest");
        };
        let err = config.validate().expect_err("a URL is not an identifier");
        assert!(format!("{err}").contains("account"), "{err}");
    }

    /// The per-table destination options ride the pipeline YAML with zero CLI
    /// code, and the duckdb destination block accepts the SAME options
    /// vocabulary as postgres — one YAML shape.
    #[test]
    fn duckdb_options_pass_through_the_yaml() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  duckdb:
    path: out.duckdb
    merge_strategy: upsert
    extensions: [httpfs]
    settings: {threads: "4"}
    tables:
      events:
        hard_delete: deleted
        dedup_sort: {column: seq, order: desc}
"#,
        );
        let DestSpec::Duckdb(config) = &parsed.destination else {
            panic!("duckdb dest");
        };
        assert_eq!(
            config.extensions.as_deref(),
            Some(&["httpfs".to_string()][..])
        );
        assert_eq!(
            config
                .settings
                .as_ref()
                .and_then(|s| s.get("threads"))
                .map(String::as_str),
            Some("4")
        );
        assert_eq!(
            config.merge_strategy,
            Some(rdlt::connector::duckdb::destination::MergeStrategy::Upsert)
        );
        let events = config.tables.as_ref().expect("tables")["events"].clone();
        assert_eq!(events.hard_delete.as_deref(), Some("deleted"));
        assert!(events.dedup_sort.is_some());
    }

    /// The duckdb destination block IS the connector's config document,
    /// embedded rather than mirrored (042 Task 6, the D-041-6 shape):
    /// a document with every key set parses through the Spec to the
    /// SAME `Config` the document type parses directly — so a field
    /// added to the connector's vocabulary is reachable from pipeline
    /// YAML with zero facade code. No relaxation rides along: the
    /// retired mirror's fields were the config's fields one for one,
    /// every one already optional except `path`, which the document
    /// requires too.
    #[test]
    fn duckdb_spec_embeds_the_connector_document() {
        let block = r#"
path: out.duckdb
memory_limit: 1GB
merge_strategy: upsert
tables:
  events: {hard_delete: deleted, dedup_sort: {column: seq, order: desc}}
extensions: [httpfs]
settings: {threads: "4"}
"#;
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  duckdb:
    path: out.duckdb
    memory_limit: 1GB
    merge_strategy: upsert
    tables:
      events: {hard_delete: deleted, dedup_sort: {column: seq, order: desc}}
    extensions: [httpfs]
    settings: {threads: "4"}
"#,
        );
        let DestSpec::Duckdb(config) = parsed.destination else {
            panic!("expected duckdb dest");
        };
        let direct: rdlt::connector::duckdb::destination::Config =
            serde_yaml::from_str(block).expect("the document type parses the same keys directly");
        assert_eq!(*config, direct, "the spec arm and the document diverge");
    }

    #[test]
    fn refinement_options_pass_through_the_yaml() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres:
    conn: host=x
    dataset: d
    tables:
      events:
        hard_delete: deleted
        dedup_sort: {column: seq, order: desc}
        merge_scope: [day, tenant]
"#,
        );
        let DestSpec::Postgres(config) = &parsed.destination else {
            panic!("postgres dest");
        };
        let events = config.tables["events"].clone();
        let dedup = events.dedup_sort.expect("dedup_sort");
        assert_eq!(dedup.column, "seq");
        assert_eq!(
            dedup.order,
            rdlt::connector::postgres::destination::SortOrder::Desc
        );
        assert_eq!(
            events.merge_scope.as_deref(),
            Some(&["day".to_string(), "tenant".to_string()][..])
        );
    }

    /// The postgres destination block IS the connector's config document,
    /// embedded rather than mirrored (D-041-6): a document with every key
    /// set parses through the Spec to the SAME `Config` the document type
    /// parses directly — so a field added to the connector's vocabulary is
    /// reachable from pipeline YAML with zero facade code.
    #[test]
    fn postgres_spec_embeds_the_connector_document() {
        let block = r#"
conn: host=x
dataset: d
tls: {mode: require}
merge_strategy: upsert
tables:
  events: {hard_delete: deleted, dedup_sort: {column: seq, order: desc}}
"#;
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres:
    conn: host=x
    dataset: d
    tls: {mode: require}
    merge_strategy: upsert
    tables:
      events: {hard_delete: deleted, dedup_sort: {column: seq, order: desc}}
"#,
        );
        let DestSpec::Postgres(config) = parsed.destination else {
            panic!("expected postgres dest");
        };
        let direct: rdlt::connector::postgres::destination::Config =
            serde_yaml::from_str(block).expect("the document type parses the same keys directly");
        assert_eq!(*config, direct, "the spec arm and the document diverge");
    }

    /// `dataset` is optional in the postgres destination block: the
    /// document defaults it to "public". D-041-6 relaxed the retired
    /// mirror's hand-required field to the connector document's default.
    #[test]
    fn postgres_dataset_defaults_to_public() {
        let parsed = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres: {conn: host=x}
"#,
        );
        let DestSpec::Postgres(config) = parsed.destination else {
            panic!("expected postgres dest");
        };
        assert_eq!(config.schema, "public");
    }

    /// A typo inside the postgres block refuses at spec load with the
    /// connector document's own wording — deny_unknown_fields now lives
    /// on the embedded vocabulary, not a facade mirror.
    #[test]
    fn a_postgres_destination_typo_is_refused_at_spec_load() {
        let err = serde_yaml::from_str::<Spec>(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres: {conn: host=x, datset: d}
"#,
        )
        .expect_err("an unknown field inside the postgres block must refuse");
        // The expected-key list is what proves the CONNECTOR document's
        // vocabulary answered — a bare "unknown field" is satisfied by any
        // deny gate, including the enum's own, and could not detect a
        // regression back to a mirror. (Line/column noise trails the
        // clause, so the pin is the full stable clause, not the full
        // rendered string.)
        assert!(
            format!("{err}").contains(
                "unknown field `datset`, expected one of `conn`, `dataset`, `tls`, \
                 `merge_strategy`, `tables`"
            ),
            "refusal carries the connector document's field vocabulary: {err}"
        );
    }

    /// ONE pipeline YAML: the source document inline is the natural form
    /// and is held to the SAME validation as file configs (from_value).
    #[test]
    fn inline_postgres_source_parses_and_validates() {
        let parsed = spec(
            r#"
pipeline: p
write_mode: {merge: {key: [id]}}
source:
  postgres:
    conn: host=localhost
    tables:
      - name: orders
        cursor: {column: updated_at, lag: "5m"}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        // Resolved through the PUBLIC accessor — the same path `run()`
        // takes, so this exercises the validation gate rather than
        // reaching past it into the parsed shape.
        let config = parsed
            .pg_source_config()
            .expect("postgres source")
            .expect("valid inline");
        let table = &config.tables.as_ref().expect("tables")[0];
        assert_eq!(table.name, "orders");
        assert_eq!(table.cursor.as_ref().expect("cursor").column, "updated_at");

        // …and rejects invalid shapes identically — cdc and cursor are
        // mutually exclusive on the same table.
        let bad = spec(
            r#"
pipeline: p
source:
  postgres:
    conn: host=localhost
    cdc: {slot: s, publication: p}
    tables:
      - name: t
        cursor: {column: id}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        let err = bad
            .pg_source_config()
            .expect("postgres source")
            .expect_err("cdc + cursor rejected inline")
            .to_string();
        assert!(err.contains("mutually exclusive"), "{err}");

        // `config:` mixed with inline fields is a LOUD error, not a
        // silently-ignored document.
        let mixed: Result<Spec, _> = serde_yaml::from_str(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml, conn: host=localhost}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        assert!(mixed.is_err(), "config + inline fields must not parse");

        // The file form still parses.
        let file = spec(
            r#"
pipeline: p
source:
  postgres: {config: src.yaml}
destination:
  postgres: {conn: host=x, dataset: d}
"#,
        );
        assert!(matches!(
            file.source,
            SourceSpec::Postgres(ConfigSpec::Path(_))
        ));
    }

    /// Every pipeline-spec form parses — write_mode's three shapes, all
    /// destination kinds, workdir default vs custom.
    #[test]
    fn pipeline_spec_forms_parse() {
        for (mode, want_merge) in [
            ("write_mode: append\n", false),
            ("write_mode: replace\n", false),
            ("write_mode: {merge: {key: [id]}}\n", true),
            ("", false), // absent = append default
        ] {
            let parsed = spec(&format!(
                "pipeline: p\n{mode}source:\n  postgres: {{config: s.yaml}}\n\
                 destination:\n  duckdb: {{path: out.db}}\n"
            ));
            assert_eq!(
                matches!(parsed.write_mode, Some(WriteModeSpec::Merge { .. })),
                want_merge,
                "{mode}"
            );
        }
        // Destination kinds + workdir.
        let parquet = spec(
            "pipeline: p\nworkdir: /tmp/x\nsource:\n  postgres: {config: s.yaml}\n\
             destination:\n  parquet: {path: out}\n",
        );
        assert!(matches!(parquet.destination, DestSpec::Parquet { .. }));
        assert_eq!(
            parquet.workdir.as_deref(),
            Some(std::path::Path::new("/tmp/x"))
        );
        let duck = spec(
            "pipeline: p\nsource:\n  postgres: {config: s.yaml}\n\
             destination:\n  duckdb: {path: out.db, memory_limit: \"1GB\"}\n",
        );
        assert!(matches!(duck.destination, DestSpec::Duckdb(_)));
        assert!(
            duck.workdir.is_none(),
            "workdir defaults downstream to .rdlt"
        );
        // 015: the file destination's full vocabulary.
        let file = spec(
            r#"
pipeline: p
source:
  postgres: {config: s.yaml}
destination:
  file:
    path: warehouse
    format: jsonl
    partition_by: day
    location:
      s3:
        endpoint: "http://x:9000"
        bucket: lake
        access_key: k
        secret_key: s
"#,
        );
        match &file.destination {
            DestSpec::File(config) => {
                // The document deserializes straight into the connector's own
                // config, so these assertions now read the SAME type the
                // destination is built from — there is no mirror in between to
                // disagree with it.
                assert!(matches!(
                    config.format,
                    rdlt::connector::file::destination::DestFormat::Jsonl
                ));
                assert!(config.location.is_some());
                assert_eq!(config.partition_by.as_deref(), Some("day"));
            }
            other => panic!("expected file destination, got {other:?}"),
        }
    }
}
