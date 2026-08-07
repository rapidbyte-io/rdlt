//! 041 Task 6: the five `-remote` pipeline templates are LIVE documents,
//! not dead files that fail only in the recorded session. Each one is
//! rendered exactly as the runner renders it (the same `substitute` over
//! the same keys the runner provides), then pushed through the REAL
//! gates a run would hit:
//!
//!   1. the facade's `pipeline_spec::Spec` parse (deny_unknown_fields —
//!      a typoed top-level or `connector:` key dies here);
//!   2. both sides must be the `connector:` arm with the expected
//!      reverse-DNS id and a `{{bins}}`-resolved path override;
//!   3. the OPAQUE `config:` block — which the Spec parse deliberately
//!      does not validate — is pushed through the named connector's own
//!      `Document` gate (`from_value`), so a config key that drifted
//!      from the in-process twin's vocabulary is caught HERE, not at
//!      the handshake in the by-hand session.
//!
//! The cell registry side (ids, verify, competitor arms) is load-checked
//! by `selftest.rs`'s whole-registry load; this suite owns the pipeline
//! documents.

use std::collections::BTreeMap;
use std::path::PathBuf;

use rdlt::pipeline_spec::{DestSpec, SourceSpec, Spec};
use rdlt::sdk::config::Document;
use rdlt_bench::template::substitute;

/// The five wire twins and the connector each side must name.
const REMOTE_PIPELINES: &[(&str, &str, &str)] = &[
    (
        "pg-to-pg-remote.yaml",
        "io.rapidbyte.postgres",
        "io.rapidbyte.postgres",
    ),
    (
        "pg-to-pg-dedup-remote.yaml",
        "io.rapidbyte.postgres",
        "io.rapidbyte.postgres",
    ),
    (
        "pg-to-s3parquet-remote.yaml",
        "io.rapidbyte.postgres",
        "io.rapidbyte.file",
    ),
    (
        "s3jsonl-to-pg-remote.yaml",
        "io.rapidbyte.file",
        "io.rapidbyte.postgres",
    ),
    (
        "s3jsonl-to-s3parquet-remote.yaml",
        "io.rapidbyte.file",
        "io.rapidbyte.file",
    ),
];

fn pipelines_dir() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt-bench sits two levels below the repo root")
        .join("benches/cells/pipelines")
}

/// The runner's render-time substitution map, with stand-in values —
/// the same KEYS `run_cell`/`run_once_subprocess` provide, so a
/// template referencing anything else leaves `{{…}}` behind and fails
/// the no-residue assertion below.
fn runner_subs() -> BTreeMap<String, String> {
    BTreeMap::from(
        [
            ("repo", "/repo"),
            ("benches", "/repo/benches"),
            ("cli", "/repo/target/release/rdlt"),
            ("bins", "/repo/target/debug"),
            ("data", "/data"),
            (
                "conn",
                "host=127.0.0.1 port=5439 user=postgres password=postgres dbname=src",
            ),
            ("port", "5439"),
            ("workdir", "/workdir"),
            ("run", "0"),
        ]
        .map(|(k, v)| (k.to_owned(), v.to_owned())),
    )
}

/// Push one `connector:` side's opaque config through the named
/// connector's own Document gate — the validation a real run performs
/// at the handshake, pulled forward to test time.
fn validate_config(id: &str, role: &str, config: &serde_json::Value, file: &str) {
    let config = config.clone();
    let outcome = match (id, role) {
        ("io.rapidbyte.postgres", "source") => {
            rdlt::connector::postgres::source::Config::from_value(config)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        ("io.rapidbyte.postgres", "destination") => {
            rdlt::connector::postgres::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        ("io.rapidbyte.file", "source") => {
            rdlt::connector::file::source::Config::from_value(config)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        ("io.rapidbyte.file", "destination") => {
            rdlt::connector::file::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        other => panic!("{file}: no gate wired for {other:?}"),
    };
    if let Err(error) = outcome {
        panic!("{file}: the {role} `config:` block fails {id}'s own document gate: {error}");
    }
}

#[test]
fn the_five_remote_pipelines_render_parse_and_pass_the_connector_gates() {
    let dir = pipelines_dir();
    let subs = runner_subs();
    for (file, source_id, destination_id) in REMOTE_PIPELINES {
        let path = dir.join(file);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let rendered = substitute(&raw, &subs);
        assert!(
            !rendered.contains("{{"),
            "{file}: a `{{{{…}}}}` key survived rendering — the template references \
             a key the runner does not provide:\n{rendered}"
        );

        let spec: Spec = serde_yaml::from_str(&rendered).unwrap_or_else(|e| {
            panic!("{file}: the rendered document is not a pipeline spec: {e}")
        });

        match &spec.source {
            SourceSpec::Connector(reference) => {
                assert_eq!(&reference.id, source_id, "{file}: source connector id");
                let bin = reference.path.as_ref().unwrap_or_else(|| {
                    panic!("{file}: the source side carries no `path:` override")
                });
                assert!(
                    bin.starts_with("/repo/target/debug"),
                    "{file}: the source path override must come from {{{{bins}}}}: {}",
                    bin.display()
                );
                validate_config(source_id, "source", &reference.config, file);
            }
            other => panic!("{file}: the source is not the `connector:` arm: {other:?}"),
        }
        match &spec.destination {
            DestSpec::Connector(reference) => {
                assert_eq!(
                    &reference.id, destination_id,
                    "{file}: destination connector id"
                );
                let bin = reference.path.as_ref().unwrap_or_else(|| {
                    panic!("{file}: the destination side carries no `path:` override")
                });
                assert!(
                    bin.starts_with("/repo/target/debug"),
                    "{file}: the destination path override must come from {{{{bins}}}}: {}",
                    bin.display()
                );
                validate_config(destination_id, "destination", &reference.config, file);
            }
            other => panic!("{file}: the destination is not the `connector:` arm: {other:?}"),
        }
    }
}
