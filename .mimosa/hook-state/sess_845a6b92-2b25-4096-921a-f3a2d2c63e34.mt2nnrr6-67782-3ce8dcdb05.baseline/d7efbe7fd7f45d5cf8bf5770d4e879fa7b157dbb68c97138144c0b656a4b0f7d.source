//! The five e2e cell pipeline templates (connector-spawning documents,
//! born as 041's `-remote` twins and owning the base cell ids since the
//! 043 D1 swap) are LIVE documents,
//! not dead files that fail only in the recorded session. Each one is
//! rendered exactly as the runner renders it (the same `substitute` over
//! the same keys the runner provides), then pushed through the REAL
//! gates a run would hit:
//!
//!   1. the facade's `pipeline_spec::Spec` parse (deny_unknown_fields —
//!      a typoed top-level or `connector:` key dies here);
//!   2. both sides must be the `connector:` arm with the expected
//!      reverse-DNS id and a `{{bins}}`-resolved path override.
//!
//! The OPAQUE `config:` blocks — which the Spec parse deliberately does
//! not validate — used to be pushed through the named connectors' own
//! `Document` gates here as well. Those crates live in the sibling
//! rdlt-connectors repository since the cut (044), so that half of the
//! pin lives with them; in a live run the spawned connector's own gate
//! still validates the block at the handshake, in its own wording.
//!
//! The cell registry side (ids, verify, competitor arms) is load-checked
//! by `selftest.rs`'s whole-registry load; this suite owns the pipeline
//! documents.

use std::collections::BTreeMap;
use std::path::PathBuf;

use rdlt::pipeline_spec::{DestSpec, SourceSpec, Spec};
use rdlt_bench::runner::PIPELINE_SUBSTITUTION_KEYS;
use rdlt_bench::template::substitute;

/// The five cell pipelines and the connector each side must name.
const REMOTE_PIPELINES: &[(&str, &str, &str)] = &[
    (
        "pg-to-pg.yaml",
        "io.rapidbyte.postgres",
        "io.rapidbyte.postgres",
    ),
    (
        "pg-to-pg-dedup.yaml",
        "io.rapidbyte.postgres",
        "io.rapidbyte.postgres",
    ),
    (
        "pg-to-s3parquet.yaml",
        "io.rapidbyte.postgres",
        "io.rapidbyte.file",
    ),
    (
        "s3jsonl-to-pg.yaml",
        "io.rapidbyte.file",
        "io.rapidbyte.postgres",
    ),
    (
        "s3jsonl-to-s3parquet.yaml",
        "io.rapidbyte.file",
        "io.rapidbyte.file",
    ),
];

fn pipelines_dir() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt-bench sits two levels below the repo root")
        .join("benches/harness/cells/pipelines")
}

/// The runner's render-time substitution map with stand-in values,
/// built from the runner's OWN key slice — so a template referencing
/// anything the runner does not provide leaves `{{…}}` behind and fails
/// the no-residue assertion below.
///
/// The keys are read from [`rdlt_bench::runner::PIPELINE_SUBSTITUTION_KEYS`],
/// never restated here. A hand-written copy used to sit in this
/// function, and it made the test agree with itself: renaming a key in
/// the runner left this list stale, every template still rendered
/// against the OLD name, and the suite stayed green while a live
/// session would die on an unrendered `{{bins}}`. The runner's `put`
/// guard closes the other direction (a substitution the slice does not
/// name refuses at the source), so neither side can drift alone.
///
/// The values are shaped only where a value is inspected: `bins` must
/// look like a release directory because the assertions below check the
/// rendered `path:` overrides came from it. Everything else is a
/// placeholder.
fn runner_subs() -> BTreeMap<String, String> {
    let value = |key: &str| match key {
        "repo" => "/repo",
        "benches" => "/repo/benches",
        "cli" => "/repo/target/release/rdlt",
        "bins" => "/repo/target/release",
        "data" => "/data",
        "conn" => "host=127.0.0.1 port=5439 user=postgres password=postgres dbname=src",
        "port" => "5439",
        "workdir" => "/workdir",
        "run" => "0",
        // A key added to the runner without a stand-in here would
        // otherwise render as an empty string and quietly pass.
        other => panic!(
            "runner::PIPELINE_SUBSTITUTION_KEYS gained `{other}` — give it a stand-in \
             value here so the templates are rendered the way a run renders them"
        ),
    };
    PIPELINE_SUBSTITUTION_KEYS
        .iter()
        .map(|key| ((*key).to_owned(), value(key).to_owned()))
        .collect()
}

#[test]
fn the_five_remote_pipelines_render_parse_and_name_their_connectors() {
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

        let spec: Spec = serde_yaml_ng::from_str(&rendered).unwrap_or_else(|e| {
            panic!("{file}: the rendered document is not a pipeline spec: {e}")
        });

        match &spec.source {
            SourceSpec::Connector(reference) => {
                assert_eq!(&reference.id, source_id, "{file}: source connector id");
                let bin = reference.path.as_ref().unwrap_or_else(|| {
                    panic!("{file}: the source side carries no `path:` override")
                });
                assert!(
                    bin.starts_with("/repo/target/release"),
                    "{file}: the source path override must come from {{{{bins}}}}: {}",
                    bin.display()
                );
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
                    bin.starts_with("/repo/target/release"),
                    "{file}: the destination path override must come from {{{{bins}}}}: {}",
                    bin.display()
                );
            }
            other => panic!("{file}: the destination is not the `connector:` arm: {other:?}"),
        }
    }
}
