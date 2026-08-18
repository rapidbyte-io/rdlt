//! The five e2e cell pipeline templates are LIVE documents, not dead files
//! that fail only in the recorded session. Each one is rendered exactly as
//! a run renders it (the same `substitute` over the same keys the product
//! side provides), then pushed through the REAL gates a run would hit: the
//! facade's `document::Document` parse (deny_unknown_fields — a typoed
//! top-level or `connector:` key dies here), and both sides must name the
//! expected reverse-DNS id with a `{{bins}}`-resolved path override (only
//! the `connector:` arm can carry one). The opaque `config:` blocks are
//! validated by the spawned connector's own gate at the handshake, in the
//! connectors repository. The cell registry side (ids, verify, competitor
//! arms) is load-checked by the selftest case's whole-registry load.

use std::collections::BTreeMap;

use rdlt::document::Document;
use rdlt_bench::product::SUBSTITUTION_KEYS;
use rdlt_bench::template::substitute;

use crate::cases::support;

/// The five cell pipelines and the connector each side must name.
const PIPELINES: &[(&str, &str, &str)] = &[
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

/// The render-time substitution map with stand-in values, built from the
/// product's OWN key slice — so a template referencing anything the product
/// does not provide leaves `{{…}}` behind and fails the no-residue
/// assertion. A hand-written copy would make the test agree with itself:
/// a key renamed in the product would leave this list stale and the suite
/// green while a live session died on an unrendered key. The values are
/// shaped only where a value is inspected: `bins` must look like a release
/// directory because the assertions check the rendered `path:` overrides
/// came from it.
fn stand_in_subs() -> BTreeMap<String, String> {
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
        // A key added to the product without a stand-in here would
        // otherwise render as an empty string and quietly pass.
        other => panic!(
            "product::SUBSTITUTION_KEYS gained `{other}` — give it a stand-in \
             value here so the templates are rendered the way a run renders them"
        ),
    };
    SUBSTITUTION_KEYS
        .iter()
        .map(|key| ((*key).to_owned(), value(key).to_owned()))
        .collect()
}

#[test]
fn the_five_pipelines_render_parse_and_name_their_connectors() {
    let dir = support::repo_paths().cells_dir.join("pipelines");
    let subs = stand_in_subs();
    for (file, source_id, destination_id) in PIPELINES {
        let path = dir.join(file);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let rendered = substitute(&raw, &subs);
        assert!(
            !rendered.contains("{{"),
            "{file}: a `{{{{…}}}}` key survived rendering — the template references \
             a key the product side does not provide:\n{rendered}"
        );

        let spec: Document = serde_yaml_ng::from_str(&rendered).unwrap_or_else(|e| {
            panic!("{file}: the rendered document is not a pipeline document: {e}")
        });

        let reference = &spec.source;
        assert_eq!(&reference.id, source_id, "{file}: source connector id");
        let bin = reference
            .path
            .as_ref()
            .unwrap_or_else(|| panic!("{file}: the source side carries no `path:` override"));
        assert!(
            bin.starts_with("/repo/target/release"),
            "{file}: the source path override must come from {{{{bins}}}}: {}",
            bin.display()
        );
        let reference = &spec.destination;
        assert_eq!(
            &reference.id, destination_id,
            "{file}: destination connector id"
        );
        let bin = reference
            .path
            .as_ref()
            .unwrap_or_else(|| panic!("{file}: the destination side carries no `path:` override"));
        assert!(
            bin.starts_with("/repo/target/release"),
            "{file}: the destination path override must come from {{{{bins}}}}: {}",
            bin.display()
        );
    }
}
