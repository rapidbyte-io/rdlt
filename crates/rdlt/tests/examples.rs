//! The examples are LOAD-BEARING: every `examples/*/pipeline.yaml`
//! must parse through the real Spec gate, and every connector
//! reference in it — rich spelling or `connector:` — must desugar to
//! a known reverse-DNS id. No spawn in the default gate: whether the
//! documents RUN against real binaries is the spawn suites' business;
//! this holds the document language itself against the examples.

use std::path::{Path, PathBuf};

/// Every id the desugar table can produce — an example desugaring to
/// anything else names a connector this workspace does not ship.
const KNOWN_IDS: &[&str] = &[
    "io.rapidbyte.rest",
    "io.rapidbyte.oracle",
    "io.rapidbyte.file",
    "io.rapidbyte.postgres",
    "io.rapidbyte.duckdb",
    "io.rapidbyte.iceberg",
    "io.rapidbyte.snowflake",
];

fn examples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples")
}

/// Every example directory carries a pipeline.yaml that parses, and
/// both of its sides desugar to a shipped connector's id. The count is
/// pinned so a deleted or unreadable example fails rather than
/// shrinking the property.
#[test]
fn every_example_pipeline_parses_and_desugars_to_known_connectors() {
    let mut seen = 0;
    for entry in std::fs::read_dir(examples_dir()).expect("examples/ exists") {
        let entry = entry.expect("entry");
        let dir = entry.path();
        let path = dir.join("pipeline.yaml");
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must read: {e}"));
        let spec: rdlt::pipeline_spec::Spec = serde_yaml::from_str(&text)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must parse: {e}"));
        // The example's own directory is the base, so a path-form
        // config resolves relative to the files beside it.
        let source = spec
            .source
            .desugar(&dir)
            .unwrap_or_else(|e| panic!("{name}: the source must desugar: {e}"));
        let destination = spec
            .destination
            .desugar(&dir)
            .unwrap_or_else(|e| panic!("{name}: the destination must desugar: {e}"));
        for reference in [&source, &destination] {
            assert!(
                KNOWN_IDS.contains(&reference.id.as_str()),
                "{name}: `{}` is not a shipped connector's id",
                reference.id
            );
        }
        seen += 1;
    }
    assert_eq!(seen, 7, "every example directory carries a pipeline.yaml");
}
