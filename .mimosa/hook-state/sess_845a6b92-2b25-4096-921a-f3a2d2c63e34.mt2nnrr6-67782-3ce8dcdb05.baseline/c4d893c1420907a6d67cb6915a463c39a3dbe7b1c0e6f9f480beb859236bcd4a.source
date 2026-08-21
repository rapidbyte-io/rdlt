//! The one-dependency rule, enforced: a connector authored on the sdk
//! carries EXACTLY ONE rdlt runtime dependency — this crate — and
//! reaches the SPI through its `spi` re-export. Dev-dependencies are
//! exempt (the testkit is the verification half).
//!
//! Only ADOPTED connectors are bound, and only IN-TREE ones can be: the
//! first-party connectors (with their recorded `rdlt-connector-sqlcore`
//! exception for SQL destinations) live in the rdlt-connectors
//! repository, whose manifests this test cannot read — the rule binds
//! them there by review. What stays in this tree is the reference
//! connector, the contract's own.

use std::path::Path;

/// (crate, allowed rdlt entries in `[dependencies]`).
///
/// `rdlt-testkit` may additionally appear OPTIONAL-ONLY (checked below):
/// a connector that ships container fixtures behind a `fixtures` feature
/// routes them through the testkit's gate posture by design, and an
/// optional dep must live in `[dependencies]` to be feature-gated. A
/// NON-optional testkit dependency stays a violation.
const ADOPTED: &[(&str, &[&str])] = &[("rdlt-connector-reference", &["rdlt-connector-sdk"])];

/// The rdlt crates named in a manifest's `[dependencies]` section (table
/// headers end the section; `rdlt-` prefixed keys are collected), with
/// whether each entry is `optional`.
fn rdlt_runtime_deps(manifest: &str) -> Vec<(String, bool)> {
    let mut in_dependencies = false;
    let mut found = Vec::new();
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_dependencies = trimmed == "[dependencies]";
            continue;
        }
        if in_dependencies
            && trimmed.starts_with("rdlt-")
            && let Some((name, spec)) = trimmed.split_once('=')
        {
            found.push((name.trim().to_owned(), spec.contains("optional = true")));
        }
    }
    found
}

#[test]
fn adopted_connectors_depend_on_the_sdk_alone() {
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/ is the parent of this crate");
    for (name, allowed) in ADOPTED {
        let manifest_path = crates_dir.join(name).join("Cargo.toml");
        let manifest = std::fs::read_to_string(&manifest_path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", manifest_path.display()));
        let deps = rdlt_runtime_deps(&manifest);
        assert!(
            deps.iter().any(|(dep, _)| dep == "rdlt-connector-sdk"),
            "{name}: an adopted connector depends on the sdk"
        );
        let forbidden: Vec<&String> = deps
            .iter()
            .filter(|(dep, optional)| {
                !(allowed.contains(&dep.as_str()) || (dep == "rdlt-testkit" && *optional))
            })
            .map(|(dep, _)| dep)
            .collect();
        assert!(
            forbidden.is_empty(),
            "{name}: rdlt runtime dependencies beyond the allowed set \
             {allowed:?}: {forbidden:?} — the SPI and vocabulary are reached \
             through the sdk's `spi` re-export, never directly (rdlt-testkit \
             is tolerated ONLY as an optional fixtures-feature dep)"
        );
    }
}

/// The parser itself is checked against a shape that must FAIL — a
/// vacuously-empty section reading as compliance would be a vacuous
/// pass.
#[test]
fn the_manifest_parser_sees_dependencies() {
    let manifest = "[dependencies]\nrdlt-connector = { workspace = true }\nserde = \"1\"\n\
                    [dev-dependencies]\nrdlt-testkit = { workspace = true }\n";
    assert_eq!(
        rdlt_runtime_deps(manifest),
        vec![("rdlt-connector".to_owned(), false)]
    );
}
