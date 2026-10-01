use std::fs;
use std::path::Path;

use super::lint_tree;
use crate::codegen::GENERATED;
use crate::rules::Rule;

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

#[test]
fn reports_findings_with_repository_relative_paths() {
    let root = tempfile::tempdir().unwrap();
    write(
        root.path(),
        "crates/a/src/lib.rs",
        "#![forbid(unsafe_code)]\n// TODO: later\nfn f() {}\n",
    );
    write(root.path(), "crates/a/src/x/mod.rs", "fn g() {}\n");
    write(
        root.path(),
        "fuzz/target/debug/build.rs",
        "// TODO: ignored\n",
    );
    write(root.path(), "crates/a/README.md", "// TODO: not rust\n");

    let found: Vec<(String, Rule)> = lint_tree(root.path())
        .unwrap()
        .into_iter()
        .map(|(path, finding)| (path.display().to_string(), finding.rule))
        .collect();

    assert_eq!(
        found,
        vec![
            ("crates/a/src/lib.rs".to_owned(), Rule::TodoWithoutIssue),
            ("crates/a/src/x/mod.rs".to_owned(), Rule::ModRs),
        ]
    );
}

#[test]
fn a_tree_without_source_roots_is_clean() {
    let root = tempfile::tempdir().unwrap();
    assert!(lint_tree(root.path()).unwrap().is_empty());
}

#[test]
fn the_generated_file_is_not_linted() {
    let root = tempfile::tempdir().unwrap();
    write(root.path(), GENERATED, "// TODO: later\nfn f() {}\n");
    assert!(lint_tree(root.path()).unwrap().is_empty());
}

// Only the fuzzing build's output and the generated file are skipped, by their paths: a directory
// that merely shares a name with them holds source like any other.
#[test]
fn a_directory_named_like_build_output_or_generated_code_is_linted() {
    let sibling = Path::new(GENERATED).with_file_name("other.rs");
    for path in [
        "crates/a/src/generated/v1.rs",
        "crates/a/src/target/v1.rs",
        "crates/a/target/v1.rs",
        "xtask/src/generated/v1.rs",
        "fuzz/fuzz_targets/target/v1.rs",
        sibling.to_str().unwrap(),
    ] {
        let root = tempfile::tempdir().unwrap();
        write(root.path(), path, "// TODO: later\nfn f() {}\n");
        let found: Vec<(String, Rule)> = lint_tree(root.path())
            .unwrap()
            .into_iter()
            .map(|(path, finding)| (path.display().to_string(), finding.rule))
            .collect();
        assert_eq!(found, vec![(path.to_owned(), Rule::TodoWithoutIssue)]);
    }
}
