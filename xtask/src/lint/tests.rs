use std::fs;
use std::path::Path;

use super::lint_tree;
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
        "// TODO: later\nfn f() {}\n",
    );
    write(root.path(), "crates/a/src/x/mod.rs", "fn g() {}\n");
    write(
        root.path(),
        "crates/a/target/debug/build.rs",
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
fn generated_code_is_not_linted() {
    let root = tempfile::tempdir().unwrap();
    write(
        root.path(),
        "crates/a/src/generated/v1.rs",
        "// TODO: later\nfn f() {}\n",
    );
    assert!(lint_tree(root.path()).unwrap().is_empty());
}
