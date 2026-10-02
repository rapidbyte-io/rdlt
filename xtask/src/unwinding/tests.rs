use std::fs;
use std::path::Path;

use crate::lint::lint_tree;
use crate::rules::Rule;

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

const ROOT: &str = "#![forbid(unsafe_code)]\n";
const GUARD: &str = "#[cfg(panic = \"abort\")]\ncompile_error!(\"build with unwinding\");\n";
const CATCHES: &str = "fn f() {\n    drop(std::panic::catch_unwind(|| ()));\n}\n";

/// The findings of the unwinding rule in a tree of `files`, each with its path and line.
fn unguarded(files: &[(&str, &str)]) -> Vec<(String, usize)> {
    let root = tempfile::tempdir().unwrap();
    for (path, contents) in files {
        write(root.path(), path, contents);
    }
    let findings = lint_tree(root.path()).unwrap().into_iter();
    findings
        .filter(|(_, finding)| finding.rule == Rule::UnguardedUnwind)
        .map(|(path, finding)| (path.display().to_string(), finding.line))
        .collect()
}

#[test]
fn a_crate_that_catches_a_panic_must_refuse_to_build_without_unwinding() {
    let found = unguarded(&[
        ("crates/a/src/lib.rs", ROOT),
        ("crates/a/src/x.rs", CATCHES),
    ]);
    assert_eq!(found, [("crates/a/src/x.rs".to_owned(), 2)]);
    // The crate's root may catch one itself.
    let root = format!("{ROOT}{CATCHES}");
    let found = unguarded(&[("crates/a/src/lib.rs", &root)]);
    assert_eq!(found, [("crates/a/src/lib.rs".to_owned(), 3)]);
}

#[test]
fn a_guard_in_the_crates_root_covers_its_files_and_no_other_crates() {
    let guarded = format!("{ROOT}{GUARD}");
    let found = unguarded(&[
        ("crates/a/src/lib.rs", &guarded),
        ("crates/a/src/x.rs", CATCHES),
        ("crates/a/src/deep/y.rs", CATCHES),
        ("crates/b/src/lib.rs", ROOT),
        ("crates/b/src/x.rs", CATCHES),
    ]);
    assert_eq!(found, [("crates/b/src/x.rs".to_owned(), 2)]);
    // A guard may be one of several conditions, but must be of the crate, not of a module.
    let featured = format!(
        "{ROOT}#[cfg(all(feature = \"serve\", panic = \"abort\"))]\ncompile_error!(\"no\");\n"
    );
    let scoped = format!("{ROOT}mod m {{\n{GUARD}}}\n");
    let other = format!("{ROOT}#[cfg(unix)]\ncompile_error!(\"no\");\n");
    for (root, expected) in [(featured, 0), (scoped, 1), (other, 1)] {
        let found = unguarded(&[
            ("crates/a/src/lib.rs", &root),
            ("crates/a/src/x.rs", CATCHES),
        ]);
        assert_eq!(found.len(), expected, "{root}");
    }
}

#[test]
fn only_a_condition_that_holds_in_a_build_that_aborts_is_a_guard() {
    let guards = [
        "panic = \"abort\"",
        "all(feature = \"serve\", panic = \"abort\")",
        "not(not(panic = \"abort\"))",
        "all(any(panic = \"abort\"), not(panic = \"unwind\"))",
        "not(panic = \"unwind\")",
    ];
    let others = [
        "not(panic = \"abort\")",
        "any(panic = \"abort\", unix)",
        "panic = \"unwind\"",
        "all(feature = \"serve\", not(panic = \"abort\"))",
        "any(not(panic = \"abort\"))",
        "all(panic = \"abort\", not(any(panic = \"abort\")))",
        "not(any(unix, panic = \"abort\"))",
        "all(panic = \"abort\"",
    ];
    let cases = guards.iter().map(|guard| (guard, 0));
    for (condition, expected) in cases.chain(others.iter().map(|other| (other, 1))) {
        let root = format!("{ROOT}#[cfg({condition})]\ncompile_error!(\"no\");\n");
        let found = unguarded(&[
            ("crates/a/src/lib.rs", &root),
            ("crates/a/src/x.rs", CATCHES),
        ]);
        assert_eq!(found.len(), expected, "{condition}");
    }
}

#[test]
fn tests_and_mentions_of_catching_a_panic_need_no_guard() {
    let mentions = "// catch_unwind\nconst S: &str = \"catch_unwind\";\nfn catch_unwind_all() {}\n";
    let found = unguarded(&[
        ("crates/a/src/lib.rs", ROOT),
        ("crates/a/src/x/tests.rs", CATCHES),
        ("crates/a/tests/it/main.rs", CATCHES),
        ("crates/a/src/y.rs", mentions),
        ("xtask/src/z.rs", CATCHES),
    ]);
    assert_eq!(found, []);
}
