use std::fs;
use std::path::{Path, PathBuf};

use super::{AUDITED_CRATE, check, check_tree, target_roots};
use crate::rules::Rule;

const FORBIDS: &str = "//! A crate.\n\n#![forbid(unsafe_code)]\n\nfn f() {}\n";

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

/// A tree holding the audited crate as it is audited.
fn tree() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    // The manifest is no Rust source: what an audited file may not say, it may.
    let manifest = "[package]\nname = \"rdlt-adopt\"\ninclude = [\"src\"]\n";
    write(
        root.path(),
        &format!("{AUDITED_CRATE}/Cargo.toml"),
        manifest,
    );
    for file in ["src/lib.rs", "src/tests.rs"] {
        let contents = "#[expect(unsafe_code, reason = \"audited\")]\nfn f() {}\n";
        write(root.path(), &format!("{AUDITED_CRATE}/{file}"), contents);
    }
    root
}

fn found(root: &Path, roots: &[&str]) -> Vec<(String, Rule)> {
    let roots: Vec<PathBuf> = roots.iter().map(PathBuf::from).collect();
    check_tree(root, &roots)
        .unwrap()
        .into_iter()
        .map(|(path, finding)| (path.display().to_string(), finding.rule))
        .collect()
}

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn a_target_root_with_a_crate_level_forbid_passes() {
    let sources = [
        FORBIDS,
        "#![forbid(unsafe_code)]",
        "#![forbid(unsafe_code,)]\n",
        "#![forbid(missing_docs, unsafe_code)]\n",
        "#![no_main]\n#![ forbid ( unsafe_code ) ]\nfn main() {}\n",
        "//! Docs.\n#![cfg(unix)]\n#![forbid(unsafe_code)]\nmod a;\n",
        "/*! Docs. */\n#![allow(dead_code)]\n#![forbid(unsafe_code)]\n",
    ];
    for source in sources {
        let root = tree();
        write(root.path(), "crates/a/src/lib.rs", source);
        let found = found(root.path(), &["crates/a/src/lib.rs"]);
        assert_eq!(found, Vec::new(), "source: {source:?}");
    }
}

// What reads like the attribute to a pattern over text, and is not the crate's to the compiler.
#[test]
fn a_target_root_without_a_crate_level_forbid_is_reported() {
    let sources = [
        "",
        "//! A crate.\n\nfn f() {}\n",
        "#![deny(unsafe_code)]\n",
        "#![forbid(missing_docs)]\n",
        "#![forbid(unsafe_code_in_name)]\n",
        "#![forbid(clippy::unsafe_code)]\n",
        "#![cfg_attr(all(), forbid(unsafe_code))]\n",
        "#![doc = \"#![forbid(unsafe_code)]\"]\n",
        "//! #![forbid(unsafe_code)]\n",
        "mod scoped {\n    #![forbid(unsafe_code)]\n}\n",
        "#[forbid(unsafe_code)]\nfn f() {}\n",
        "fn f() {}\n#![forbid(unsafe_code)]\n",
        "const A: &str = \"\n#![forbid(unsafe_code)]\n\";\n",
        "const A: &core::ffi::CStr = cr#\"\" \n#![forbid(unsafe_code)]\n \"#;\n",
        "macro_rules! m { () => { #![forbid(unsafe_code)] } }\n",
    ];
    for source in sources {
        let root = tree();
        write(root.path(), "crates/a/src/lib.rs", source);
        let found = found(root.path(), &["crates/a/src/lib.rs"]);
        let expected = vec![("crates/a/src/lib.rs".to_owned(), Rule::UnforbiddenUnsafe)];
        assert_eq!(found, expected, "source: {source:?}");
    }
}

#[test]
fn a_target_root_that_cannot_be_read_as_rust_is_reported() {
    for source in [
        "#![forbid(unsafe_code)]\nconst A: &str = \"open;\n",
        "#![forbid(unsafe_code]\n",
    ] {
        let root = tree();
        write(root.path(), "crates/a/src/lib.rs", source);
        let found = found(root.path(), &["crates/a/src/lib.rs"]);
        let expected = vec![("crates/a/src/lib.rs".to_owned(), Rule::UnforbiddenUnsafe)];
        assert_eq!(found, expected, "source: {source:?}");
    }
    let root = tree();
    assert!(check_tree(root.path(), &[PathBuf::from("crates/a/src/missing.rs")]).is_err());
}

#[test]
fn every_target_root_is_checked_and_only_roots() {
    let root = tree();
    write(root.path(), "crates/a/src/lib.rs", FORBIDS);
    write(root.path(), "crates/a/src/module.rs", "fn f() {}\n");
    write(root.path(), "crates/a/src/bin/tool.rs", "fn main() {}\n");
    write(root.path(), "crates/a/examples/e.rs", "fn main() {}\n");
    write(root.path(), "crates/a/tests/it/main.rs", FORBIDS);
    write(root.path(), "fuzz/fuzz_targets/t.rs", "#![no_main]\n");
    let roots = [
        "crates/a/src/lib.rs",
        "crates/a/src/bin/tool.rs",
        "crates/a/examples/e.rs",
        "crates/a/tests/it/main.rs",
        "fuzz/fuzz_targets/t.rs",
    ];
    let expected: Vec<(String, Rule)> = [
        "crates/a/src/bin/tool.rs",
        "crates/a/examples/e.rs",
        "fuzz/fuzz_targets/t.rs",
    ]
    .iter()
    .map(|path| ((*path).to_owned(), Rule::UnforbiddenUnsafe))
    .collect();
    assert_eq!(found(root.path(), &roots), expected);
}

#[test]
fn the_audited_crate_need_not_forbid() {
    let root = tree();
    let audited = format!("{AUDITED_CRATE}/src/lib.rs");
    assert_eq!(found(root.path(), &[&audited]), Vec::new());
    // Only its own directory is exempt, not one whose name starts alike.
    let beside = format!("{AUDITED_CRATE}-more/src/lib.rs");
    write(root.path(), &beside, "fn f() {}\n");
    assert_eq!(
        found(root.path(), &[&beside]),
        vec![(beside.clone(), Rule::UnforbiddenUnsafe)]
    );
}

// Whatever its extension or directory, a file beside the audited ones is not audited.
#[test]
fn the_audited_crate_holds_only_its_audited_files() {
    for extra in [
        "src/more.rs",
        "src/more.inc",
        "src/generated/more.rs",
        "src/target/more.rs",
        "src/lib/more.rs",
        "build.rs",
        "examples/more.rs",
        "tests/it/main.rs",
        "more.txt",
    ] {
        let root = tree();
        let path = format!("{AUDITED_CRATE}/{extra}");
        write(root.path(), &path, "fn f() {}\n");
        let expected = vec![(path, Rule::UnauditedFile)];
        assert_eq!(found(root.path(), &[]), expected, "file: {extra}");
    }
}

#[test]
fn a_tree_without_the_audited_crate_is_clean() {
    let root = tempfile::tempdir().unwrap();
    assert_eq!(found(root.path(), &[]), Vec::new());
}

// Code the audited files would compile from another file is code nobody audited.
#[test]
fn the_audited_files_bring_in_no_other_file() {
    let sources = [
        ("include!(\"../../other.rs\");\n", 1),
        ("fn f() {\n    include!(\"x.inc\")\n}\n", 2),
        ("const A: &str = include_str!(\"x\");\n", 1),
        ("const A: &[u8] = include_bytes!(\"x\");\n", 1),
        ("#[path = \"../../other.rs\"]\nmod other;\n", 1),
        ("\n\n#[cfg_attr(unix, path = \"x.rs\")]\nmod other;\n", 3),
        ("mod other {\n    #![path = \"x\"]\n}\n", 2),
        (
            "macro_rules! m {\n    () => {\n        include!(\"x\");\n    };\n}\n",
            3,
        ),
        (
            "macro_rules! m {\n    ($i:ident) => { $i!(\"x\"); };\n}\nm!(include);\n",
            4,
        ),
    ];
    for file in ["src/lib.rs", "src/tests.rs"] {
        for (source, line) in sources {
            let root = tree();
            let path = format!("{AUDITED_CRATE}/{file}");
            write(root.path(), &path, source);
            let findings = check_tree(root.path(), &[]).unwrap();
            let found: Vec<_> = findings
                .iter()
                .map(|(path, finding)| (path.display().to_string(), finding.rule, finding.line))
                .collect();
            let expected = vec![(path, Rule::IncludedCode, line)];
            assert_eq!(found, expected, "source: {source:?}");
        }
    }
}

#[test]
fn the_audited_files_may_name_paths_and_inclusion_in_prose_and_text() {
    let sources = [
        "// include!(\"x\")\n/// #[path = \"x\"]\nfn f() {}\n",
        "const A: &str = \"include!(x) #[path = y]\";\n",
        "fn f(path: &str) -> &str {\n    path\n}\n",
        "fn f() {\n    let included = 1;\n    let path = included;\n}\n",
        "fn f() {\n    let mut path = 1;\n    let _unit = [path = 2];\n}\n",
        "fn f(path: u8) -> bool {\n    #[cfg(unix)]\n    let unix = path == 1;\n    unix\n}\n",
        "#[derive(path::Trait)]\n#[cfg(feature = \"path\")]\nstruct A;\n",
    ];
    for source in sources {
        let root = tree();
        write(root.path(), &format!("{AUDITED_CRATE}/src/lib.rs"), source);
        assert_eq!(found(root.path(), &[]), Vec::new(), "source: {source:?}");
    }
}

#[test]
fn an_audited_file_that_cannot_be_read_as_rust_is_reported() {
    let root = tree();
    let path = format!("{AUDITED_CRATE}/src/lib.rs");
    write(root.path(), &path, "const A: &str = \"open;\n");
    assert_eq!(found(root.path(), &[]), vec![(path, Rule::IncludedCode)]);
}

#[test]
fn the_roots_of_every_kind_of_target_are_found() {
    let roots = target_roots(&repository()).unwrap();
    for root in [
        "crates/rdlt-connector/src/lib.rs",
        "crates/rdlt-connector-macros/src/lib.rs",
        "crates/rdlt-adopt/src/lib.rs",
        "crates/rdlt-certify/src/main.rs",
        "crates/rdlt-connector-reference/src/bin/rdlt-connector-files.rs",
        "crates/rdlt-engine/examples/crash_run/main.rs",
        "crates/rdlt-engine/tests/it/main.rs",
        "crates/rdlt-engine/benches/shred.rs",
        "fuzz/fuzz_targets/shred.rs",
        "xtask/src/main.rs",
    ] {
        assert!(roots.contains(&PathBuf::from(root)), "{root}");
    }
    assert!(roots.iter().all(|root| root.is_relative()));
}

#[test]
fn this_repository_forbids_unsafe_code_outside_its_audited_crate() {
    let found: Vec<_> = check(&repository())
        .unwrap()
        .into_iter()
        .map(|(path, finding)| (path, finding.rule))
        .collect();
    assert_eq!(found, Vec::new());
}

// The audited crate's own tree, as committed, is the audited set: the check is not vacuous.
#[test]
fn this_repository_has_its_audited_crate() {
    assert!(
        repository()
            .join(AUDITED_CRATE)
            .join("src/lib.rs")
            .is_file()
    );
}
