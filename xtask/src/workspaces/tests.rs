use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::{MANIFESTS, manifests, members, tracked, unlisted};

fn paths(paths: &[&str]) -> Vec<PathBuf> {
    paths.iter().map(PathBuf::from).collect()
}

fn write(root: &Path, relative: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, "version = 4\n").unwrap();
}

fn git(root: &Path, args: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

fn repository() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

#[test]
fn a_manifest_of_a_listed_workspace_is_listed() {
    let members = paths(&["Cargo.toml", "crates/a/Cargo.toml", "fuzz/Cargo.toml"]);
    assert_eq!(unlisted(&members, &members), paths(&[]));
    assert_eq!(unlisted(&[], &members), paths(&[]));
}

// A workspace is found by its manifest, so one that has committed no lockfile is found too.
#[test]
fn a_manifest_in_no_listed_workspace_is_unlisted() {
    let members = paths(&["Cargo.toml", "crates/a/Cargo.toml", "fuzz/Cargo.toml"]);
    for manifest in [
        "tools/Cargo.toml",
        "crates/b/Cargo.toml",
        "crates/a/nested/Cargo.toml",
        "fuzz/nested/Cargo.toml",
        "fuzz2/Cargo.toml",
    ] {
        let tracked = paths(&["Cargo.toml", manifest, "crates/a/Cargo.toml"]);
        assert_eq!(unlisted(&tracked, &members), paths(&[manifest]));
    }
}

// A file not yet added is found; one git ignores, as build output is, is not.
#[test]
fn manifests_are_those_git_tracks_or_would_track() {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "--quiet"]);
    fs::write(root.path().join(".gitignore"), "/target\n").unwrap();
    for file in [
        "Cargo.toml",
        "fuzz/Cargo.toml",
        "tools/deep/Cargo.toml",
        "target/package/Cargo.toml",
        "tools/Cargo.toml.orig",
        "tools/NotCargo.toml",
        "tools/Cargo.lock",
    ] {
        write(root.path(), file);
    }
    git(root.path(), &["add", "Cargo.toml"]);
    let expected = paths(&["Cargo.toml", "fuzz/Cargo.toml", "tools/deep/Cargo.toml"]);
    assert_eq!(manifests(root.path()).unwrap(), expected);
    assert_eq!(
        tracked(root.path(), &["tools"]).unwrap(),
        paths(&[
            "tools/Cargo.lock",
            "tools/Cargo.toml.orig",
            "tools/NotCargo.toml",
            "tools/deep/Cargo.toml"
        ])
    );
}

#[test]
fn a_tree_that_is_no_repository_has_no_files_to_list() {
    let root = tempfile::tempdir().unwrap();
    assert!(manifests(&root.path().join("missing")).is_err());
    assert!(members(&root.path().join("missing")).is_err());
}

#[test]
fn the_members_of_every_listed_workspace_are_found() {
    let members = members(repository()).unwrap();
    for manifest in [
        "Cargo.toml",
        "crates/rdlt-engine/Cargo.toml",
        "crates/rdlt-adopt/Cargo.toml",
        "xtask/Cargo.toml",
        "fuzz/Cargo.toml",
    ] {
        assert!(members.contains(&PathBuf::from(manifest)), "{manifest}");
    }
}

#[test]
fn this_repository_lists_every_workspace_it_has_each_with_its_lockfile() {
    let root = repository();
    let unlisted = unlisted(&manifests(root).unwrap(), &members(root).unwrap());
    assert_eq!(unlisted, paths(&[]));
    for manifest in MANIFESTS {
        let lockfile = Path::new(manifest).with_file_name("Cargo.lock");
        let pathspec = lockfile.to_str().unwrap();
        assert_eq!(tracked(root, &[pathspec]).unwrap(), vec![lockfile.clone()]);
    }
}
