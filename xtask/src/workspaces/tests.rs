use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::{MANIFESTS, lockfiles, unlisted};

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

#[test]
fn a_lockfile_of_a_listed_workspace_is_listed() {
    assert_eq!(
        unlisted(&paths(&["Cargo.lock", "fuzz/Cargo.lock"])),
        paths(&[])
    );
    assert_eq!(unlisted(&[]), paths(&[]));
}

#[test]
fn a_lockfile_beside_no_listed_manifest_is_unlisted() {
    for lockfile in [
        "tools/Cargo.lock",
        "crates/a/Cargo.lock",
        "fuzz/nested/Cargo.lock",
        "fuzz2/Cargo.lock",
    ] {
        let found = unlisted(&paths(&["Cargo.lock", lockfile, "fuzz/Cargo.lock"]));
        assert_eq!(found, paths(&[lockfile]));
    }
}

// A lockfile not yet added is found; one git ignores, as build output is, is not.
#[test]
fn lockfiles_are_those_git_tracks_or_would_track() {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "--quiet"]);
    fs::write(root.path().join(".gitignore"), "/target\n").unwrap();
    for file in [
        "Cargo.lock",
        "fuzz/Cargo.lock",
        "tools/deep/Cargo.lock",
        "target/package/Cargo.lock",
        "tools/Cargo.lock.orig",
        "tools/NotCargo.lock",
    ] {
        write(root.path(), file);
    }
    git(root.path(), &["add", "Cargo.lock"]);
    let expected = paths(&["Cargo.lock", "fuzz/Cargo.lock", "tools/deep/Cargo.lock"]);
    assert_eq!(lockfiles(root.path()).unwrap(), expected);
}

#[test]
fn a_tree_that_is_no_repository_has_no_lockfiles_to_list() {
    let root = tempfile::tempdir().unwrap();
    let inside = root.path().join("missing");
    assert!(lockfiles(&inside).is_err());
}

#[test]
fn this_repository_lists_every_workspace_it_has() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let found = lockfiles(root).unwrap();
    assert_eq!(unlisted(&found), paths(&[]));
    for manifest in MANIFESTS {
        let lockfile = Path::new(manifest).with_file_name("Cargo.lock");
        assert!(found.contains(&lockfile), "{manifest}");
    }
}
