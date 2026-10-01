use std::path::Path;

use super::{prune, remove};
use crate::files::manifest::{self, Listed, Manifest, TableFiles};
use crate::rooted::Dir;

fn touch(root: &Path, path: &str) {
    let path = root.join(path);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, b"x").unwrap();
}

#[test]
fn a_removed_file_takes_the_directories_of_its_own_it_left_empty() {
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    let gone = "staging/7/load/3/rows/table/1.jsonl".to_owned();
    let sibling = "staging/7/load/3/other/table/1.jsonl".to_owned();
    let alone = "staging/7/load/4/rows/table/1.jsonl".to_owned();
    for path in [&gone, &sibling, &alone] {
        touch(root.path(), path);
    }
    remove(&dir, [&gone].into_iter());
    let exists = |path: &str| root.path().join(path).exists();
    assert!(!exists("staging/7/load/3/rows"), "its directories stay");
    assert!(exists(&sibling) && exists(&alone));
    // The last file of a segment takes the segment's directory, and no directory writers of
    // the session share: the load's, the epoch's and the staging directory stay.
    remove(&dir, [&sibling, &alone].into_iter());
    assert!(!exists("staging/7/load/3") && !exists("staging/7/load/4"));
    assert!(exists("staging/7/load"));
}

#[test]
fn a_path_that_cannot_be_removed_is_left_and_the_others_still_go() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("kept"), b"x").unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    let real = "staging/7/load/3/rows/table/1.jsonl".to_owned();
    touch(root.path(), &real);
    std::os::unix::fs::symlink(outside.path(), root.path().join("staging").join("link")).unwrap();
    let paths = [
        "staging/7/load/9/rows/table/1.jsonl".to_owned(),
        "staging/link/kept".to_owned(),
        format!("{}/kept", outside.path().display()),
        "../kept".to_owned(),
        "staging".to_owned(),
        "manifests/1.json".to_owned(),
        String::new(),
        real.clone(),
    ];
    touch(root.path(), "manifests/1.json");
    remove(&dir, paths.iter());
    assert!(!root.path().join(&real).exists());
    assert!(outside.path().join("kept").exists());
    assert!(root.path().join("manifests/1.json").exists());
    assert!(root.path().join("staging").join("link").exists());
}

#[test]
fn only_what_the_latest_manifest_does_not_list_is_pruned() {
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    let listed = "staging/7/load/1/rows/table/1.jsonl".to_owned();
    let unlisted = "staging/7/load/2/rows/table/1.jsonl".to_owned();
    for path in [&listed, &unlisted] {
        touch(root.path(), path);
    }
    let paths = [listed.clone(), unlisted.clone()];
    // No manifest reads: nothing is known to be unlisted, and nothing goes.
    touch(root.path(), "manifests/00000000000000000001.json");
    prune(&dir, &paths);
    assert!(root.path().join(&listed).exists() && root.path().join(&unlisted).exists());
    std::fs::remove_file(root.path().join("manifests/00000000000000000001.json")).unwrap();
    let mut manifest = Manifest {
        version: 1,
        ..Manifest::default()
    };
    let file = Listed {
        path: listed.clone(),
        rows: 1,
        bytes: 1,
    };
    let table = TableFiles {
        files: vec![file],
        ..TableFiles::default()
    };
    manifest.tables.insert("rows".to_owned(), table);
    assert!(manifest::put(&dir, &manifest).unwrap());
    prune(&dir, &paths);
    assert!(root.path().join(&listed).exists());
    assert!(!root.path().join(&unlisted).exists());
    // A pipeline with no manifest lists nothing.
    let fresh = tempfile::tempdir().unwrap();
    touch(fresh.path(), &unlisted);
    prune(&Dir::ambient(fresh.path()).unwrap(), &paths);
    assert!(!fresh.path().join(&unlisted).exists());
}
