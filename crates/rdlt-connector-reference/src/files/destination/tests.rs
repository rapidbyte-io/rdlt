use std::path::Path;

use rdlt_connector::Epoch;

use super::super::io::tests::SYNCED;
use super::{checked, discard};

fn staged(dir: &Path, epoch: u64) -> std::path::PathBuf {
    let path = dir
        .join("staging")
        .join(epoch.to_string())
        .join("rows.jsonl");
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("the directory is created");
    std::fs::write(&path, "{}\n").expect("the file is written");
    path
}

#[test]
fn an_open_discards_what_older_sessions_staged_and_keeps_its_own_and_newer_ones() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let dir = root.path().join("pipeline");
    let [older, own, newer] = [4, 5, 6].map(|epoch| staged(&dir, epoch));
    discard(root.path(), &dir, Epoch(5)).expect("the discard runs");
    assert!(!older.exists(), "an older session's staging stays");
    assert!(
        own.exists() && newer.exists(),
        "a current or newer session's staging went"
    );
}

#[test]
fn a_check_makes_the_catalog_directory_durable_in_the_root() {
    let root = tempfile::tempdir().expect("a temporary directory");
    SYNCED.with(|synced| synced.borrow_mut().clear());
    checked(root.path()).expect("the check passes");
    let synced = SYNCED.with(|synced| synced.borrow().clone());
    assert!(synced.contains(&root.path().to_owned()), "{synced:?}");
    assert!(root.path().join("_rdlt").is_dir());
}
