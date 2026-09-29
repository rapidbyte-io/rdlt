use std::path::Path;

use rdlt_connector::Epoch;

use super::discard;

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
