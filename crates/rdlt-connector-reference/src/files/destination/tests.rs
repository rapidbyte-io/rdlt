use std::path::Path;

use rdlt_connector::{Epoch, Field, LogicalType, PipelineId, TableSchema};

use super::super::io::tests::SYNCED;
use super::super::{manifest, tables};
use super::{checked, discard, next_epoch};

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

#[test]
fn an_open_removes_the_catalogs_of_tables_its_pipeline_dropped_before_anything_creates_them() {
    // A drop committed, but the catalogs outlived it: the process ended before removing them.
    let root = tempfile::tempdir().expect("a temporary directory");
    let (owner, other) = (
        PipelineId::parse("a").unwrap(),
        PipelineId::parse("b").unwrap(),
    );
    let dir = manifest::pipeline_dir(root.path(), &owner);
    let schema = TableSchema::new(vec![Field::new("x", LogicalType::Int64, true)]).unwrap();
    for (table, pipeline) in [("dropped", &owner), ("taken", &other)] {
        tables::claim(root.path(), table, pipeline).expect("the table is claimed");
        tables::update(root.path(), table, |_| Ok(Some(schema.clone()))).expect("it has columns");
    }
    let left = manifest::Manifest {
        dropped: ["dropped".to_owned(), "taken".to_owned()].into(),
        ..manifest::Manifest::default()
    };
    assert!(manifest::put(&dir, &left).expect("the manifest is written"));
    let opened = next_epoch(&dir, root.path(), &owner).expect("the open succeeds");
    assert!(opened.dropped.is_empty());
    assert_eq!(tables::read(root.path(), "dropped").unwrap(), None);
    assert_eq!(tables::owner(root.path(), "dropped").unwrap(), None);
    // A table another pipeline created since is its own.
    assert_eq!(
        tables::owner(root.path(), "taken").unwrap().as_deref(),
        Some("b")
    );
    assert!(tables::read(root.path(), "taken").unwrap().is_some());
}
