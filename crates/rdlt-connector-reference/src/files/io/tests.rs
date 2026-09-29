use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::ConnectorErrorKind;

use super::super::FileFormat;
use super::created;

thread_local! {
    /// Every directory this thread synced, in order.
    pub(crate) static SYNCED: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
}

fn rows() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch")
}

#[test]
fn a_new_file_s_directory_and_every_directory_made_for_it_are_synced() {
    let root = tempfile::tempdir().expect("a temporary directory");
    std::fs::create_dir(root.path().join("existing")).expect("a directory");
    let path = root.path().join("existing/new/newer/rows.jsonl");
    SYNCED.with(|synced| synced.borrow_mut().clear());
    FileFormat::Jsonl
        .write(&path, &rows())
        .expect("the file writes");
    let synced = SYNCED.with(|synced| synced.borrow().clone());
    let dir = |relative: &str| root.path().join(relative);
    // Each new directory's entry is synced in its parent, and the file's in its own.
    for expected in [
        dir("existing"),
        dir("existing/new"),
        dir("existing/new/newer"),
    ] {
        assert!(
            synced.contains(&expected),
            "{expected:?} unsynced: {synced:?}"
        );
    }
    assert!(!synced.contains(&root.path().to_owned()), "{synced:?}");
}

#[test]
fn a_listed_file_that_is_missing_is_lost_for_good() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let error = FileFormat::Jsonl
        .read(&root.path().join("gone.jsonl"), &schema)
        .expect_err("the file is missing");
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("file_missing"));
}

#[test]
fn a_file_another_writer_linked_first_loses_and_any_other_failure_is_an_error() {
    let path = std::path::Path::new("manifests/1.json");
    assert!(created(Ok(()), path).expect("created"));
    let lost = std::io::Error::from(std::io::ErrorKind::AlreadyExists);
    assert!(!created(Err(lost), path).expect("lost to another writer"));
    let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
    let error = created(Err(denied), path).expect_err("a failure");
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
}

#[test]
fn a_relative_directory_whose_parent_is_the_working_directory_is_created() {
    let scratch = tempfile::Builder::new()
        .tempdir_in(".")
        .expect("a temporary directory here");
    let name = scratch
        .path()
        .file_name()
        .expect("a name")
        .to_string_lossy()
        .into_owned();
    // One relative component, whose parent is the empty path.
    let relative = std::path::PathBuf::from(format!("{name}-root"));
    let created = super::create_dirs(&relative.join("rows"));
    let exists = relative.join("rows").is_dir();
    drop(std::fs::remove_dir_all(&relative));
    created.expect("the directories are created");
    assert!(exists);
}
