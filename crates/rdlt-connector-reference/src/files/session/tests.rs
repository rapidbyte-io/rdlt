use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use rdlt_connector::{Epoch, GenerationId, LoadId, PipelineId};

use super::Location;
use crate::files::FileFormat;
use crate::rooted::Dir;

/// A session's location under a fresh root: the private directory is the root itself, the
/// pipeline's directory `pipeline` in it.
pub(crate) fn location(format: FileFormat) -> (tempfile::TempDir, Location) {
    let root = tempfile::tempdir().unwrap();
    let rdlt = Dir::ambient(root.path()).unwrap();
    let dir = rdlt.dir_created("pipeline").unwrap();
    let location = Location {
        rdlt: Arc::new(rdlt),
        pipeline: PipelineId::parse("p").unwrap(),
        dir: Arc::new(dir),
        format,
        epoch: Epoch(7),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        lock_wait: Duration::from_secs(20),
    };
    (root, location)
}

#[test]
fn a_staged_file_s_place_names_its_session_its_segment_its_table_and_its_part() {
    let (root, location) = location(FileFormat::Arrow);
    let load = location.load_id.to_string();
    let (names, file) = location.staged(&["3".to_owned()], "rows", None, 9);
    assert_eq!(names, ["staging", "7", load.as_str(), "3", "rows", "table"]);
    assert_eq!(file, "9.arrow");
    let segment = ["merged".to_owned(), "4".to_owned()];
    let (names, file) = location.staged(&segment, "rows", Some(GenerationId(5)), 0);
    assert_eq!(
        names,
        ["staging", "7", load.as_str(), "merged", "4", "rows", "g5"]
    );
    assert_eq!(file, "0.arrow");
    let dir = location.staging(&names).unwrap();
    let made = root.path().join("pipeline").join(names.join("/"));
    assert_eq!(dir.path(), made);
    assert!(made.is_dir());
    // A name that is no component makes nothing.
    let (mut names, _) = location.staged(&["3".to_owned()], "rows", None, 1);
    names.push("../out".to_owned());
    assert_eq!(
        location.staging(&names).unwrap_err().code(),
        Some("invalid_name")
    );
    let (_, jsonl) = Location {
        format: FileFormat::Jsonl,
        ..location
    }
    .staged(&[], "rows", None, 2);
    assert_eq!(jsonl, "2.jsonl");
}

#[test]
fn a_staged_file_s_directory_and_every_directory_made_for_it_are_synced() {
    use crate::rooted::trace;
    let (root, location) = location(FileFormat::Jsonl);
    let pipeline = root.path().join("pipeline");
    let (names, file) = location.staged(&["3".to_owned()], "rows", None, 1);
    location.staging(&names[..3]).unwrap();
    trace::clear();
    let dir = location.staging(&names).unwrap();
    let ids: arrow_array::ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![1, 2]));
    let batch = arrow_array::RecordBatch::try_from_iter([("id", ids)]).unwrap();
    location.format.write(&dir, &file, &[batch]).unwrap();
    let synced = trace::synced();
    // Each new directory's entry is synced in its parent, and the file's in its own; the
    // directories that were there are not synced again.
    let load = pipeline.join(names[..3].join("/"));
    let expected = [
        load.clone(),
        load.join("3"),
        load.join("3").join("rows"),
        load.join("3").join("rows").join("table"),
    ];
    assert_eq!(synced, expected);
}

#[test]
fn a_file_a_commit_writes_is_named_for_its_own_load_and_number_and_for_no_other_try() {
    use rdlt_connector::{CommitMeta, CommitSeq, SegmentSet};
    let meta = CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 42),
        commit_seq: CommitSeq::FIRST.next(),
        epoch: Epoch(7),
        segments: SegmentSet::default(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    let [kind, first] = Location::written_by("merged", &meta).unwrap();
    let [_, second] = Location::written_by("merged", &meta).unwrap();
    assert_eq!(kind, "merged");
    let commit = format!("{}-2-", meta.load_id);
    assert!(
        first.starts_with(&commit) && first.len() == commit.len() + 32,
        "{first}"
    );
    assert_ne!(first, second);
    assert!(crate::rooted::component(first.as_ref()).is_ok());
}
