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

/// One pipeline's sessions over a root, driven as the engine drives them but on the test's own
/// thread, so each step they take is recorded and can be refused.
pub(crate) struct Sessions {
    pub(crate) root: tempfile::TempDir,
    pub(crate) location: Location,
    pub(in crate::files::session) shared: parking_lot::Mutex<super::Shared>,
}

impl Sessions {
    /// A first session of `format` under a fresh root.
    pub(crate) fn new(format: FileFormat) -> Self {
        let (root, location) = location(format);
        let mut sessions = Self {
            root,
            location,
            shared: parking_lot::Mutex::default(),
        };
        sessions.open(1);
        sessions
    }

    /// Opens the pipeline's next session for `load`, as an open does: the next epoch, and what
    /// older sessions staged and never published discarded.
    pub(crate) fn open(&mut self, load: u128) {
        let (dir, rdlt) = (&self.location.dir, &self.location.rdlt);
        let wait = self.location.lock_wait;
        let manifest =
            crate::files::destination::next_epoch(dir, rdlt, &self.location.pipeline, wait)
                .unwrap();
        self.location.epoch = manifest.epoch;
        self.location.load_id = LoadId::from_parts(UNIX_EPOCH, load);
        self.shared = parking_lot::Mutex::default();
        crate::files::destination::discard(dir, manifest.epoch).unwrap();
    }

    /// Creates `table` with `schema`, as a schema change does.
    pub(crate) fn create(
        &self,
        table: &rdlt_connector::TableRef,
        schema: &rdlt_connector::TableSchema,
    ) {
        let (rdlt, wait) = (&self.location.rdlt, self.location.lock_wait);
        crate::files::tables::locked(rdlt, &table.name, wait, || {
            super::claim(&self.location, &table.name)?;
            crate::files::tables::update(rdlt, &table.name, |_| Ok(Some(schema.clone())))
        })
        .unwrap();
        self.shared
            .lock()
            .names
            .insert(super::path_key(&table.path), table.name.to_string());
    }

    /// Stages `batch` of `table` as `segment`.
    pub(crate) fn stage(
        &self,
        table: &rdlt_connector::TableRef,
        segment: u64,
        batch: arrow_array::RecordBatch,
    ) {
        let buffered = vec![(rdlt_connector::SegmentId(segment), batch)];
        super::stage(&self.location, &self.shared, table, buffered).unwrap();
    }

    /// Commit `seq` of `load` of `segments`.
    pub(crate) fn meta(
        &self,
        load: u128,
        seq: u64,
        segments: &[u64],
    ) -> rdlt_connector::CommitMeta {
        let seq = (1..seq).fold(rdlt_connector::CommitSeq::FIRST, |seq, _| seq.next());
        rdlt_connector::CommitMeta {
            load_id: LoadId::from_parts(UNIX_EPOCH, load),
            commit_seq: seq,
            epoch: self.location.epoch,
            segments: segments
                .iter()
                .copied()
                .map(rdlt_connector::SegmentId)
                .collect(),
            state_delta: Vec::new(),
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
            drop_tables: Vec::new(),
        }
    }

    pub(crate) fn commit(
        &self,
        meta: &rdlt_connector::CommitMeta,
    ) -> rdlt_connector::Result<rdlt_connector::Receipt> {
        super::commit::commit(&self.location, &self.shared, meta)
    }

    /// The `id` of every row the latest manifest publishes for `table`, in order; every file it
    /// lists must read.
    pub(crate) fn ids(&self, table: &str) -> Vec<i64> {
        use arrow_array::cast::AsArray as _;
        let schema = crate::files::tables::read(&self.location.rdlt, table)
            .unwrap()
            .map_or_else(arrow_schema::Schema::empty, |schema| schema.to_arrow());
        let schema = Arc::new(schema);
        let Some(manifest) = crate::files::manifest::latest(&self.location.dir).unwrap() else {
            return Vec::new();
        };
        let Some(files) = manifest.tables.get(table) else {
            return Vec::new();
        };
        let mut ids = Vec::new();
        for file in &files.files {
            let read = crate::files::manifest::read(&self.location.dir, &file.path, &schema);
            for batch in read.unwrap() {
                let column = batch.column_by_name("id").unwrap();
                let column = arrow_cast::cast(column, &arrow_schema::DataType::Int64).unwrap();
                ids.extend(
                    column
                        .as_primitive::<arrow_array::types::Int64Type>()
                        .values(),
                );
            }
        }
        ids
    }

    /// The data files under the pipeline's staging, relative to the pipeline's directory.
    pub(crate) fn data_files(&self) -> Vec<String> {
        let base = self.location.dir.path().to_owned();
        let mut pending = vec![base.join("staging")];
        let mut found = Vec::new();
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    let relative = path.strip_prefix(&base).unwrap();
                    found.push(relative.to_string_lossy().into_owned());
                }
            }
        }
        found.sort();
        found
    }
}
