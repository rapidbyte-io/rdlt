use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{Epoch, GenerationId, PipelineId};

use super::Location;
use crate::files::FileFormat;
use crate::rooted::Dir;

/// A session's location under a fresh root: the private directory is the root itself, the
/// pipeline's directory `pipeline` in it.
pub(crate) fn location(format: FileFormat) -> (tempfile::TempDir, Location) {
    let root = crate::scratch::tempdir().unwrap();
    let rdlt = Dir::ambient(root.path()).unwrap();
    let dir = rdlt.dir_created("pipeline").unwrap();
    let location = Location {
        rdlt: Arc::new(rdlt),
        pipeline: PipelineId::parse("p").unwrap(),
        dir: Arc::new(dir),
        format,
        epoch: Epoch(7),
        lock_wait: Duration::from_secs(20),
    };
    (root, location)
}

#[test]
fn a_staged_file_s_place_names_its_session_its_segment_its_table_and_its_part() {
    let (root, location) = location(FileFormat::Arrow);
    let (names, file) = location.staged(&["3".to_owned()], "rows", None, 9);
    assert_eq!(names, ["staging", "7", "3", "rows", "table"]);
    assert_eq!(file, "9.arrow");
    let segment = ["merged".to_owned(), "4".to_owned()];
    let (names, file) = location.staged(&segment, "rows", Some(GenerationId(5)), 0);
    assert_eq!(names, ["staging", "7", "merged", "4", "rows", "g5"]);
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
    location.staging(&names[..2]).unwrap();
    trace::clear();
    let dir = location.staging(&names).unwrap();
    let ids: arrow_array::ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![1, 2]));
    let batch = arrow_array::RecordBatch::try_from_iter([("id", ids)]).unwrap();
    location.format.write(&dir, &file, &[batch]).unwrap();
    let synced = trace::synced();
    // Each new directory's entry is synced in its parent, and the file's in its own; the
    // directories that were there are not synced again.
    let session = pipeline.join(names[..2].join("/"));
    let expected = [
        session.clone(),
        session.join("3"),
        session.join("3").join("rows"),
        session.join("3").join("rows").join("table"),
    ];
    assert_eq!(synced, expected);
}

#[test]
fn a_file_a_commit_writes_is_named_for_its_own_key_and_for_no_other_try() {
    use rdlt_connector::{CommitKey, CommitMeta, SegmentSet};
    let meta = CommitMeta {
        key: CommitKey {
            epoch: Epoch(7),
            seq: 2,
        },
        after: None,
        segments: SegmentSet::default(),
        abandoned: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    let [kind, first] = Location::written_by("merged", &meta).unwrap();
    let [_, second] = Location::written_by("merged", &meta).unwrap();
    assert_eq!(kind, "merged");
    let commit = format!("{}-2-", meta.key.epoch);
    assert!(
        first.starts_with(&commit) && first.len() == commit.len() + 32,
        "{first}"
    );
    assert_ne!(first, second);
    assert!(crate::rooted::component(first.as_ref()).is_ok());
}

#[test]
fn a_table_that_keeps_changing_is_checked_a_bounded_number_of_times_and_no_other_error_again() {
    use rdlt_connector::{ConnectorError, ConnectorErrorKind, Result};

    use super::fitted::TABLE_CHANGED;
    use super::{CHECKS, checked_while_changed};
    let failed = |code: &'static str| {
        move || -> Result<()> {
            Err(ConnectorError::new(ConnectorErrorKind::Transient, "checked").with_code(code))
        }
    };
    // How many checks ran, and the code of the error they ended with, if they did, where the
    // first `changes` of them find the table changed.
    let checked = |changes: u32, then: &dyn Fn() -> Result<()>| {
        let mut calls = 0;
        let done = checked_while_changed(|| {
            calls += 1;
            assert!(calls <= CHECKS, "checked {calls} times");
            if calls <= changes {
                failed(TABLE_CHANGED)()
            } else {
                then()
            }
        });
        let code = done.err().map(|error| error.code().map(str::to_owned));
        (calls, code)
    };
    let refused = |code: &str| Some(Some(code.to_owned()));
    let taken = || Ok(());
    assert_eq!(checked(u32::MAX, &taken), (CHECKS, refused(TABLE_CHANGED)));
    assert_eq!(checked(CHECKS - 1, &taken), (CHECKS, None));
    let locked = failed("lock_timeout");
    assert_eq!(checked(0, &locked), (1, refused("lock_timeout")));
}

/// One pipeline's sessions over a root, driven as the engine drives them but on the test's own
/// thread, so each step they take is recorded and can be refused.
pub(crate) struct Sessions {
    pub(crate) root: tempfile::TempDir,
    pub(crate) location: Location,
    pub(in crate::files::session) shared: parking_lot::Mutex<super::Shared>,
    /// The receipt of the last commit that had landed when the session opened.
    pub(crate) landed: Option<rdlt_connector::Receipt>,
}

impl Sessions {
    /// A first session of `format` under a fresh root.
    pub(crate) fn new(format: FileFormat) -> Self {
        let (root, location) = location(format);
        let mut sessions = Self {
            root,
            location,
            shared: parking_lot::Mutex::default(),
            landed: None,
        };
        sessions.open();
        sessions
    }

    /// Opens the pipeline's next session, as an open does: the next epoch, the last commit that
    /// landed, and what older sessions staged and never published discarded.
    pub(crate) fn open(&mut self) {
        let (dir, rdlt) = (&self.location.dir, &self.location.rdlt);
        let wait = self.location.lock_wait;
        let manifest =
            crate::files::destination::next_epoch(dir, rdlt, &self.location.pipeline, wait)
                .unwrap();
        self.location.epoch = manifest.epoch;
        self.landed = manifest.fence().landed;
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

    /// Commit `seq` of the session, of `segments`, expecting the commit the latest manifest
    /// holds landed.
    pub(crate) fn meta(&self, seq: u64, segments: &[u64]) -> rdlt_connector::CommitMeta {
        let latest = crate::files::manifest::latest(&self.location.dir).unwrap();
        rdlt_connector::CommitMeta {
            key: rdlt_connector::CommitKey {
                epoch: self.location.epoch,
                seq,
            },
            after: latest.and_then(|manifest| manifest.fence().landed.map(|landed| landed.key)),
            segments: segments
                .iter()
                .copied()
                .map(rdlt_connector::SegmentId)
                .collect(),
            abandoned: rdlt_connector::SegmentSet::new(),
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
