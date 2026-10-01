use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use rdlt_connector::{ConnectorErrorKind, Epoch, Field, LogicalType, PipelineId, TableSchema};

use super::super::manifest::{Listed, Manifest, TableFiles};
use super::super::{manifest, tables};
use super::{checked, discard, existing, next_epoch, private};
use crate::rooted::Dir;
use crate::rooted::trace;

const WAIT: Duration = Duration::from_secs(20);

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
    let [older, own, newer] = [4, 5, 6].map(|epoch| staged(root.path(), epoch));
    let dir = Dir::ambient(root.path()).unwrap();
    discard(&dir, Epoch(5)).expect("the discard runs");
    assert!(!older.exists(), "an older session's staging stays");
    assert!(!older.parent().unwrap().exists(), "its directory stays");
    assert!(
        own.exists() && newer.exists(),
        "a current or newer session's staging went"
    );
}

#[test]
fn a_discard_keeps_what_the_latest_manifest_lists_and_enters_no_link() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let outside = tempfile::tempdir().expect("a temporary directory");
    std::fs::write(outside.path().join("kept"), b"x").unwrap();
    let [listed, unlisted] = [1, 2].map(|epoch| staged(root.path(), epoch));
    let staging = root.path().join("staging");
    std::os::unix::fs::symlink(outside.path(), staging.join("1").join("link")).unwrap();
    std::os::unix::fs::symlink(outside.path(), staging.join("0")).unwrap();
    // Not a session's staging: no epoch names it.
    std::fs::write(staging.join("notes"), b"x").unwrap();
    // A link under a listed name is not the listed file.
    std::fs::create_dir(staging.join("3")).unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("kept"),
        staging.join("3").join("rows.jsonl"),
    )
    .unwrap();
    let file = |path: &str| Listed {
        path: path.to_owned(),
        rows: 1,
        bytes: 3,
    };
    let mut manifest = Manifest {
        version: 1,
        ..Manifest::default()
    };
    let table = TableFiles {
        files: vec![file("staging/1/rows.jsonl"), file("staging/3/rows.jsonl")],
        ..TableFiles::default()
    };
    manifest.tables.insert("rows".to_owned(), table);
    let dir = Dir::ambient(root.path()).unwrap();
    assert!(manifest::put(&dir, &manifest).unwrap());
    discard(&dir, Epoch(9)).expect("the discard runs");
    assert!(listed.exists() && !unlisted.exists());
    assert!(std::fs::symlink_metadata(staging.join("1").join("link")).is_err());
    assert!(std::fs::symlink_metadata(staging.join("0")).is_err());
    assert!(!staging.join("3").exists());
    assert!(staging.join("notes").exists() && !staging.join("2").exists());
    assert!(outside.path().join("kept").exists());
    // A pipeline that staged nothing has nothing to discard.
    let empty = tempfile::tempdir().unwrap();
    discard(&Dir::ambient(empty.path()).unwrap(), Epoch(9)).unwrap();
}

#[test]
fn a_check_makes_the_private_directory_durable_in_the_root_and_leaves_no_probe() {
    let root = tempfile::tempdir().expect("a temporary directory");
    trace::clear();
    let held = parking_lot::Mutex::new(None);
    let rdlt = super::held_or_opened(root.path(), &held).expect("the directory opens");
    checked(&rdlt).expect("the check passes");
    let synced = trace::synced();
    assert!(synced.contains(&root.path().to_owned()), "{synced:?}");
    assert!(root.path().join("_rdlt").is_dir());
    assert_eq!(
        std::fs::read_dir(root.path().join("_rdlt"))
            .unwrap()
            .count(),
        0
    );
    // The directory is held from its first open, and the root's path must still lead to it:
    // a root moved aside, or another directory put in its place, takes no write unnoticed.
    let moved = root.path().with_extension("moved");
    std::fs::rename(root.path(), &moved).unwrap();
    let replaced = |held| {
        let error = super::held_or_opened(root.path(), held).expect_err("the root is gone");
        assert_eq!(error.kind(), ConnectorErrorKind::Config);
        assert_eq!(error.code(), Some("root_replaced"));
    };
    replaced(&held);
    std::fs::create_dir(root.path()).unwrap();
    replaced(&held);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    // Put back, it is the held directory again.
    std::fs::remove_dir(root.path()).unwrap();
    std::fs::rename(&moved, root.path()).unwrap();
    let again = super::held_or_opened(root.path(), &held).expect("the held directory");
    assert!(Arc::ptr_eq(&rdlt, &again));
    checked(&again).expect("the check passes");
}

#[test]
fn a_private_directory_is_read_only_where_it_exists_and_is_its_user_s() {
    use std::os::unix::fs::PermissionsExt as _;
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    assert!(existing(&root).unwrap().is_none(), "no root");
    std::fs::create_dir(&root).unwrap();
    assert!(existing(&root).unwrap().is_none(), "no private directory");
    assert!(!root.join("_rdlt").exists(), "a reader made one");
    private(&root).unwrap();
    assert!(existing(&root).unwrap().is_some());
    let shared = std::fs::Permissions::from_mode(0o777);
    std::fs::set_permissions(root.join("_rdlt"), shared).unwrap();
    for refused in [existing(&root).map(drop), private(&root).map(drop)] {
        assert_eq!(refused.unwrap_err().kind(), ConnectorErrorKind::Config);
    }
    // A root that is a file is no root.
    let file = base.path().join("file");
    std::fs::write(&file, b"").unwrap();
    assert_eq!(
        existing(&file).unwrap_err().kind(),
        ConnectorErrorKind::Config
    );
    assert_eq!(
        private(&file.join("below")).unwrap_err().kind(),
        ConnectorErrorKind::Config
    );
}

fn schema() -> TableSchema {
    TableSchema::new(vec![Field::new("x", LogicalType::Int64, true)]).unwrap()
}

#[test]
fn an_open_removes_the_catalogs_of_tables_its_pipeline_dropped_before_anything_creates_them() {
    // A drop committed, but the catalogs outlived it: the process ended before removing them.
    let root = tempfile::tempdir().expect("a temporary directory");
    let rdlt = Dir::ambient(root.path()).unwrap();
    let (owner, other) = (
        PipelineId::parse("a").unwrap(),
        PipelineId::parse("b").unwrap(),
    );
    let dir = rdlt.dir_created("pipeline").unwrap();
    for (table, pipeline) in [("dropped", &owner), ("taken", &other)] {
        tables::claim(&rdlt, table, pipeline).expect("the table is claimed");
        tables::update(&rdlt, table, |_| Ok(Some(schema()))).expect("it has columns");
    }
    let left = Manifest {
        dropped: ["dropped".to_owned(), "taken".to_owned()].into(),
        ..Manifest::default()
    };
    assert!(manifest::put(&dir, &left).expect("the manifest is written"));
    let opened = next_epoch(&dir, &rdlt, &owner, WAIT).expect("the open succeeds");
    assert!(opened.dropped.is_empty());
    assert_eq!((opened.version, opened.epoch), (1, Epoch(1)));
    assert_eq!(tables::read(&rdlt, "dropped").unwrap(), None);
    assert_eq!(tables::owner(&rdlt, "dropped").unwrap(), None);
    // A table another pipeline created since is its own.
    assert_eq!(tables::owner(&rdlt, "taken").unwrap().as_deref(), Some("b"));
    assert!(tables::read(&rdlt, "taken").unwrap().is_some());
}

#[test]
fn an_overtaken_session_s_release_leaves_a_table_a_newer_session_created_again() {
    let root = tempfile::tempdir().unwrap();
    let rdlt = Dir::ambient(root.path()).unwrap();
    let pipeline = PipelineId::parse("a").unwrap();
    let dir = rdlt.dir_created("pipeline").unwrap();
    let create = || {
        tables::locked(&rdlt, "t", WAIT, || {
            tables::claim(&rdlt, "t", &pipeline)?;
            tables::update(&rdlt, "t", |_| Ok(Some(schema())))
        })
    };
    // The older session committed its drop of the table: its manifest lists it as dropped.
    create().unwrap();
    let dropping = Manifest {
        version: 1,
        epoch: Epoch(1),
        dropped: ["t".to_owned()].into(),
        ..Manifest::default()
    };
    assert!(manifest::put(&dir, &dropping).unwrap());
    // A newer session opens, removing the catalog, and creates the table again.
    let opened = next_epoch(&dir, &rdlt, &pipeline, WAIT).unwrap();
    assert_eq!(opened.epoch, Epoch(2));
    create().unwrap();
    // The older session's release lands only now, as its commit ends.
    let still = || super::still_dropped(&dir, "t");
    tables::release(&rdlt, "t", &pipeline, WAIT, still).unwrap();
    assert_eq!(tables::owner(&rdlt, "t").unwrap().as_deref(), Some("a"));
    assert_eq!(tables::read(&rdlt, "t").unwrap(), Some(schema()));
    // While its own manifest is the latest, its release removes the catalog.
    let again = Manifest {
        version: 3,
        epoch: Epoch(2),
        dropped: ["t".to_owned()].into(),
        ..Manifest::default()
    };
    assert!(manifest::put(&dir, &again).unwrap());
    tables::release(&rdlt, "t", &pipeline, WAIT, still).unwrap();
    assert_eq!(tables::owner(&rdlt, "t").unwrap(), None);
}

#[test]
fn a_manifest_at_the_end_of_its_versions_or_epochs_is_followed_by_none() {
    let root = tempfile::tempdir().unwrap();
    let rdlt = Dir::ambient(root.path()).unwrap();
    let pipeline = PipelineId::parse("a").unwrap();
    for (version, epoch) in [(u64::MAX, 1), (1, u64::MAX)] {
        let last = Manifest {
            version,
            epoch: Epoch(epoch),
            ..Manifest::default()
        };
        let ended = tempfile::tempdir().unwrap();
        let dir = Dir::ambient(ended.path()).unwrap();
        assert!(manifest::put(&dir, &last).unwrap());
        let error = next_epoch(&dir, &rdlt, &pipeline, WAIT).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Data);
        assert_eq!(manifest::latest(&dir).unwrap(), Some(last));
    }
    let fresh = rdlt.dir_created("pipeline").unwrap();
    let first = next_epoch(&fresh, &rdlt, &pipeline, WAIT).unwrap();
    assert_eq!((first.version, first.epoch), (1, Epoch(1)));
}

#[test]
fn a_table_is_still_dropped_only_while_the_latest_manifest_lists_nothing_for_it() {
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    assert!(!super::still_dropped(&dir, "t").unwrap(), "no manifest");
    let mut manifest = Manifest {
        version: 1,
        dropped: ["t".to_owned()].into(),
        ..Manifest::default()
    };
    assert!(manifest::put(&dir, &manifest).unwrap());
    assert!(super::still_dropped(&dir, "t").unwrap());
    assert!(!super::still_dropped(&dir, "u").unwrap());
    // Listed as dropped and as published: it was created again, and is dropped no longer.
    manifest.version = 2;
    manifest
        .tables
        .insert("t".to_owned(), TableFiles::default());
    assert!(manifest::put(&dir, &manifest).unwrap());
    assert!(!super::still_dropped(&dir, "t").unwrap());
}

#[test]
fn what_commits_wrote_and_no_manifest_lists_is_swept_and_what_writers_stage_is_not() {
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    let touch = |path: &str| {
        let path = root.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"x").unwrap();
    };
    let listed = "staging/5/load/merged/c1/rows/table/0.jsonl";
    let kept = [
        listed,
        // A writer of the session is staging these: no commit wrote them.
        "staging/5/load/7/rows/table/1.jsonl",
        "staging/5/other/8/rows/table/1.jsonl",
        // A newer session's.
        "staging/6/load/merged/c1/rows/table/0.jsonl",
    ];
    let swept = [
        "staging/5/load/merged/c0/rows/table/0.jsonl",
        "staging/5/load/compacted/c0/rows/table/0.jsonl",
        "staging/5/other/tombstones/c0/rows/table/0.jsonl",
        "staging/4/load/3/rows/table/1.jsonl",
    ];
    for path in kept.iter().chain(&swept) {
        touch(path);
    }
    touch("staging/5/stray");
    let mut manifest = Manifest {
        version: 1,
        ..Manifest::default()
    };
    let file = Listed {
        path: listed.to_owned(),
        rows: 1,
        bytes: 1,
    };
    let table = TableFiles {
        files: vec![file],
        ..TableFiles::default()
    };
    manifest.tables.insert("rows".to_owned(), table);
    assert!(manifest::put(&dir, &manifest).unwrap());
    super::discard_superseded(&dir, Epoch(5)).unwrap();
    for path in kept {
        assert!(root.path().join(path).exists(), "{path}");
    }
    for path in swept {
        assert!(!root.path().join(path).exists(), "{path}");
    }
    assert!(!root.path().join("staging/5/load/compacted").exists());
    assert!(root.path().join("staging/5/stray").exists());
    // A pipeline whose session staged nothing has nothing to sweep.
    let empty = tempfile::tempdir().unwrap();
    super::discard_superseded(&Dir::ambient(empty.path()).unwrap(), Epoch(5)).unwrap();
}

/// Version `version` of a manifest listing one file of the table `rows`, at `path`.
fn listing(version: u64, path: &str) -> Manifest {
    let file = Listed {
        path: path.to_owned(),
        rows: 1,
        bytes: 9,
    };
    let table = TableFiles {
        files: vec![file],
        ..TableFiles::default()
    };
    let mut manifest = Manifest {
        version,
        ..Manifest::default()
    };
    manifest.tables.insert("rows".to_owned(), table);
    manifest
}

#[test]
fn a_reader_that_finds_a_listed_file_gone_reads_the_newer_manifest_or_reports_it_lost() {
    use std::cell::Cell;
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    let write = |path: &str, line: &str| {
        let path = root.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, line).unwrap();
    };
    // A commit superseded the first manifest's file while a reader held that manifest.
    let (stale, current) = (
        listing(1, "staging/1/old.jsonl"),
        listing(2, "staging/1/new.jsonl"),
    );
    write("staging/1/new.jsonl", "{\"id\":2}\n");
    assert!(manifest::put(&dir, &current).unwrap());
    let schema = Arc::new(schema().to_arrow());
    let reads = Cell::new(0);
    let read = super::published_by(&dir, "rows", &schema, |dir| {
        reads.set(reads.get() + 1);
        if reads.get() == 1 {
            Ok(Some(stale.clone()))
        } else {
            manifest::latest(dir)
        }
    })
    .unwrap();
    assert_eq!(read.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        reads.get(),
        3,
        "the stale read, the check and the read again"
    );
    // A file gone under the manifest that is still the latest is lost: the reader says so.
    std::fs::remove_file(root.path().join("staging/1/new.jsonl")).unwrap();
    let lost = super::published_by(&dir, "rows", &schema, manifest::latest).unwrap_err();
    assert_eq!(lost.code(), Some("file_missing"));
    // A reader that only ever sees stale manifests gives up.
    let versions = Cell::new(10);
    let endless = super::published_by(&dir, "rows", &schema, |_| {
        versions.set(versions.get() + 1);
        Ok(Some(listing(versions.get(), "staging/1/old.jsonl")))
    })
    .unwrap_err();
    assert_eq!(endless.kind(), ConnectorErrorKind::Transient);
    // Any other failure is the reader's at once, and a pipeline without the table has no rows.
    write("staging/1/new.jsonl", "not json\n");
    let unreadable = super::published_by(&dir, "rows", &schema, manifest::latest).unwrap_err();
    assert_eq!(unreadable.kind(), ConnectorErrorKind::Data);
    assert!(
        super::published_by(&dir, "none", &schema, manifest::latest)
            .unwrap()
            .is_empty()
    );
}
