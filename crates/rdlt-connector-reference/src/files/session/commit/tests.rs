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

mod steps {
    //! A commit's durable steps: their order, and a crash at each of them.

    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
    use rdlt_connector::{
        DroppedTable, Field, LogicalType, MergeKey, SchemaVersion, TablePath, TableRef, TableSchema,
    };

    use crate::files::FileFormat;
    use crate::files::manifest;
    use crate::files::session::tests::Sessions;
    use crate::rooted::trace::{self, Step};

    fn table(merge: bool) -> TableRef {
        let merge = merge.then(|| MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        });
        TableRef {
            path: TablePath::new(["rows"]).unwrap(),
            name: "rows".into(),
            version: SchemaVersion(1),
            generation: None,
            merge,
        }
    }

    fn schema() -> TableSchema {
        TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("seq", LogicalType::Binary, false),
        ])
        .unwrap()
    }

    fn rows(ids: &[i64]) -> RecordBatch {
        let seqs = BinaryArray::from_iter_values(ids.iter().map(|id| id.to_be_bytes().repeat(2)));
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
            ("seq", Arc::new(seqs) as ArrayRef),
        ])
        .unwrap()
    }

    /// Sessions whose first commit published ids 1 and 2 to the table, and whose second, of
    /// id 3, is staged and not yet committed.
    fn pending(format: FileFormat, merge: bool) -> Sessions {
        let sessions = Sessions::new(format);
        let table = table(merge);
        sessions.create(&table, &schema());
        sessions.stage(&table, 1, rows(&[1, 2]));
        sessions.commit(&sessions.meta(1, 1, &[1])).unwrap();
        sessions.stage(&table, 2, rows(&[3]));
        sessions
    }

    fn at(steps: &[Step], wanted: impl Fn(&Step) -> bool) -> usize {
        steps
            .iter()
            .position(wanted)
            .unwrap_or_else(|| panic!("no such step in {steps:#?}"))
    }

    fn staged(path: &Path) -> bool {
        path.components().any(|part| part.as_os_str() == "staging")
    }

    /// Checks the order of a commit's `steps`: every file it wrote is durable, with its name,
    /// before the manifest is linked; the manifest's bytes are durable before its link and its
    /// name after; and nothing is removed from staging before that. The staged files it removed.
    fn ordered(steps: &[Step]) -> Vec<PathBuf> {
        let link = at(steps, |step| matches!(step, Step::Link(_)));
        let Step::Link(manifest) = &steps[link] else {
            unreachable!()
        };
        let manifests = manifest.parent().unwrap().to_owned();
        // The temporary the manifest was written to is synced just before it is linked.
        assert!(
            matches!(&steps[link - 1], Step::SyncFile(path) if path.parent() == Some(&manifests)),
            "{steps:#?}"
        );
        assert_eq!(steps[link + 1], Step::SyncDir(manifests), "{steps:#?}");
        for (index, step) in steps.iter().enumerate() {
            match step {
                Step::Create(path) if staged(path) => {
                    let synced = at(steps, |step| *step == Step::SyncFile(path.clone()));
                    let parent = path.parent().unwrap().to_owned();
                    let named = at(&steps[synced..], |step| {
                        *step == Step::SyncDir(parent.clone())
                    });
                    assert!(index < synced && synced + named < link, "{steps:#?}");
                }
                Step::Remove(path) | Step::RemoveDir(path) if staged(path) => {
                    assert!(index > link + 1, "removed before the manifest: {steps:#?}");
                }
                _ => {}
            }
        }
        steps
            .iter()
            .filter_map(|step| match step {
                Step::Remove(path) if staged(path) => Some(path.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_commit_makes_what_it_wrote_durable_before_its_manifest_and_removes_only_after() {
        for format in [FileFormat::Jsonl, FileFormat::Arrow] {
            for merge in [true, false] {
                let sessions = pending(format, merge);
                let before = sessions.data_files();
                trace::clear();
                sessions.commit(&sessions.meta(1, 2, &[2])).unwrap();
                let removed = ordered(&trace::steps());
                // Both files the commit read went, and the file it wrote stays, alone.
                let base = sessions.location.dir.path();
                let mut removed: Vec<String> = removed
                    .iter()
                    .map(|path| {
                        path.strip_prefix(base)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned()
                    })
                    .collect();
                removed.sort();
                assert_eq!(removed, before, "{format:?} {merge}");
                assert_eq!(sessions.data_files().len(), 1, "{format:?} {merge}");
                assert_eq!(sessions.ids("rows"), [1, 2, 3], "{format:?} {merge}");
            }
        }
    }

    /// What the latest manifest lists, sorted.
    fn listed(sessions: &Sessions) -> Vec<String> {
        let manifest = manifest::latest(&sessions.location.dir).unwrap().unwrap();
        let mut listed: Vec<String> = manifest.files().map(|file| file.path.clone()).collect();
        listed.sort();
        listed
    }

    #[test]
    fn a_commit_that_dies_at_any_step_publishes_all_of_it_or_none_and_lands_once_when_repeated() {
        for format in [FileFormat::Jsonl, FileFormat::Arrow] {
            for merge in [true, false] {
                let mut died = 0;
                for step in 0.. {
                    let mut sessions = pending(format, merge);
                    let meta = sessions.meta(1, 2, &[2]);
                    trace::crash_at(step);
                    drop(sessions.commit(&meta));
                    let crashed = trace::refused();
                    trace::clear();
                    if !crashed {
                        break;
                    }
                    died += 1;
                    // The next attempt opens the pipeline and finds every published row.
                    sessions.open(2);
                    let found = sessions.ids("rows");
                    assert!(
                        found == [1, 2] || found == [1, 2, 3],
                        "{format:?} {merge} step {step}: {found:?}"
                    );
                    // It stages the segment again and repeats the commit as it was logged.
                    sessions.stage(&table(merge), 2, rows(&[3]));
                    let mut again = sessions.meta(1, 2, &[2]);
                    again.epoch = sessions.location.epoch;
                    sessions.commit(&again).unwrap();
                    assert_eq!(
                        sessions.ids("rows"),
                        [1, 2, 3],
                        "{format:?} {merge} step {step}"
                    );
                    assert_eq!(sessions.data_files(), listed(&sessions), "step {step}");
                }
                assert!(died > 6, "{format:?} {merge}: only {died} steps");
            }
        }
    }

    #[test]
    fn a_commit_whose_manifest_was_linked_and_not_synced_is_made_durable_when_repeated() {
        let twin = pending(FileFormat::Jsonl, true);
        trace::clear();
        twin.commit(&twin.meta(1, 2, &[2])).unwrap();
        let steps = trace::steps();
        let link = at(&steps, |step| matches!(step, Step::Link(_)));
        // The same commit, refused the sync that makes its manifest's name durable.
        let sessions = pending(FileFormat::Jsonl, true);
        let meta = sessions.meta(1, 2, &[2]);
        trace::fail_at(link + 1);
        sessions.commit(&meta).expect_err("the sync is refused");
        trace::clear();
        assert_eq!(sessions.ids("rows"), [1, 2, 3], "the manifest is there");
        assert_eq!(sessions.data_files().len(), 3, "nothing is pruned yet");
        let receipt = sessions.commit(&meta).expect("the commit is answered");
        assert_eq!(receipt.rows, 1);
        let manifests = sessions.location.dir.path().join("manifests");
        let repeated = trace::steps();
        assert_eq!(repeated[0], Step::SyncDir(manifests), "{repeated:#?}");
        // And the commit's tail ran: what it superseded is gone, and its segment is held no more.
        assert_eq!(sessions.data_files(), listed(&sessions));
        assert!(sessions.shared.lock().staged.is_empty());
    }

    #[test]
    fn a_drop_that_dies_at_any_step_leaves_the_table_whole_or_gone_once_the_pipeline_opens() {
        let catalog = |sessions: &Sessions| sessions.root.path().join("tables").join("rows");
        let mut died = 0;
        for step in 0.. {
            let mut sessions = pending(FileFormat::Jsonl, false);
            let mut meta = sessions.meta(1, 2, &[]);
            let table = table(false);
            meta.drop_tables = vec![DroppedTable {
                path: table.path.clone(),
                name: table.name.clone(),
            }];
            trace::crash_at(step);
            drop(sessions.commit(&meta));
            let crashed = trace::refused();
            trace::clear();
            if !crashed {
                assert!(!catalog(&sessions).exists());
                break;
            }
            died += 1;
            sessions.open(2);
            let manifest = manifest::latest(&sessions.location.dir).unwrap().unwrap();
            assert!(manifest.dropped.is_empty(), "step {step}");
            if manifest.tables.contains_key("rows") {
                assert!(catalog(&sessions).exists(), "step {step}");
                assert_eq!(sessions.ids("rows"), [1, 2], "step {step}");
            } else {
                assert!(!catalog(&sessions).exists(), "step {step}");
            }
        }
        assert!(died > 6, "only {died} steps");
    }

    #[test]
    fn a_release_moves_the_catalog_away_durably_before_it_removes_it() {
        let sessions = pending(FileFormat::Jsonl, false);
        let mut meta = sessions.meta(1, 2, &[]);
        let table = table(false);
        meta.drop_tables = vec![DroppedTable {
            path: table.path.clone(),
            name: table.name.clone(),
        }];
        trace::clear();
        sessions.commit(&meta).unwrap();
        let steps = trace::steps();
        let link = at(&steps, |step| matches!(step, Step::Link(_)));
        let tables = sessions.root.path().join("tables");
        let moved = at(
            &steps,
            |step| matches!(step, Step::Rename(to) if to.parent().is_some_and(|dir| dir.ends_with("trash"))),
        );
        let synced = at(&steps[moved..], |step| {
            *step == Step::SyncDir(tables.clone())
        });
        let emptied = at(
            &steps,
            |step| matches!(step, Step::Remove(path) if path.components().any(|part| part.as_os_str() == "trash")),
        );
        assert!(
            link + 1 < moved && synced > 0 && moved + synced < emptied,
            "{steps:#?}"
        );
    }
}
