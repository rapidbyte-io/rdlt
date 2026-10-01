//! The files destination opens, creates and removes only what lies under its root: it follows no
//! link, takes no name that is not one path component, and reads only regular files of bounded
//! size.

use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::path::Path;
use std::time::{Duration, SystemTime};

use rdlt_connector::{
    CommitSeq, ConnectorErrorKind, DroppedTable, Field, LogicalType, TableChange, TablePath,
    TableRef, TableSchema,
};
use rdlt_connector_reference::files;
use serde_json::json;

use crate::fixtures::{
    connect, connect_with, dirs_under, files_under, ids, keyed, latest_manifest, merge_table, meta,
    open, pipeline_dir, plant_manifest, stage, table,
};

/// A directory outside the root holding files a co-tenant must not lose.
fn victim(base: &Path) -> std::path::PathBuf {
    let victim = base.join("victim");
    std::fs::create_dir_all(victim.join("sub")).expect("the fixture is made");
    std::fs::write(victim.join("a.txt"), "precious").expect("the fixture is made");
    std::fs::write(victim.join("sub").join("b.txt"), "precious").expect("the fixture is made");
    victim
}

#[tokio::test]
async fn an_open_never_deletes_through_a_planted_staging_link() {
    let base = crate::fixtures::tempdir().unwrap();
    let (root, victim) = (base.path().join("root"), victim(base.path()));
    let destination = connect(&root, "jsonl").await;
    let first = open(destination.as_ref(), 1).await;
    first.session.close().await.unwrap();
    let staging = pipeline_dir(&root).join("staging");
    std::fs::create_dir_all(staging.join("1").join("load")).unwrap();
    // One link named as an older session's staging, one below an older session's staging.
    symlink(&victim, staging.join("0")).unwrap();
    symlink(&victim, staging.join("1").join("load").join("segment")).unwrap();
    let second = destination.open(&crate::fixtures::context(2)).await;
    assert!(victim.join("a.txt").exists() && victim.join("sub").join("b.txt").exists());
    second.expect("the links are removed, not followed");
    assert!(std::fs::symlink_metadata(staging.join("0")).is_err());
    assert!(std::fs::symlink_metadata(staging.join("1")).is_err());
}

#[tokio::test]
async fn a_check_never_writes_through_a_planted_link() {
    let base = crate::fixtures::tempdir().unwrap();
    let root = base.path().join("root");
    let target = base.path().join("target");
    std::fs::write(&target, "precious").unwrap();
    std::fs::create_dir_all(root.join("_rdlt")).unwrap();
    std::fs::set_permissions(root.join("_rdlt"), std::fs::Permissions::from_mode(0o700)).unwrap();
    symlink(&target, root.join("_rdlt").join(".check")).unwrap();
    let destination = connect(&root, "jsonl").await;
    destination.check().await.expect("the root is writable");
    assert_eq!(std::fs::read(&target).unwrap(), b"precious");
}

/// The manifest entry listing `path`, as the destination writes one.
fn listed(path: &str) -> serde_json::Value {
    json!({ "path": path, "rows": 1, "bytes": 1 })
}

#[tokio::test]
async fn a_manifest_path_outside_the_pipeline_is_refused() {
    let base = crate::fixtures::tempdir().unwrap();
    let root = base.path().join("root");
    let secret = base.path().join("secret.jsonl");
    std::fs::write(&secret, "{\"id\":7}\n").unwrap();
    let (destination, reader) = connect_with(&root, json!({})).await;
    let mut opened = open(destination.as_ref(), 1).await;
    // The sequence may be missing, so a file holding only ids reads as rows of the table.
    let (_, batch) = keyed(&[1], 1);
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("seq", LogicalType::Binary, true),
    ])
    .unwrap();
    let rows = merge_table("rows");
    stage(&mut opened, &rows, &schema, batch.clone(), 1).await;
    opened
        .session
        .commit(&meta(&opened, 1, CommitSeq::FIRST, &[1]))
        .await
        .unwrap();
    let dir = pipeline_dir(&root);
    symlink(&secret, dir.join("staging").join("link.jsonl")).unwrap();
    std::fs::write(root.join("_rdlt").join("inside.jsonl"), "{\"id\":8}\n").unwrap();
    let escapes = [
        secret.to_string_lossy().into_owned(),
        "../../../../secret.jsonl".to_owned(),
        "staging/../../../../../secret.jsonl".to_owned(),
        "staging/link.jsonl".to_owned(),
        "manifests/00000000000000000001.json".to_owned(),
        "staging/./link.jsonl".to_owned(),
        "staging//link.jsonl".to_owned(),
        String::new(),
    ];
    for escape in escapes {
        let (latest, mut manifest) = latest_manifest(&root);
        manifest["tables"]["rows"]["files"] = json!([listed(&escape)]);
        plant_manifest(&latest, manifest);
        let read = reader.published(&rows).await;
        let error = read.expect_err(&escape);
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{escape}");
        // The next commit merges the table's listed files: it reads none of them either.
        stage(&mut opened, &rows, &schema, batch.clone(), 2).await;
        let commit = opened
            .session
            .commit(&meta(&opened, 1, CommitSeq::FIRST.next(), &[2]))
            .await;
        let error = commit.expect_err(&escape);
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{escape}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_manifest_named_for_another_version_is_refused_not_retried_forever() {
    let root = crate::fixtures::tempdir().unwrap();
    let destination = connect(root.path(), "jsonl").await;
    drop(open(destination.as_ref(), 1).await);
    let (latest, _) = latest_manifest(root.path());
    std::fs::copy(&latest, latest.with_file_name("00000000000000000100.json")).unwrap();
    let context = crate::fixtures::context(2);
    let opened = tokio::time::timeout(Duration::from_secs(20), destination.open(&context))
        .await
        .expect("the open ends");
    let Err(error) = opened else {
        panic!("a manifest named for another version was followed");
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("manifest_invalid"));
}

/// Names that are no identifier of the files destination.
fn bad_names() -> Vec<String> {
    vec![
        "../../../somedir".to_owned(),
        "..".to_owned(),
        ".".to_owned(),
        String::new(),
        "a/b".to_owned(),
        "/absolute".to_owned(),
        "nul\0byte".to_owned(),
        "sp ace".to_owned(),
        "dash-ed".to_owned(),
        "dot.ted".to_owned(),
        "ünï".to_owned(),
        "x".repeat(129),
    ]
}

#[tokio::test]
async fn a_dropped_name_that_is_no_identifier_is_refused() {
    let base = crate::fixtures::tempdir().unwrap();
    let root = base.path().join("a").join("b").join("root");
    std::fs::create_dir_all(base.path().join("a").join("somedir")).unwrap();
    let destination = connect(&root, "jsonl").await;
    let mut opened = open(destination.as_ref(), 1).await;
    let mut seq = CommitSeq::FIRST;
    for name in bad_names() {
        let mut dropping = meta(&opened, 1, seq, &[]);
        dropping.drop_tables = vec![DroppedTable {
            path: TablePath::new(["t"]).unwrap(),
            name: name.as_str().into(),
        }];
        let error = opened
            .session
            .commit(&dropping)
            .await
            .expect_err("the name is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{name:?}");
        assert_eq!(error.code(), Some("invalid_name"), "{name:?}");
        seq = seq.next();
    }
    // Nothing recorded the names: the pipeline opens again, and nothing was made outside the root.
    drop(open(destination.as_ref(), 2).await);
    let outside: Vec<_> = files_under(base.path())
        .into_iter()
        .filter(|path| !path.starts_with(&root))
        .collect();
    assert!(outside.is_empty(), "{outside:?}");
}

#[tokio::test]
async fn a_dropped_name_a_manifest_was_tampered_to_hold_is_refused() {
    let base = crate::fixtures::tempdir().unwrap();
    let root = base.path().join("a").join("b").join("root");
    let destination = connect(&root, "jsonl").await;
    drop(open(destination.as_ref(), 1).await);
    let (latest, mut manifest) = latest_manifest(&root);
    manifest["dropped"] = json!(["../../../../planted"]);
    plant_manifest(&latest, manifest);
    for load in [2, 3] {
        let Err(error) = destination.open(&crate::fixtures::context(load)).await else {
            panic!("a manifest dropping no identifier was followed");
        };
        assert_eq!(error.kind(), ConnectorErrorKind::Data);
        assert_eq!(error.code(), Some("manifest_invalid"));
    }
    assert!(!base.path().join("a").join("planted").exists());
}

#[tokio::test]
async fn a_table_name_that_is_no_identifier_is_refused() {
    let base = crate::fixtures::tempdir().unwrap();
    let root = base.path().join("root");
    let destination = connect(&root, "jsonl").await;
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, _) = ids(&[1]);
    for name in bad_names() {
        let named = TableRef {
            name: name.as_str().into(),
            ..table("t")
        };
        let create = TableChange::Create {
            table: named.clone(),
            schema: schema.clone(),
        };
        let changed = opened.session.apply_schema(&create).await;
        let error = changed.expect_err("the name is refused");
        assert_eq!(error.code(), Some("invalid_name"), "{name:?}");
        let Err(error) = opened.session.writer(&named).await else {
            panic!("a writer opened for {name:?}");
        };
        assert_eq!(error.code(), Some("invalid_name"), "{name:?}");
        assert!(files::published(&root, &name).is_err(), "{name:?}");
    }
    let made: Vec<_> = dirs_under(base.path())
        .into_iter()
        .filter(|path| !path.starts_with(root.join("_rdlt")) && *path != root)
        .collect();
    assert!(made.is_empty(), "{made:?}");
    assert!(dirs_under(&root.join("_rdlt").join("tables")).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_version_at_the_end_of_its_range_is_refused() {
    let root = crate::fixtures::tempdir().unwrap();
    let destination = connect(root.path(), "jsonl").await;
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = ids(&[1]);
    stage(&mut opened, &table("t"), &schema, batch, 1).await;
    let catalog = root.path().join("_rdlt").join("tables").join("t");
    std::fs::copy(
        catalog.join("00000000000000000001.json"),
        catalog.join("18446744073709551615.json"),
    )
    .unwrap();
    for column in ["b", "c"] {
        let add = TableChange::AddColumn {
            table: table("t"),
            field: Field::new(column, LogicalType::Int64, true),
        };
        let changing = opened.session.apply_schema(&add);
        let changed = tokio::time::timeout(Duration::from_secs(20), changing)
            .await
            .expect("the change ends");
        let error = changed.expect_err("no version follows the last");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{column}");
    }
}

#[tokio::test]
async fn a_lock_file_that_is_a_link_is_refused() {
    let base = crate::fixtures::tempdir().unwrap();
    let root = base.path().join("root");
    let destination = connect(&root, "jsonl").await;
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = ids(&[1]);
    stage(&mut opened, &table("held"), &schema, batch, 1).await;
    let locks = root.join("_rdlt").join("locks");
    let marker = base.path().join("marker");
    symlink(&marker, locks.join("orders")).unwrap();
    let Err(error) = opened.session.writer(&table("orders")).await else {
        panic!("a writer opened through a link");
    };
    assert_eq!(error.code(), Some("not_a_regular_file"));
    assert!(!marker.exists(), "the link's target was created");
}

#[tokio::test]
async fn stale_temporaries_are_swept_at_open_and_fresh_ones_left() {
    let root = crate::fixtures::tempdir().unwrap();
    let destination = connect(root.path(), "jsonl").await;
    drop(open(destination.as_ref(), 1).await);
    let manifests = pipeline_dir(root.path()).join("manifests");
    let (stale, fresh) = (
        manifests.join(".tmp-00000000000000000000000000000001"),
        manifests.join(".tmp-00000000000000000000000000000002"),
    );
    for path in [&stale, &fresh] {
        std::fs::write(path, b"{").unwrap();
    }
    let long_ago = SystemTime::now() - Duration::from_hours(24);
    std::fs::File::options()
        .write(true)
        .open(&stale)
        .unwrap()
        .set_modified(long_ago)
        .unwrap();
    drop(open(destination.as_ref(), 2).await);
    assert!(!stale.exists(), "a temporary a crash left stays");
    assert!(fresh.exists(), "a temporary another writer is writing went");
}

#[tokio::test(flavor = "multi_thread")]
async fn control_files_that_are_not_regular_or_too_large_are_refused() {
    let root = crate::fixtures::tempdir().unwrap();
    let destination = connect(root.path(), "jsonl").await;
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = ids(&[1]);
    stage(&mut opened, &table("t"), &schema, batch, 1).await;
    let catalog = root.path().join("_rdlt").join("tables").join("t");
    let bounded = Duration::from_secs(20);
    // An owner file that is a link to an endless device.
    let owner = catalog.join("owner");
    let kept = std::fs::read(&owner).unwrap();
    std::fs::remove_file(&owner).unwrap();
    symlink("/dev/zero", &owner).unwrap();
    let writer = tokio::time::timeout(bounded, opened.session.writer(&table("t")))
        .await
        .expect("the claim ends");
    assert!(writer.is_err(), "an owner was read through a link");
    std::fs::remove_file(&owner).unwrap();
    std::fs::write(&owner, kept).unwrap();
    // A catalog version larger than any catalog.
    let huge = catalog.join("00000000000000000002.json");
    std::fs::File::create(&huge)
        .unwrap()
        .set_len(1 << 40)
        .unwrap();
    let read = files::published(root.path(), "t").expect_err("the catalog is too large");
    assert_eq!(read.code(), Some("limit_exceeded"));
    std::fs::remove_file(&huge).unwrap();
    // A manifest that is a named pipe nobody writes.
    let (latest, _) = latest_manifest(root.path());
    let pipe = latest.with_file_name("00000000000000000009.json");
    let made = std::process::Command::new("mkfifo")
        .arg(&pipe)
        .status()
        .expect("mkfifo runs");
    assert!(made.success());
    let context = crate::fixtures::context(2);
    let opened = tokio::time::timeout(bounded, destination.open(&context))
        .await
        .expect("the open ends");
    assert!(opened.is_err(), "a pipe was read as a manifest");
}

#[tokio::test]
async fn everything_the_destination_creates_is_private() {
    let base = crate::fixtures::tempdir().unwrap();
    let root = base.path().join("made").join("root");
    let destination = connect(&root, "arrow").await;
    destination.check().await.unwrap();
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = keyed(&[1, 2], 1);
    stage(&mut opened, &merge_table("merged"), &schema, batch, 1).await;
    let (schema, batch) = ids(&[1]);
    stage(&mut opened, &table("rows"), &schema, batch, 2).await;
    opened
        .session
        .commit(&meta(&opened, 1, CommitSeq::FIRST, &[1, 2]))
        .await
        .unwrap();
    let mode = |path: &Path| {
        std::fs::symlink_metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777
    };
    // What is no pipeline's directory among them is not read as one.
    std::fs::write(root.join("_rdlt").join("pipelines").join("notes"), b"x").unwrap();
    std::fs::set_permissions(
        root.join("_rdlt").join("pipelines").join("notes"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let published = files::published(&root, "rows").expect("the table reads back");
    assert_eq!(published.len(), 1);
    let dirs = dirs_under(base.path());
    assert!(dirs.len() > 8, "{dirs:?}");
    for dir in dirs {
        assert_eq!(mode(&dir), 0o700, "{dir:?}");
    }
    let files = files_under(base.path());
    assert!(files.len() > 5, "{files:?}");
    for file in files {
        assert_eq!(mode(&file), 0o600, "{file:?}");
    }
}

#[tokio::test]
async fn a_root_or_a_directory_beneath_it_that_others_may_write_is_refused() {
    // Only another user's directory can show the owner's half; a directory others may write is
    // refused as one others made, on the root and on every directory entered beneath it.
    let base = crate::fixtures::tempdir().unwrap();
    let root = base.path().join("root");
    let destination = connect(&root, "jsonl").await;
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = ids(&[1]);
    stage(&mut opened, &table("rows"), &schema, batch, 1).await;
    opened
        .session
        .commit(&meta(&opened, 1, CommitSeq::FIRST, &[1]))
        .await
        .unwrap();
    let mode = |path: &Path, mode| {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    };
    let rdlt = root.join("_rdlt");
    for shared in [
        root.clone(),
        rdlt.clone(),
        rdlt.join("pipelines"),
        pipeline_dir(&root),
        pipeline_dir(&root).join("manifests"),
    ] {
        for bits in [0o777, 0o1777, 0o770] {
            mode(&shared, bits);
            let Err(error) = destination.open(&crate::fixtures::context(2)).await else {
                panic!("{} with mode {bits:o} was opened", shared.display());
            };
            assert_eq!(error.kind(), ConnectorErrorKind::Config, "{shared:?}");
            assert_eq!(error.code(), Some("not_private"), "{shared:?}");
            let read = files::published(&root, "rows").expect_err("the reader refuses it too");
            assert_eq!(read.code(), Some("not_private"), "{shared:?}");
            mode(&shared, 0o700);
        }
    }
    // The catalog and the locks, which a writer and a reader enter.
    let tables = rdlt.join("tables");
    for shared in [rdlt.join("locks"), tables.clone(), tables.join("rows")] {
        mode(&shared, 0o777);
        let Err(error) = opened.session.writer(&table("rows")).await else {
            panic!(
                "{} was entered though others may write it",
                shared.display()
            );
        };
        assert_eq!(error.code(), Some("not_private"), "{shared:?}");
        if shared.starts_with(&tables) {
            let read = files::published(&root, "rows").expect_err("the reader refuses it too");
            assert_eq!(read.code(), Some("not_private"), "{shared:?}");
        }
        mode(&shared, 0o700);
    }
    mode(&rdlt, 0o777);
    let error = destination.check().await.expect_err("the check refuses it");
    assert_eq!(error.code(), Some("not_private"));
    mode(&rdlt, 0o755);
    destination
        .check()
        .await
        .expect("a directory others only read is the user's alone");
}
