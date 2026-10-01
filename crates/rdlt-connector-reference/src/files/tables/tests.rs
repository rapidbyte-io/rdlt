use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::time::{Duration, Instant};

use rdlt_connector::{ConnectorErrorKind, Field, LogicalType, PipelineId, TableSchema};

use super::{LOCK_TIMEOUT, claim, empty_trash, locked, named, owner, read, release, update};
use crate::limits::{CATALOG_BYTES, KEPT_VERSIONS, OWNER_BYTES, TABLE_NAME_BYTES};
use crate::rooted::Dir;

/// How long these tests wait for a lock nobody holds for long.
const WAIT: Duration = Duration::from_secs(20);

/// A destination's private directory.
fn private() -> (tempfile::TempDir, Dir) {
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    (root, dir)
}

/// `current` with a nullable Int64 column `name` added.
fn with(current: Option<&TableSchema>, name: &str) -> TableSchema {
    let mut fields: Vec<Field> = current
        .map(|schema| schema.fields().iter().cloned().collect())
        .unwrap_or_default();
    fields.push(Field::new(name, LogicalType::Int64, true));
    TableSchema::new(fields).unwrap()
}

fn names(rdlt: &Dir) -> Vec<String> {
    read(rdlt, "t")
        .unwrap()
        .unwrap()
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect()
}

fn pipeline(name: &str) -> PipelineId {
    PipelineId::parse(name).unwrap()
}

#[test]
fn a_table_name_is_an_identifier_of_ascii_words() {
    let longest = "x".repeat(usize::from(TABLE_NAME_BYTES));
    for name in ["t", "T_9", "_", "0", longest.as_str()] {
        named(name).expect(name);
    }
    let longer = "x".repeat(usize::from(TABLE_NAME_BYTES) + 1);
    for name in [
        "",
        ".",
        "..",
        "a/b",
        "a.b",
        "a-b",
        "a b",
        "ä",
        "a\0",
        longer.as_str(),
    ] {
        let error = named(name).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{name:?}");
        assert_eq!(error.code(), Some("invalid_name"), "{name:?}");
    }
    // Every entry point refuses such a name before it touches the disk.
    let (root, rdlt) = private();
    let bad = "../t";
    assert!(read(&rdlt, bad).is_err());
    assert!(update(&rdlt, bad, |current| Ok(Some(with(current, "a")))).is_err());
    assert!(claim(&rdlt, bad, &pipeline("a")).is_err());
    assert!(owner(&rdlt, bad).is_err());
    assert!(locked(&rdlt, bad, WAIT, || Ok(())).is_err());
    assert!(release(&rdlt, bad, &pipeline("a"), WAIT, || Ok(true)).is_err());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn a_change_made_while_another_is_worked_out_is_never_lost() {
    let (_root, rdlt) = private();
    let mut interleaved = false;
    update(&rdlt, "t", |current| {
        if !interleaved {
            interleaved = true;
            update(&rdlt, "t", |current| Ok(Some(with(current, "a")))).unwrap();
        }
        Ok(Some(with(current, "b")))
    })
    .unwrap();
    assert_eq!(names(&rdlt), ["a", "b"]);
}

#[test]
fn a_change_that_other_changes_always_overtake_ends_as_a_transient_error() {
    let (_root, rdlt) = private();
    let mut tries = 0;
    let changed = update(&rdlt, "t", |current| {
        tries += 1;
        update(&rdlt, "t", |current| {
            Ok(Some(with(current, &format!("c{tries}"))))
        })
        .unwrap();
        Ok(Some(with(current, "late")))
    });
    assert_eq!(changed.unwrap_err().kind(), ConnectorErrorKind::Transient);
    assert_eq!(tries, crate::limits::PUBLISH_ATTEMPTS);
}

#[test]
fn a_change_that_changes_nothing_writes_nothing() {
    let (_root, rdlt) = private();
    assert_eq!(read(&rdlt, "t").unwrap(), None);
    update(&rdlt, "t", |current| Ok(Some(with(current, "a")))).unwrap();
    update(&rdlt, "t", |_| Ok(None)).unwrap();
    assert_eq!(names(&rdlt), ["a"]);
}

#[test]
fn superseded_catalog_versions_are_removed_but_the_most_recent() {
    let (root, rdlt) = private();
    for column in 0..20 {
        update(&rdlt, "t", |current| {
            Ok(Some(with(current, &format!("c{column}"))))
        })
        .unwrap();
    }
    let catalog = root.path().join("tables").join("t");
    let mut kept: Vec<String> = std::fs::read_dir(&catalog)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    kept.sort();
    let expected: Vec<String> = (20 - KEPT_VERSIONS..=20)
        .map(|version| format!("{version:020}.json"))
        .collect();
    assert_eq!(kept, expected);
    assert_eq!(names(&rdlt).len(), 20);
}

#[test]
fn a_catalog_version_at_the_end_of_its_range_is_followed_by_none() {
    let (root, rdlt) = private();
    update(&rdlt, "t", |current| Ok(Some(with(current, "a")))).unwrap();
    let catalog = root.path().join("tables").join("t");
    std::fs::rename(
        catalog.join("00000000000000000001.json"),
        catalog.join(format!("{}.json", u64::MAX)),
    )
    .unwrap();
    let changed = update(&rdlt, "t", |current| Ok(Some(with(current, "b"))));
    assert_eq!(changed.unwrap_err().kind(), ConnectorErrorKind::Data);
    assert_eq!(names(&rdlt), ["a"]);
    assert_eq!(std::fs::read_dir(&catalog).unwrap().count(), 1);
}

#[test]
fn a_catalog_version_beyond_the_limit_is_neither_written_nor_read() {
    let (root, rdlt) = private();
    let wide = "x".repeat(usize::try_from(CATALOG_BYTES).unwrap());
    let changed = update(&rdlt, "t", |current| Ok(Some(with(current, &wide))));
    let error = changed.unwrap_err();
    assert_eq!(error.limit().map(|limit| limit.name), Some("catalog bytes"));
    let catalog = root.path().join("tables").join("t");
    assert_eq!(std::fs::read_dir(&catalog).unwrap().count(), 0);
    let huge = std::fs::File::create(catalog.join("00000000000000000001.json")).unwrap();
    huge.set_len(CATALOG_BYTES + 1).unwrap();
    let error = read(&rdlt, "t").unwrap_err();
    assert_eq!(
        error.limit().map(|limit| limit.actual),
        Some(CATALOG_BYTES + 1)
    );
}

#[test]
fn a_catalog_that_cannot_be_listed_is_an_error_not_a_missing_table() {
    let (root, rdlt) = private();
    update(&rdlt, "t", |current| Ok(Some(with(current, "a")))).unwrap();
    let catalog = root.path().join("tables").join("t");
    let mode = |mode| std::fs::set_permissions(&catalog, std::fs::Permissions::from_mode(mode));
    mode(0o000).unwrap();
    // Where permissions bind nothing, as for root, the fault cannot be made.
    let listable = std::fs::read_dir(&catalog).is_ok();
    let read = read(&rdlt, "t");
    mode(0o755).unwrap();
    if !listable {
        read.expect_err("the catalog cannot be listed");
    }
}

#[test]
fn a_released_catalog_is_gone_and_any_pipeline_may_create_the_table_again() {
    let (root, rdlt) = private();
    let dropped = || Ok(true);
    claim(&rdlt, "t", &pipeline("a")).unwrap();
    update(&rdlt, "t", |current| Ok(Some(with(current, "x")))).unwrap();
    release(&rdlt, "t", &pipeline("a"), WAIT, dropped).unwrap();
    assert_eq!(read(&rdlt, "t").unwrap(), None);
    assert_eq!(owner(&rdlt, "t").unwrap(), None);
    assert_eq!(
        std::fs::read_dir(root.path().join("trash"))
            .unwrap()
            .count(),
        0
    );
    claim(&rdlt, "t", &pipeline("b")).unwrap();
    assert_eq!(owner(&rdlt, "t").unwrap().as_deref(), Some("b"));
    // Releasing what is not there, or what another pipeline now owns, changes nothing.
    release(&rdlt, "u", &pipeline("a"), WAIT, dropped).unwrap();
    release(&rdlt, "t", &pipeline("a"), WAIT, dropped).unwrap();
    assert_eq!(owner(&rdlt, "t").unwrap().as_deref(), Some("b"));
    let error = claim(&rdlt, "t", &pipeline("a")).unwrap_err();
    assert_eq!(error.code(), Some("table_owned"));
}

#[test]
fn a_release_removes_nothing_once_the_table_is_no_longer_dropped() {
    let (_root, rdlt) = private();
    claim(&rdlt, "t", &pipeline("a")).unwrap();
    update(&rdlt, "t", |current| Ok(Some(with(current, "x")))).unwrap();
    // The session was overtaken: a newer one removed the catalog and created the table again.
    release(&rdlt, "t", &pipeline("a"), WAIT, || Ok(false)).unwrap();
    assert_eq!(owner(&rdlt, "t").unwrap().as_deref(), Some("a"));
    assert_eq!(names(&rdlt), ["x"]);
    // What cannot be told is not removed either.
    let unknown = || Err(rdlt_connector::ConnectorError::data("no manifest reads"));
    assert!(release(&rdlt, "t", &pipeline("a"), WAIT, unknown).is_err());
    assert_eq!(names(&rdlt), ["x"]);
}

#[test]
fn an_owner_that_cannot_be_read_is_an_error_not_a_missing_owner() {
    let (root, rdlt) = private();
    // A directory where the owner file belongs reads as neither an owner nor none.
    let owner_path = root.path().join("tables").join("t").join("owner");
    std::fs::create_dir_all(&owner_path).unwrap();
    assert!(owner(&rdlt, "t").is_err());
    assert!(claim(&rdlt, "t", &pipeline("a")).is_err());
    assert!(release(&rdlt, "t", &pipeline("a"), WAIT, || Ok(true)).is_err());
    // So does a link, an owner longer than any pipeline's name, and one that is no text.
    std::fs::remove_dir(&owner_path).unwrap();
    symlink("/dev/zero", &owner_path).unwrap();
    assert_eq!(
        owner(&rdlt, "t").unwrap_err().code(),
        Some("not_a_regular_file")
    );
    std::fs::remove_file(&owner_path).unwrap();
    let long = "x".repeat(usize::try_from(OWNER_BYTES).unwrap() + 1);
    std::fs::write(&owner_path, &long).unwrap();
    assert_eq!(
        owner(&rdlt, "t").unwrap_err().code(),
        Some("limit_exceeded")
    );
    std::fs::write(&owner_path, &long[1..]).unwrap();
    assert_eq!(
        owner(&rdlt, "t").unwrap().map(|owner| owner.len()),
        Some(long.len() - 1)
    );
    std::fs::write(&owner_path, [0xff, 0xfe]).unwrap();
    let error = owner(&rdlt, "t").unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some(super::CATALOG_INVALID));
}

#[test]
fn a_catalog_that_cannot_be_moved_away_is_not_released() {
    let (root, rdlt) = private();
    claim(&rdlt, "t", &pipeline("a")).unwrap();
    locked(&rdlt, "t", WAIT, || Ok(())).unwrap();
    let tables = root.path().join("tables");
    std::fs::set_permissions(&tables, std::fs::Permissions::from_mode(0o555)).unwrap();
    let released = release(&rdlt, "t", &pipeline("a"), WAIT, || Ok(true));
    std::fs::set_permissions(&tables, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(released.is_err(), "{released:?}");
    assert_eq!(owner(&rdlt, "t").unwrap().as_deref(), Some("a"));
}

#[test]
fn a_release_waits_for_a_claim_under_way() {
    let (root, rdlt) = private();
    claim(&rdlt, "t", &pipeline("a")).unwrap();
    let (released, heard) = std::sync::mpsc::channel();
    let releasing = locked(&rdlt, "t", WAIT, || {
        let path = root.path().to_owned();
        let releasing = std::thread::spawn(move || {
            let rdlt = Dir::ambient(&path).unwrap();
            release(&rdlt, "t", &pipeline("a"), WAIT, || Ok(true)).unwrap();
            released.send(()).unwrap();
        });
        let waited = heard.recv_timeout(Duration::from_millis(200));
        assert!(
            waited.is_err(),
            "the release ran while a claim held the table"
        );
        assert_eq!(owner(&rdlt, "t").unwrap().as_deref(), Some("a"));
        Ok(releasing)
    })
    .unwrap();
    releasing.join().unwrap();
    heard
        .recv()
        .expect("the release ran once the claim was done");
    assert_eq!(owner(&rdlt, "t").unwrap(), None);
}

#[test]
fn a_lock_another_holds_is_waited_for_no_longer_than_its_wait() {
    let (root, rdlt) = private();
    locked(&rdlt, "orders", WAIT, || Ok(())).unwrap();
    let path = root.path().join("locks").join("orders");
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    // Another holder of the lock, through a descriptor of its own that only reads.
    let holder = std::fs::File::open(&path).unwrap();
    holder.lock().unwrap();
    for wait in [Duration::ZERO, Duration::from_millis(150)] {
        let started = Instant::now();
        let error = locked(&rdlt, "orders", wait, || Ok(())).unwrap_err();
        let waited = started.elapsed();
        assert_eq!(error.kind(), ConnectorErrorKind::Transient);
        assert_eq!(error.code(), Some(LOCK_TIMEOUT));
        assert!(
            waited >= wait && waited < wait + Duration::from_secs(5),
            "{waited:?}"
        );
    }
    // A lock freed while it is waited for is taken.
    let freeing = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(holder);
    });
    locked(&rdlt, "orders", WAIT, || Ok(())).unwrap();
    freeing.join().unwrap();
    // The work's own outcome is the call's.
    let failed = locked(&rdlt, "orders", WAIT, || {
        Err::<(), _>(rdlt_connector::ConnectorError::data("the work failed"))
    });
    assert_eq!(failed.unwrap_err().kind(), ConnectorErrorKind::Data);
    assert_eq!(locked(&rdlt, "orders", WAIT, || Ok(7)).unwrap(), 7);
}

#[test]
fn a_lock_file_is_this_user_s_regular_file_or_refused() {
    let (root, rdlt) = private();
    let base = tempfile::tempdir().unwrap();
    locked(&rdlt, "made", WAIT, || Ok(())).unwrap();
    let locks = root.path().join("locks");
    // A link, to a file or to nothing, is not opened and its target not created.
    std::fs::write(base.path().join("target"), b"").unwrap();
    symlink(base.path().join("target"), locks.join("linked")).unwrap();
    symlink(base.path().join("marker"), locks.join("dangling")).unwrap();
    for name in ["linked", "dangling"] {
        let error = locked(&rdlt, name, WAIT, || Ok(())).unwrap_err();
        assert_eq!(error.code(), Some("not_a_regular_file"), "{name}");
    }
    assert!(!base.path().join("marker").exists());
    // A lock file others may write is not this user's alone.
    std::fs::write(locks.join("shared"), b"").unwrap();
    std::fs::set_permissions(locks.join("shared"), std::fs::Permissions::from_mode(0o666)).unwrap();
    let error = locked(&rdlt, "shared", WAIT, || Ok(())).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
}

#[test]
fn catalogs_a_release_left_renamed_out_of_place_are_removed() {
    let (root, rdlt) = private();
    empty_trash(&rdlt).unwrap();
    let trash = root.path().join("trash");
    std::fs::create_dir_all(trash.join("left").join("deeper")).unwrap();
    std::fs::write(trash.join("left").join("owner"), b"a").unwrap();
    std::fs::write(trash.join("file"), b"a").unwrap();
    empty_trash(&rdlt).unwrap();
    assert_eq!(std::fs::read_dir(&trash).unwrap().count(), 0);
}

#[test]
fn a_catalog_version_that_is_no_schema_is_a_data_error_no_retry_reads_differently() {
    let (root, rdlt) = private();
    update(&rdlt, "t", |current| Ok(Some(with(current, "a")))).unwrap();
    let catalog = root.path().join("tables").join("t");
    for junk in [&b"not json"[..], b"{", b"[[[[[[[[", b"", b"\xff\xfe"] {
        std::fs::write(catalog.join("00000000000000000002.json"), junk).unwrap();
        let error = read(&rdlt, "t").unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{junk:?}");
        assert_eq!(error.code(), Some(super::CATALOG_INVALID), "{junk:?}");
    }
}
