use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::{LoadId, PipelineId};

use super::dir::{Refusal, SYNCED, owned};
use super::{LocalWal, names};
use crate::conformance;
use crate::wal::{Chunk, WalStore};

fn pipeline(name: &str) -> PipelineId {
    PipelineId::parse(name).expect("a valid pipeline")
}

fn chunk(load: u128, number: u64) -> Chunk {
    Chunk {
        load: LoadId::from_parts(UNIX_EPOCH, load),
        number,
    }
}

fn mode(path: &Path) -> u32 {
    std::fs::symlink_metadata(path)
        .expect("it exists")
        .permissions()
        .mode()
        & 0o777
}

fn set_mode(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmods");
}

/// The refusal `error` carries.
fn refusal(error: &std::io::Error) -> Option<&Refusal> {
    error.get_ref().and_then(|inner| inner.downcast_ref())
}

fn not_private(error: &std::io::Error) -> bool {
    matches!(refusal(error), Some(Refusal::NotPrivate { .. }))
}

fn stray(error: &std::io::Error) -> bool {
    matches!(refusal(error), Some(Refusal::Stray { .. }))
}

/// Opens `load`'s log of `pipeline` in `wal` where it was not opened yet.
async fn opened(wal: &LocalWal, pipeline: &PipelineId, load: LoadId) {
    match wal.open_log(pipeline, load).await {
        Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => {
            panic!("the log opens: {error}")
        }
        _ => {}
    }
}

/// Stages `bytes` as `chunk` of `pipeline`'s log in `wal` and publishes it.
async fn published(wal: &LocalWal, pipeline: &PipelineId, chunk: Chunk, bytes: &'static [u8]) {
    opened(wal, pipeline, chunk.load).await;
    let mut staged = wal.stage(pipeline, chunk).await.expect("stages");
    staged
        .append(Bytes::from_static(bytes))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
}

#[tokio::test]
async fn a_local_log_keeps_the_store_s_contract() {
    let base = tempfile::tempdir().expect("a temporary directory");
    conformance::conforms(&LocalWal::new(base.path())).await;
}

proptest! {
    /// Two pipelines share a directory only where they are one, whatever their names fold to.
    #[test]
    fn a_pipeline_s_directory_is_its_own_whatever_its_name_folds_to(
        a in "[A-Za-z0-9._-]{1,128}",
        b in "[a-zA-Z._]{1,128}",
    ) {
        let (a, b) = (pipeline(&a), pipeline(&b));
        let (named_a, named_b) = (names::pipeline(&a), names::pipeline(&b));
        prop_assert!(named_a.len() <= 255, "{named_a}");
        prop_assert_eq!(a == b, named_a == named_b);
        // A file system that folds case tells them apart too.
        prop_assert_eq!(a == b, named_a.to_lowercase() == named_b.to_lowercase());
        prop_assert!(named_a != "." && named_a != "..");
    }
}

#[test]
fn pipelines_whose_names_fold_alike_keep_directories_of_their_own() {
    let wal = LocalWal::new("/base");
    let folded = [
        "orders.eu",
        "orders_eu",
        "Orders.eu",
        "ORDERS.EU",
        // Two names a 32-bit hash of the sanitized name put in one directory.
        "tenant_.__._____.___________orders",
        "tenant.__..._.____.._.______orders",
    ];
    let dirs: Vec<_> = folded
        .iter()
        .map(|name| wal.pipeline_dir(&pipeline(name)))
        .collect();
    for (index, dir) in dirs.iter().enumerate() {
        assert!(dir.starts_with("/base"), "{}", dir.display());
        for other in &dirs[index + 1..] {
            let (dir, other) = (dir.to_string_lossy(), other.to_string_lossy());
            assert_ne!(dir.to_lowercase(), other.to_lowercase());
        }
    }
}

#[test]
fn a_pipeline_s_directory_reads_as_its_name_where_it_has_no_capital() {
    assert_eq!(
        names::pipeline(&pipeline("orders-eu_1.v2")),
        "p.orders-eu_1.v2"
    );
    assert_eq!(names::pipeline(&pipeline("..")), "p...");
    // RFC 4648's base32, lower-cased and unpadded, ending in each part of a group of five
    // bytes and in a whole one.
    for (id, encoded) in [
        ("Q", "x.ke"),
        ("QQ", "x.kfiq"),
        ("QQQ", "x.kfivc"),
        ("QQQQ", "x.kfivcui"),
        ("QQQQQ", "x.kfivcukr"),
        ("QQQQQQ", "x.kfivcukrke"),
        ("EU-orders", "x.ivks233smrsxe4y"),
    ] {
        assert_eq!(names::pipeline(&pipeline(id)), encoded, "{id}");
    }
}

#[test]
fn only_the_names_the_store_writes_are_read_as_loads_and_chunks() {
    // A load whose id has letters, which may be written in upper case.
    let load = chunk(0xdead_beef, 0).load;
    let named = names::load(load);
    assert_eq!(names::parse_load(named.as_ref()), Some(load));
    assert_eq!(names::parse_load(named.to_uppercase().as_ref()), None);
    assert_eq!(names::parse_load(format!("{{{named}}}").as_ref()), None);
    assert_eq!(names::parse_load(named.replace('-', "").as_ref()), None);
    for (number, token) in [(0, 0), (12, u64::MAX), (u64::MAX, 7)] {
        assert!(names::is_part(names::part(number, token).as_ref()));
    }
    for stray in [
        "00000000.wal",
        "00000000.part",
        "0.0000000000000000.part",
        "00000000.000000000000000A.part",
        "00000000.00000000000000000.part",
        "00000000.000000000000000g.part",
    ] {
        assert!(!names::is_part(stray.as_ref()), "{stray}");
    }
    for number in [0, 7, 99_999_999, 100_000_000, u64::MAX] {
        assert_eq!(
            names::parse_chunk(names::chunk(number).as_ref()),
            Some(number)
        );
    }
    for alias in [
        "0.wal",
        "00.wal",
        "+0000000.wal",
        "0000000a.wal",
        "00000000",
        ".wal",
    ] {
        assert_eq!(names::parse_chunk(alias.as_ref()), None, "{alias}");
    }
}

#[tokio::test]
async fn files_and_directories_a_log_creates_are_its_owner_s_alone() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path().join("wal"));
    let orders = pipeline("orders");
    published(&wal, &orders, chunk(1, 0), b"x").await;
    let mut staged = wal.stage(&orders, chunk(1, 1)).await.expect("stages");
    staged
        .append(Bytes::from_static(b"y"))
        .await
        .expect("appends");
    let dir = wal.pipeline_dir(&orders);
    let load = dir.join(names::load(chunk(1, 0).load));
    for dir in [base.path().join("wal"), dir.clone(), load.clone()] {
        assert_eq!(mode(&dir), 0o700, "{}", dir.display());
    }
    let files: Vec<_> = std::fs::read_dir(&load)
        .expect("lists")
        .map(|entry| entry.expect("an entry").path())
        .collect();
    assert_eq!(
        files.len(),
        3,
        "the mark of an open log, a chunk and a staged one"
    );
    for file in files {
        assert_eq!(mode(&file), 0o600, "{}", file.display());
    }
}

#[tokio::test]
async fn a_link_where_a_log_s_directory_or_file_belongs_is_refused() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let (x, y) = (pipeline("x"), pipeline("y"));
    let load = chunk(7, 0).load;
    published(&wal, &y, chunk(7, 0), b"Y-frames").await;
    // Another pipeline's directory planted where this one's belongs.
    std::os::unix::fs::symlink(wal.pipeline_dir(&y), wal.pipeline_dir(&x)).expect("links");
    assert!(not_private(&wal.loads(&x).await.expect_err("refused")));
    assert!(not_private(
        &wal.chunks(&x, load).await.expect_err("refused")
    ));
    let refused = wal.read(&x, chunk(7, 0), 0, 8).await.expect_err("refused");
    assert!(not_private(&refused));
    let refused = wal.stage(&x, chunk(7, 1)).await.err().expect("refused");
    assert!(not_private(&refused));
    let refused = wal.remove(&x, chunk(7, 0)).await.expect_err("refused");
    assert!(not_private(&refused));
    let refused = wal.remove_log(&x, load).await.expect_err("refused");
    assert!(not_private(&refused));
    assert_eq!(
        wal.loads(&y).await.expect("loads list"),
        [load],
        "Y's log stays"
    );
    // A load's directory, and a chunk, that are links.
    std::fs::remove_file(wal.pipeline_dir(&x)).expect("removes");
    published(&wal, &x, chunk(8, 0), b"X").await;
    let x_dir = wal.pipeline_dir(&x);
    let y_load = wal.pipeline_dir(&y).join(names::load(load));
    std::os::unix::fs::symlink(&y_load, x_dir.join(names::load(load))).expect("links");
    assert!(not_private(&wal.loads(&x).await.expect_err("refused")));
    std::fs::remove_file(x_dir.join(names::load(load))).expect("removes");
    let x_load = x_dir.join(names::load(chunk(8, 0).load));
    std::os::unix::fs::symlink(y_load.join("00000000.wal"), x_load.join("00000001.wal"))
        .expect("links");
    let refused = wal.chunks(&x, chunk(8, 0).load).await.expect_err("refused");
    assert!(not_private(&refused), "{refused}");
    let refused = wal.read(&x, chunk(8, 1), 0, 8).await.expect_err("refused");
    assert!(not_private(&refused), "{refused}");
}

#[tokio::test]
async fn a_log_s_directory_or_file_others_may_reach_is_refused() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    published(&wal, &orders, chunk(1, 0), b"frame").await;
    let dir = wal.pipeline_dir(&orders);
    let load_dir = dir.join(names::load(load));
    let file = load_dir.join("00000000.wal");
    for (path, private) in [(&dir, 0o700), (&load_dir, 0o700), (&file, 0o600)] {
        for reach in [0o004, 0o040, 0o001, 0o010, 0o002, 0o020] {
            set_mode(path, private | reach);
            let refused = wal
                .read(&orders, chunk(1, 0), 0, 5)
                .await
                .expect_err("refused");
            assert!(
                not_private(&refused),
                "{} {reach:o}: {refused}",
                path.display()
            );
            set_mode(path, private);
        }
    }
    assert_eq!(wal.chunks(&orders, load).await.expect("lists"), [(0, 5)]);
    set_mode(&file, 0o640);
    let refused = wal.chunks(&orders, load).await.expect_err("refused");
    assert!(not_private(&refused), "{refused}");
    set_mode(&file, 0o600);
    // The base may be read by others, never written.
    set_mode(base.path(), 0o755);
    wal.read(&orders, chunk(1, 0), 0, 5).await.expect("reads");
    for reach in [0o020, 0o002] {
        set_mode(base.path(), 0o755 | reach);
        let refused = wal.loads(&orders).await.expect_err("refused");
        assert!(not_private(&refused), "{reach:o}: {refused}");
    }
    set_mode(base.path(), 0o700);
}

#[test]
fn what_another_user_owns_is_refused_whatever_its_mode() {
    let me = rustix::process::geteuid().as_raw();
    assert!(owned(me, 0o700, 0o077, Path::new("/x")).is_ok());
    assert!(owned(me, 0o755, 0o022, Path::new("/x")).is_ok());
    let error = owned(me.wrapping_add(1), 0o700, 0o077, Path::new("/x")).expect_err("refused");
    assert!(not_private(&error), "{error}");
    let error = owned(me, 0o710, 0o077, Path::new("/x")).expect_err("refused");
    assert!(not_private(&error), "{error}");
}

#[tokio::test]
async fn a_name_the_store_never_writes_is_refused_not_read() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    published(&wal, &orders, chunk(1, 0), b"frame").await;
    let dir = wal.pipeline_dir(&orders);
    let load_dir = dir.join(names::load(load));
    // A second name of chunk 0, which a listing that parsed numbers read twice.
    std::fs::hard_link(load_dir.join("00000000.wal"), load_dir.join("0.wal")).expect("links");
    let refused = wal.chunks(&orders, load).await.expect_err("refused");
    assert!(stray(&refused), "{refused}");
    // A removal closes the log first, then refuses what it cannot remove: the log is left
    // closed, for a later removal.
    assert!(stray(
        &wal.remove_log(&orders, load).await.expect_err("refused")
    ));
    assert_eq!(wal.loads(&orders).await.expect("lists"), []);
    assert_eq!(wal.leftovers(&orders).await.expect("lists"), [load]);
    std::fs::remove_file(load_dir.join("0.wal")).expect("removes");
    std::fs::write(load_dir.join("open"), b"").expect("opens again by hand");
    set_mode(&load_dir.join("open"), 0o600);
    // A directory where a chunk belongs.
    std::fs::create_dir(load_dir.join("00000001.wal")).expect("creates");
    assert!(not_private(
        &wal.chunks(&orders, load).await.expect_err("refused")
    ));
    assert!(not_private(
        &wal.remove(&orders, chunk(1, 1)).await.expect_err("refused")
    ));
    std::fs::remove_dir(load_dir.join("00000001.wal")).expect("removes");
    // Names beside the loads.
    let capitals = names::load(chunk(0xdead_beef, 0).load).to_uppercase();
    for name in ["notes.txt", "00000000.wal", &capitals] {
        std::fs::write(dir.join(name), b"").expect("writes");
        set_mode(&dir.join(name), 0o600);
        assert!(
            stray(&wal.loads(&orders).await.expect_err("refused")),
            "{name}"
        );
        std::fs::remove_file(dir.join(name)).expect("removes");
    }
    // A load's name that is a file.
    std::fs::write(dir.join(names::load(chunk(2, 0).load)), b"").expect("writes");
    assert!(not_private(&wal.loads(&orders).await.expect_err("refused")));
    std::fs::remove_file(dir.join(names::load(chunk(2, 0).load))).expect("removes");
    assert_eq!(wal.loads(&orders).await.expect("lists"), [load]);
}

#[tokio::test]
async fn a_load_that_only_staged_is_listed_so_replay_removes_it() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(3, 0).load;
    opened(&wal, &orders, load).await;
    let mut staged = wal.stage(&orders, chunk(3, 0)).await.expect("stages");
    staged
        .append(Bytes::from_static(b"x"))
        .await
        .expect("appends");
    drop(staged);
    assert_eq!(wal.loads(&orders).await.expect("loads list"), [load]);
    assert_eq!(wal.chunks(&orders, load).await.expect("chunks"), []);
    wal.remove_log(&orders, load).await.expect("removes");
    assert_eq!(wal.loads(&orders).await.expect("loads list"), []);
    let left = std::fs::read_dir(wal.pipeline_dir(&orders))
        .expect("the pipeline's directory stays")
        .count();
    assert_eq!(left, 0, "nothing of the log is left");
}

#[tokio::test]
async fn a_staging_that_empties_a_removed_log_s_directory_removes_it() {
    // A removal that closed and listed the log before a staging's file landed, or before its
    // publish linked it, finds the directory full and leaves it: the publish refused, or the
    // discard, that empties the directory after it removes it.
    for publish in [true, false] {
        let base = tempfile::tempdir().expect("a temporary directory");
        let wal = LocalWal::new(base.path());
        let orders = pipeline("orders");
        let load = chunk(1, 0).load;
        opened(&wal, &orders, load).await;
        let mut staged = wal.stage(&orders, chunk(1, 1)).await.expect("stages");
        staged
            .append(Bytes::from_static(b"late"))
            .await
            .expect("appends");
        let dir = wal.pipeline_dir(&orders);
        std::fs::remove_file(dir.join(names::load(load)).join(names::OPEN)).expect("closes");
        assert_eq!(wal.leftovers(&orders).await.expect("lists"), [load]);
        if publish {
            let refused = staged.publish().await.expect_err("the log was removed");
            assert_eq!(refused.kind(), std::io::ErrorKind::NotFound, "{refused}");
        } else {
            staged.discard().await.expect("discards");
        }
        assert_eq!(
            wal.leftovers(&orders).await.expect("lists"),
            [],
            "{publish}"
        );
        let left = std::fs::read_dir(&dir).expect("lists").count();
        assert_eq!(left, 0, "{publish}: nothing of the log is left");
    }
}

#[tokio::test]
async fn a_log_whose_mark_is_gone_or_no_file_is_not_open_to_stage_in() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    opened(&wal, &orders, load).await;
    let mark = wal
        .pipeline_dir(&orders)
        .join(names::load(load))
        .join(names::OPEN);
    // Closed as a removal closes it, its directory still there.
    std::fs::remove_file(&mark).expect("closes");
    let refused = wal.stage(&orders, chunk(1, 0)).await.err().expect("closed");
    assert_eq!(refused.kind(), std::io::ErrorKind::NotFound, "{refused}");
    // A directory, however private, where the mark belongs.
    std::fs::create_dir(&mark).expect("creates");
    set_mode(&mark, 0o700);
    let refused = wal
        .stage(&orders, chunk(1, 0))
        .await
        .err()
        .expect("refused");
    assert!(not_private(&refused), "{refused}");
    let refused = wal.loads(&orders).await.expect_err("refused");
    assert!(not_private(&refused), "{refused}");
}

/// How far a removal running alongside a publish has come when a step of the publish fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Removal {
    NotBegun,
    Closed,
    Done,
}

#[tokio::test]
async fn a_publish_whose_step_fails_once_its_log_is_closed_is_refused_as_removed() {
    // A step a removal meets may fail as the platform has it (macOS answers some `EINVAL`): the
    // publish is refused as `NotFound` where its log is closed, and fails as the step did where
    // the log is open still.
    for removal in [Removal::NotBegun, Removal::Closed, Removal::Done] {
        let base = tempfile::tempdir().expect("a temporary directory");
        let root = base.path().canonicalize().expect("a real path");
        let wal = LocalWal::new(root.join("wal"));
        let orders = pipeline("orders");
        let load = chunk(1, 0).load;
        published(&wal, &orders, chunk(1, 0), b"first").await;
        let mut staged = wal.stage(&orders, chunk(1, 1)).await.expect("stages");
        staged
            .append(Bytes::from_static(b"late"))
            .await
            .expect("appends");
        let load_dir = wal.pipeline_dir(&orders).join(names::load(load));
        let armed = std::sync::atomic::AtomicBool::new(true);
        *super::dir::SYNCING.lock() = Some(Box::new(move |path: &Path| {
            if path != load_dir || !armed.swap(false, std::sync::atomic::Ordering::Relaxed) {
                return Ok(());
            }
            match removal {
                Removal::NotBegun => {}
                Removal::Closed => std::fs::remove_file(path.join(names::OPEN))?,
                Removal::Done => std::fs::remove_dir_all(path)?,
            }
            Err(std::io::Error::from_raw_os_error(
                rustix::io::Errno::INVAL.raw_os_error(),
            ))
        }));
        let failed = staged.publish().await.expect_err("a step fails");
        *super::dir::SYNCING.lock() = None;
        if removal == Removal::NotBegun {
            assert_eq!(failed.kind(), std::io::ErrorKind::InvalidInput, "{failed}");
            continue;
        }
        assert_eq!(
            failed.kind(),
            std::io::ErrorKind::NotFound,
            "{removal:?}: {failed}"
        );
        let left = if removal == Removal::Closed {
            vec![(0, 5)]
        } else {
            Vec::new()
        };
        assert_eq!(wal.chunks(&orders, load).await.expect("lists"), left);
        wal.remove_log(&orders, load).await.expect("removes");
        assert_eq!(wal.leftovers(&orders).await.expect("lists"), []);
        let pipeline_dir = wal.pipeline_dir(&orders);
        let left = std::fs::read_dir(&pipeline_dir).expect("lists").count();
        assert_eq!(left, 0, "{removal:?}: nothing of the log is left");
    }
}

#[tokio::test]
async fn a_removal_whose_directory_cannot_go_but_for_what_fills_it_fails() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let root = base.path().canonicalize().expect("a real path");
    let wal = LocalWal::new(root.join("wal"));
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    published(&wal, &orders, chunk(1, 0), b"first").await;
    // As the removal closes the log, a file takes its directory's name.
    let load_dir = wal.pipeline_dir(&orders).join(names::load(load));
    let armed = std::sync::atomic::AtomicBool::new(true);
    *super::dir::SYNCING.lock() = Some(Box::new(move |path: &Path| {
        if path == load_dir && armed.swap(false, std::sync::atomic::Ordering::Relaxed) {
            std::fs::remove_dir_all(path)?;
            std::fs::write(path, b"")?;
        }
        Ok(())
    }));
    let failed = wal.remove_log(&orders, load).await;
    *super::dir::SYNCING.lock() = None;
    let failed = failed.expect_err("a file is no directory to remove");
    assert_eq!(failed.kind(), std::io::ErrorKind::NotADirectory, "{failed}");
}

/// The directories synced under `base` since `from` syncs were recorded.
fn synced_under(base: &Path, from: usize) -> Vec<std::path::PathBuf> {
    SYNCED.lock()[from..]
        .iter()
        .filter(|path| path.starts_with(base))
        .cloned()
        .collect()
}

#[tokio::test]
async fn a_chunk_s_name_is_durable_once_published_and_its_deletion_once_deleted() {
    let base = tempfile::tempdir().expect("a temporary directory");
    // Where the temporary directory lies behind a link (macOS), syncs are made by its real path.
    let root = base.path().canonicalize().expect("a real path");
    let wal = LocalWal::new(root.join("wal"));
    let orders = pipeline("orders");
    let load_dir = wal
        .pipeline_dir(&orders)
        .join(names::load(chunk(1, 0).load));
    opened(&wal, &orders, chunk(1, 0).load).await;
    let mut staged = wal.stage(&orders, chunk(1, 0)).await.expect("stages");
    staged
        .append(Bytes::from_static(b"frame"))
        .await
        .expect("appends");
    let from = SYNCED.lock().len();
    staged.publish().await.expect("publishes");
    assert_eq!(
        synced_under(root.as_path(), from),
        std::slice::from_ref(&load_dir)
    );
    for number in [1, 2] {
        published(&wal, &orders, chunk(1, number), b"next").await;
    }
    let from = SYNCED.lock().len();
    wal.remove(&orders, chunk(1, 0)).await.expect("removes");
    assert_eq!(
        synced_under(root.as_path(), from),
        std::slice::from_ref(&load_dir)
    );
    // A log is closed durably first, then its files go, then its directory.
    let from = SYNCED.lock().len();
    wal.remove_log(&orders, chunk(1, 0).load)
        .await
        .expect("removes");
    assert_eq!(
        synced_under(root.as_path(), from),
        [load_dir.clone(), load_dir, wal.pipeline_dir(&orders)]
    );
}

#[test]
fn a_base_missing_is_created_private_with_its_parents_each_durable() {
    let base = tempfile::tempdir().expect("a temporary directory");
    // Where the temporary directory lies behind a link (macOS), syncs are made by its real path.
    let root = base.path().canonicalize().expect("a real path");
    let deep = root.join("a").join("b");
    let from = SYNCED.lock().len();
    super::dir::Dir::base(&deep).expect("creates");
    assert_eq!(
        synced_under(root.as_path(), from),
        [root.clone(), root.join("a")]
    );
    assert_eq!(mode(&deep), 0o700);
    let from = SYNCED.lock().len();
    super::dir::Dir::base(&deep).expect("opens");
    assert!(
        synced_under(root.as_path(), from).is_empty(),
        "nothing was created"
    );
}

#[test]
fn a_base_under_a_relative_path_is_created_the_first_time() {
    let base = tempfile::tempdir().expect("a temporary directory");
    // Each test runs in a process of its own, whose working directory this one may move.
    std::env::set_current_dir(base.path()).expect("moves");
    super::dir::Dir::base(Path::new(".rdlt/orders")).expect("creates");
    assert!(base.path().join(".rdlt/orders").is_dir());
}

#[tokio::test]
async fn a_directory_inside_a_load_s_keeps_its_log_from_going() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    published(&wal, &orders, chunk(2, 0), b"frame").await;
    let load_dir = wal
        .pipeline_dir(&orders)
        .join(names::load(chunk(2, 0).load));
    std::fs::create_dir(load_dir.join("inside")).expect("creates");
    let refused = wal
        .remove_log(&orders, chunk(2, 0).load)
        .await
        .expect_err("refused");
    assert!(stray(&refused), "{refused}");
    assert_eq!(
        wal.chunks(&orders, chunk(2, 0).load)
            .await
            .expect_err("refused")
            .kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn a_base_is_resolved_once_and_never_made_again_once_it_is_gone() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let path = base.path().join("wal");
    let wal = LocalWal::new(&path);
    let orders = pipeline("orders");
    published(&wal, &orders, chunk(1, 0), b"frame").await;
    std::fs::remove_dir_all(&path).expect("removes");
    assert!(
        wal.loads(&orders).await.is_err(),
        "the base it opened is gone"
    );
    assert!(
        wal.open_log(&orders, chunk(2, 0).load).await.is_err(),
        "nothing is logged where no reader looks"
    );
    assert!(!path.exists(), "the base is not made again");
}

#[tokio::test]
async fn a_base_another_directory_took_the_place_of_is_refused() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let path = base.path().join("wal");
    let wal = LocalWal::new(&path);
    let orders = pipeline("orders");
    published(&wal, &orders, chunk(1, 0), b"frame").await;
    // The base is moved aside, still whole, and a private directory made where it was.
    std::fs::rename(&path, base.path().join("aside")).expect("moves");
    std::fs::create_dir(&path).expect("creates");
    set_mode(&path, 0o700);
    let refused = wal
        .loads(&orders)
        .await
        .expect_err("not the base it opened");
    assert!(not_private(&refused), "{refused}");
    assert!(
        wal.open_log(&orders, chunk(2, 0).load).await.is_err(),
        "nothing is logged where no reader looks"
    );
    assert_eq!(std::fs::read_dir(&path).expect("lists").count(), 0);
}

#[tokio::test]
async fn a_base_under_a_directory_others_may_write_is_refused_unless_it_is_sticky() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let shared = base.path().join("shared");
    std::fs::create_dir(&shared).expect("creates");
    let orders = pipeline("orders");
    for (mode, refused) in [
        (0o777, true),
        (0o775, true),
        (0o1777, false),
        (0o755, false),
    ] {
        set_mode(&shared, mode);
        let wal = LocalWal::new(shared.join("wal"));
        let listed = wal.loads(&orders).await;
        assert_eq!(listed.is_err(), refused, "{mode:o}: {listed:?}");
        if let Err(error) = listed {
            assert!(not_private(&error), "{mode:o}: {error}");
        }
    }
    // A link to the base that others could replace is refused as well.
    let own = base.path().join("own");
    std::fs::create_dir(&own).expect("creates");
    set_mode(&shared, 0o777);
    std::os::unix::fs::symlink(&own, shared.join("link")).expect("links");
    let wal = LocalWal::new(shared.join("link").join("wal"));
    assert!(not_private(&wal.loads(&orders).await.expect_err("refused")));
    set_mode(&shared, 0o700);
}

#[tokio::test]
async fn a_chunk_is_published_only_from_the_file_its_writer_staged() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    opened(&wal, &orders, load).await;
    let mut staged = wal.stage(&orders, chunk(1, 0)).await.expect("stages");
    staged
        .append(Bytes::from_static(b"mine"))
        .await
        .expect("appends");
    // Another process takes the staged file's name once it is gone, as one of another process
    // namespace may.
    let load_dir = wal.pipeline_dir(&orders).join(names::load(load));
    let part = std::fs::read_dir(&load_dir)
        .expect("lists")
        .map(|entry| entry.expect("an entry").path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "part")
        })
        .expect("a staged file");
    std::fs::remove_file(&part).expect("removes");
    std::fs::write(&part, b"theirs").expect("writes");
    set_mode(&part, 0o600);
    staged.publish().await.expect_err("not the file staged");
    assert_eq!(wal.chunks(&orders, load).await.expect("lists"), []);
}

#[tokio::test]
async fn a_directory_above_the_base_swapped_once_it_was_passed_is_refused_all_the_same() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let shared = base.path().join("shared");
    std::fs::create_dir_all(shared.join("wal")).expect("creates");
    set_mode(&shared.join("wal"), 0o700);
    set_mode(&shared, 0o777);
    // Once the base is opened under a directory others may write, that directory is swapped for
    // a private one before the path is looked at again.
    let moved = base.path().join("moved");
    *super::dir::OPENED.lock() = Some(Box::new({
        let (shared, moved) = (shared.clone(), moved.clone());
        move |_: &Path| {
            std::fs::rename(&shared, &moved).expect("moves");
            std::fs::create_dir_all(shared.join("wal")).expect("creates");
            set_mode(&shared.join("wal"), 0o700);
            set_mode(&shared, 0o700);
        }
    }));
    let wal = LocalWal::new(shared.join("wal"));
    let listed = wal.loads(&pipeline("orders")).await;
    *super::dir::OPENED.lock() = None;
    for dir in [&shared, &moved] {
        if dir.exists() {
            set_mode(dir, 0o700);
        }
    }
    let refused = listed.expect_err("the directory the base was opened under is refused");
    assert!(not_private(&refused), "{refused}");
}

#[tokio::test]
async fn a_link_on_the_way_to_the_base_is_followed_only_through_directories_that_pass() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let orders = pipeline("orders");
    // A link in a private directory, to a private directory, is followed.
    let own = base.path().join("own");
    std::fs::create_dir(&own).expect("creates");
    std::os::unix::fs::symlink(&own, base.path().join("to_own")).expect("links");
    let wal = LocalWal::new(base.path().join("to_own").join("wal"));
    wal.loads(&orders).await.expect("lists");
    assert!(own.join("wal").is_dir());
    // A link whose target passes through a directory others may write is not, though the link
    // itself is in a private one and the directory it ends at lies in private ones: whoever may
    // write that directory may point the next resolution elsewhere.
    let shared = base.path().join("shared");
    std::fs::create_dir(&shared).expect("creates");
    std::os::unix::fs::symlink(&own, shared.join("hop")).expect("links");
    set_mode(&shared, 0o777);
    std::os::unix::fs::symlink(shared.join("hop"), base.path().join("to_shared")).expect("links");
    let wal = LocalWal::new(base.path().join("to_shared").join("wal"));
    let refused = wal.loads(&orders).await.expect_err("refused");
    assert!(not_private(&refused), "{refused}");
    set_mode(&shared, 0o700);
}

#[tokio::test]
async fn names_a_file_system_or_a_desktop_makes_beside_a_log_are_passed_over() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    published(&wal, &orders, chunk(1, 0), b"frame").await;
    let dir = wal.pipeline_dir(&orders);
    let load_dir = dir.join(names::load(load));
    for made in [dir.join(".DS_Store"), load_dir.join(".nfs0000000000a1b2c3")] {
        std::fs::write(&made, b"").expect("writes");
        set_mode(&made, 0o600);
    }
    assert_eq!(wal.loads(&orders).await.expect("lists"), [load]);
    assert_eq!(wal.chunks(&orders, load).await.expect("lists"), [(0, 5)]);
    wal.remove_log(&orders, load).await.expect("removes");
    assert_eq!(wal.loads(&orders).await.expect("lists"), []);
    assert_eq!(wal.leftovers(&orders).await.expect("lists"), []);
    // A name the store could take for its own stays refused.
    std::fs::write(dir.join("0.wal"), b"").expect("writes");
    assert!(stray(&wal.loads(&orders).await.expect_err("refused")));
}

#[tokio::test]
async fn a_log_appears_open_or_not_at_all_and_an_open_a_crash_cut_is_a_leftover() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let (opened, cut) = (chunk(1, 0).load, chunk(2, 0).load);
    wal.open_log(&orders, opened).await.expect("opens");
    // What a crash leaves of an open half done: never a load's directory without its mark.
    let dir = wal.pipeline_dir(&orders);
    let half = dir.join(format!(".{}.opening", names::load(cut)));
    std::fs::create_dir(&half).expect("creates");
    set_mode(&half, 0o700);
    assert_eq!(wal.loads(&orders).await.expect("lists"), [opened]);
    assert_eq!(wal.leftovers(&orders).await.expect("lists"), [cut]);
    wal.remove_log(&orders, cut).await.expect("removes");
    assert!(!half.exists());
    assert_eq!(wal.leftovers(&orders).await.expect("lists"), []);
    let names: Vec<_> = std::fs::read_dir(&dir)
        .expect("lists")
        .map(|entry| entry.expect("an entry").file_name())
        .collect();
    assert_eq!(names, [std::ffi::OsString::from(names::load(opened))]);
}

#[test]
fn a_link_in_a_directory_others_may_write_is_followed_only_where_it_is_the_user_s_or_root_s() {
    use super::dir::link_followed;
    let (me, other) = (1_000, 1_001);
    for (mode, owner, followed) in [
        (0o1777, me, true),
        (0o1777, 0, true),
        (0o1777, other, false),
        (0o1770, other, false),
        (0o755, other, true),
        (0o700, me, true),
    ] {
        assert_eq!(link_followed(mode, owner, me), followed, "{mode:o} {owner}");
    }
}

#[tokio::test]
async fn a_link_of_the_user_s_own_in_a_sticky_directory_is_followed() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let (sticky, own) = (base.path().join("sticky"), base.path().join("own"));
    for dir in [&sticky, &own] {
        std::fs::create_dir(dir).expect("creates");
    }
    std::os::unix::fs::symlink(&own, sticky.join("link")).expect("links");
    set_mode(&sticky, 0o1777);
    let wal = LocalWal::new(sticky.join("link").join("wal"));
    wal.loads(&pipeline("orders")).await.expect("lists");
    assert!(own.join("wal").is_dir());
    set_mode(&sticky, 0o700);
}

#[tokio::test]
async fn a_relative_base_is_taken_against_the_directory_the_store_was_made_in() {
    let (made, moved) = (
        tempfile::tempdir().expect("a temporary directory"),
        tempfile::tempdir().expect("a temporary directory"),
    );
    // Each test runs in a process of its own, whose working directory this one may move.
    std::env::set_current_dir(made.path()).expect("moves");
    let wal = LocalWal::new(".rdlt");
    std::env::set_current_dir(moved.path()).expect("moves");
    wal.open_log(&pipeline("orders"), chunk(1, 0).load)
        .await
        .expect("opens");
    assert!(made.path().join(".rdlt").is_dir());
    assert!(!moved.path().join(".rdlt").exists());
}

#[tokio::test]
async fn a_store_s_identity_is_its_first_and_kept_in_its_base() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let (first, second) = (chunk(1, 0).load, chunk(2, 0).load);
    let identity = LocalWal::new(base.path())
        .identity(first)
        .await
        .expect("named");
    assert_eq!(identity, first);
    let again = LocalWal::new(base.path())
        .identity(second)
        .await
        .expect("named");
    assert_eq!(again, first, "another process of the same base");
    assert_eq!(mode(&base.path().join("store")), 0o600);
}

#[test]
fn every_way_a_kernel_answers_a_link_opened_without_following_reads_as_a_link() {
    use super::dir::{is_link, is_not_a_directory};
    use rustix::io::Errno;
    // Linux answers `ELOOP`, FreeBSD `EMLINK`, and a directory open of a file `ENOTDIR`.
    for errno in [Errno::LOOP, Errno::MLINK] {
        assert!(is_link(errno), "{errno:?}");
        assert!(is_not_a_directory(errno), "{errno:?}");
    }
    assert!(!is_link(Errno::NOTDIR));
    assert!(is_not_a_directory(Errno::NOTDIR));
    for errno in [Errno::NOENT, Errno::ACCESS, Errno::EXIST] {
        assert!(!is_link(errno) && !is_not_a_directory(errno), "{errno:?}");
    }
}

#[tokio::test]
async fn a_directory_on_the_way_to_the_base_the_user_cannot_open_fails_as_it_is() {
    if rustix::process::geteuid().is_root() {
        return;
    }
    let base = tempfile::tempdir().expect("a temporary directory");
    let locked = base.path().join("locked");
    std::fs::create_dir(&locked).expect("creates");
    set_mode(&locked, 0o000);
    let wal = LocalWal::new(locked.join("wal"));
    let failed = wal
        .loads(&pipeline("orders"))
        .await
        .expect_err("it cannot be opened");
    set_mode(&locked, 0o700);
    // Not a link, nor anything else the walk reads as one: the kernel's answer, as it is.
    assert!(refusal(&failed).is_none(), "{failed}");
    assert_eq!(
        failed.kind(),
        std::io::ErrorKind::PermissionDenied,
        "{failed}"
    );
}

#[tokio::test]
async fn a_file_where_a_log_s_directory_belongs_is_refused() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let x = pipeline("x");
    published(&wal, &pipeline("y"), chunk(7, 0), b"Y").await;
    // This user's own regular file, which opens to read all the same.
    let path = wal.pipeline_dir(&x);
    std::fs::write(&path, b"").expect("writes");
    set_mode(&path, 0o600);
    let refused = wal.loads(&x).await.expect_err("refused");
    let why = "it is not a directory";
    // The walk names the real path, which a temporary directory behind a link (macOS) is not.
    let path = path.canonicalize().expect("a real path");
    assert_eq!(
        refusal(&refused),
        Some(&Refusal::NotPrivate { path, why }),
        "{refused}"
    );
}

#[tokio::test]
async fn a_log_s_directory_the_user_cannot_open_fails_as_it_is() {
    if rustix::process::geteuid().is_root() {
        return;
    }
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let x = pipeline("x");
    published(&wal, &x, chunk(7, 0), b"X").await;
    let path = wal.pipeline_dir(&x);
    set_mode(&path, 0o000);
    let failed = wal.loads(&x).await.expect_err("it cannot be opened");
    set_mode(&path, 0o700);
    // The kernel's answer, as it is: no refusal that the name is no directory.
    assert!(refusal(&failed).is_none(), "{failed}");
    assert_eq!(
        failed.kind(),
        std::io::ErrorKind::PermissionDenied,
        "{failed}"
    );
}

#[test]
fn a_staging_token_holds_the_process_s_id_above_its_count() {
    let count = std::sync::atomic::AtomicU32::new(7);
    let id = u64::from(std::process::id());
    // Another process's tokens differ in their id, and this process's in their count.
    assert_eq!(names::token(&count), (id << 32) + 7);
    assert_eq!(names::token(&count), (id << 32) + 8);
}

#[tokio::test]
async fn a_staging_takes_another_name_only_where_its_own_is_taken() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    opened(&wal, &orders, load).await;
    let load_dir = wal.pipeline_dir(&orders).join(names::load(load));
    let parts = || -> Vec<String> {
        let mut parts: Vec<String> = std::fs::read_dir(&load_dir)
            .expect("lists")
            .map(|entry| entry.expect("an entry").file_name())
            .filter(|name| names::is_part(name))
            .map(|name| name.into_string().expect("UTF-8"))
            .collect();
        parts.sort();
        parts
    };
    let first = wal.stage(&orders, chunk(1, 0)).await.expect("stages");
    let [name] = &parts()[..] else {
        panic!("one staged file: {:?}", parts());
    };
    let token = name.split('.').nth(1).expect("a token");
    let token = u64::from_str_radix(token, 16).expect("a hexadecimal token");
    // Files an earlier process of the same id left take the names the next stagings try first.
    let left: Vec<String> = (1..=3).map(|next| names::part(1, token + next)).collect();
    for name in &left {
        std::fs::write(load_dir.join(name), b"left").expect("writes");
    }
    let second = wal
        .stage(&orders, chunk(1, 1))
        .await
        .expect("stages under a free name");
    assert_eq!(parts().len(), 5, "{:?}", parts());
    for name in &left {
        assert_eq!(std::fs::read(load_dir.join(name)).expect("reads"), b"left");
    }
    first.discard().await.expect("discards");
    second.discard().await.expect("discards");
    // A staging that fails for any other reason fails as it did, at its first name.
    if rustix::process::geteuid().is_root() {
        return;
    }
    set_mode(&load_dir, 0o500);
    let staged = wal.stage(&orders, chunk(1, 2)).await;
    set_mode(&load_dir, 0o700);
    let Err(failed) = staged else {
        panic!("staged in a directory the user may not write");
    };
    assert_eq!(
        failed.kind(),
        std::io::ErrorKind::PermissionDenied,
        "{failed}"
    );
}

#[test]
fn a_directory_s_names_are_its_entries_in_order_without_itself_or_its_parent() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let dir = super::dir::Dir::base(&base.path().join("wal")).expect("creates");
    dir.dir_new("b").expect("creates");
    drop(dir.create("a").expect("creates"));
    assert_eq!(dir.names().expect("lists"), ["a", "b"]);
}

#[test]
fn a_file_is_created_only_under_a_free_name_and_never_through_a_link() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = base.path().join("wal");
    let dir = super::dir::Dir::base(&wal).expect("creates");
    drop(dir.create("a").expect("creates"));
    let taken = dir.create("a").expect_err("its name is taken");
    assert_eq!(taken.kind(), std::io::ErrorKind::AlreadyExists, "{taken}");
    // A link to where nothing is yet takes its name all the same, and nothing is made there.
    let elsewhere = base.path().join("elsewhere");
    std::os::unix::fs::symlink(&elsewhere, wal.join("b")).expect("links");
    let linked = dir.create("b").expect_err("a link takes the name");
    assert_eq!(linked.kind(), std::io::ErrorKind::AlreadyExists, "{linked}");
    assert!(!elsewhere.exists());
}

#[tokio::test]
async fn a_proposal_another_linked_in_first_reads_its_identity_and_any_other_failure_fails() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let first = chunk(1, 0).load;
    LocalWal::new(base.path())
        .identity(first)
        .await
        .expect("named");
    let dir = super::dir::Dir::base(base.path()).expect("opens");
    let taken = std::io::Error::from(std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        super::identity::unwritten(&dir, taken).expect("read"),
        first
    );
    let full = std::io::Error::from(std::io::ErrorKind::StorageFull);
    let failed = super::identity::unwritten(&dir, full).expect_err("fails");
    assert_eq!(failed.kind(), std::io::ErrorKind::StorageFull, "{failed}");
}

#[tokio::test]
async fn a_store_s_identity_the_user_cannot_read_fails_as_it_is() {
    if rustix::process::geteuid().is_root() {
        return;
    }
    let base = tempfile::tempdir().expect("a temporary directory");
    LocalWal::new(base.path())
        .identity(chunk(1, 0).load)
        .await
        .expect("named");
    let store = base.path().join("store");
    set_mode(&store, 0o000);
    let failed = LocalWal::new(base.path())
        .identity(chunk(2, 0).load)
        .await
        .expect_err("it cannot be read");
    set_mode(&store, 0o600);
    // Not a link, nor anything else the open reads as one: the kernel's answer, as it is.
    assert!(refusal(&failed).is_none(), "{failed}");
    assert_eq!(
        failed.kind(),
        std::io::ErrorKind::PermissionDenied,
        "{failed}"
    );
}

#[tokio::test]
#[expect(
    clippy::disallowed_methods,
    reason = "a pipe's open is bounded on the real clock, which no Env drives here"
)]
async fn a_pipe_where_the_store_s_identity_belongs_is_refused_at_once() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store = base.path().join("store");
    let made = std::process::Command::new("mkfifo").arg(&store).status();
    assert!(made.expect("mkfifo runs").success());
    set_mode(&store, 0o600);
    let wal = LocalWal::new(base.path());
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        wal.identity(chunk(1, 0).load),
    )
    .await;
    if read.is_err() {
        // An open that waits for a writer is given one, so the test ends.
        drop(std::fs::OpenOptions::new().write(true).open(&store));
    }
    let refused = read.expect("refused at once").expect_err("refused");
    let not_regular = matches!(
        refusal(&refused),
        Some(Refusal::NotPrivate {
            why: "it is not a regular file",
            ..
        })
    );
    assert!(not_regular, "{refused}");
}

#[tokio::test]
async fn a_read_starts_at_a_64_byte_address_whatever_its_offset() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let written: &'static [u8] = (0..1_000_u32)
        .map(|byte| u8::try_from(byte % 251).expect("a byte"))
        .collect::<Vec<u8>>()
        .leak();
    published(&wal, &orders, chunk(1, 0), written).await;
    for offset in [0_u64, 1, 13, 64, 999, 1_000] {
        let read = wal
            .read(&orders, chunk(1, 0), offset, 500)
            .await
            .expect("reads");
        let from = usize::try_from(offset).expect("small");
        assert_eq!(read, written[from..(from + 500).min(1_000)], "{offset}");
        assert!(
            read.is_empty() || rdlt_wire::aligned::is_aligned(&read),
            "{offset}"
        );
    }
}

#[tokio::test]
async fn a_frame_the_local_store_reads_is_decoded_where_it_lies() {
    use std::sync::Arc;

    use arrow_array::{Array as _, ArrayRef, Int64Array, RecordBatch, StringArray};
    use rdlt_connector::SegmentId;

    use crate::wal::frame::{self, Batch, Frame};
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1_000));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..1_000).map(|id| format!("name {id}")),
    ));
    let batch = RecordBatch::try_from_iter([("id", ids), ("name", names)]).expect("a batch");
    let encoded = Frame::Batch(Batch {
        segment: SegmentId(1),
        table: 0,
        ordinal: 0,
        batch: batch.clone(),
    })
    .encode()
    .expect("the frame encodes");
    let chunk_bytes: &'static [u8] = [&frame::preamble()[..], &encoded[..]].concat().leak();
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    published(&wal, &orders, chunk(1, 0), chunk_bytes).await;
    let offset = u64::try_from(frame::PREAMBLE).expect("small");
    let len = u64::try_from(encoded.len()).expect("small");
    let read = wal
        .read(&orders, chunk(1, 0), offset, len)
        .await
        .expect("reads");
    let range = read.as_ptr_range();
    let pending = frame::pending(read, frame::limits(1 << 30)).expect("a batch frame");
    let decoded = pending.decode().expect("it decodes").batch;
    assert_eq!(decoded, batch);
    // Every buffer lies in the bytes the store read: nothing copied them.
    for column in decoded.columns() {
        let data = column.to_data();
        for buffer in data.buffers() {
            let at = buffer.as_ptr_range();
            assert!(range.start <= at.start && at.end <= range.end);
        }
    }
}

/// Whether `name` is one plain name a directory may hold: not empty, not `.` or `..`, and
/// holding no separator or NUL.
fn plain(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\0')
}

/// Two pipeline ids that fold alike or nearly: an id and the same with its case changed, or
/// its `.` and `_` swapped, or another id.
fn pipeline_pair() -> impl Strategy<Value = (String, String)> {
    prop_oneof!["[A-Za-z0-9._-]{1,128}", Just("A".repeat(128))].prop_flat_map(|id| {
        let swapped: String = id
            .chars()
            .map(|c| match c {
                '.' => '_',
                '_' => '.',
                c => c,
            })
            .collect();
        let other = prop_oneof![
            Just(id.to_uppercase()),
            Just("A".repeat(128)),
            Just(id.to_lowercase()),
            Just(swapped),
            "[A-Za-z0-9._-]{1,128}",
        ];
        (Just(id), other)
    })
}

/// Names near those a log writes: chunk numbers of every width, staging names with a token of
/// any case and width, ids as written, in upper case or in another form, each with none, one or
/// two dots before it and with or without the ending of the name a load is opened in, in either
/// case, and noise.
fn near_names() -> impl Strategy<Value = String> {
    prop_oneof![
        (any::<u32>(), 0_usize..12).prop_map(|(number, width)| format!("{number:0width$}.wal")),
        (any::<u32>(), any::<u64>(), 0_usize..20, any::<bool>()).prop_map(
            |(number, token, width, upper)| {
                let token = if upper {
                    format!("{token:0width$X}")
                } else {
                    format!("{token:0width$x}")
                };
                format!("{number:08}.{token}.part")
            }
        ),
        (
            any::<u128>(),
            0_u8..5,
            prop::sample::select(vec!["", ".", ".."]),
            prop::sample::select(vec!["", ".opening", ".OPENING"]),
        )
            .prop_map(|(random, form, dots, opening)| {
                let load = LoadId::from_parts(UNIX_EPOCH, random).to_string();
                let simple = load.replace('-', "");
                let form = match form {
                    0 => load.to_uppercase(),
                    1 => simple,
                    2 => format!("{{{load}}}"),
                    3 => format!("urn:uuid:{load}"),
                    _ => load,
                };
                format!("{dots}{form}{opening}")
            }),
        "[0-9a-fA-F.+ -]{0,40}(\\.wal|\\.part|\\.opening)?",
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// A pipeline's directory is one plain name, at most 207 bytes, that no listing passes over
    /// as a system's, and no other pipeline's folds to it, on a file system that folds case.
    #[test]
    fn pipelines_keep_directories_of_their_own_that_are_one_plain_name(
        (one, other) in pipeline_pair(),
    ) {
        let one_dir = names::pipeline(&pipeline(&one));
        let other_dir = names::pipeline(&pipeline(&other));
        for name in [&one_dir, &other_dir] {
            prop_assert!(plain(name), "{}", name);
            prop_assert!(name.len() <= 207, "{}", name);
            prop_assert!(!names::is_made_by_system(name.as_ref()), "{}", name);
            prop_assert!(name.starts_with("p.") || name.starts_with("x."), "{}", name);
        }
        prop_assert_eq!(one_dir.to_lowercase() == other_dir.to_lowercase(), one == other);
    }

    /// Every name a log writes for a chunk, a staging file, a load or a load being opened is one
    /// plain name read back as what it names, and a name is read only as it is written.
    #[test]
    fn the_names_a_log_writes_are_read_back_only_as_written(
        number in any::<u64>(),
        token in any::<u64>(),
        random in any::<u128>(),
        near in near_names(),
    ) {
        let load = LoadId::from_parts(UNIX_EPOCH, random);
        prop_assert_eq!(names::parse_chunk(names::chunk(number).as_ref()), Some(number));
        prop_assert!(names::is_part(names::part(number, token).as_ref()));
        prop_assert_eq!(names::parse_load(names::load(load).as_ref()), Some(load));
        prop_assert_eq!(names::parse_opening(names::opening(load).as_ref()), Some(load));
        for name in [names::chunk(number), names::part(number, token), names::load(load)] {
            prop_assert!(plain(&name), "{}", name);
            prop_assert!(!names::is_made_by_system(name.as_ref()), "{}", name);
        }
        prop_assert!(plain(&names::opening(load)), "{}", names::opening(load));
        prop_assert!(names::is_made_by_system(names::opening(load).as_ref()));
        if let Some(number) = names::parse_chunk(near.as_ref()) {
            prop_assert_eq!(&near, &names::chunk(number));
        }
        if let Some(load) = names::parse_load(near.as_ref()) {
            prop_assert_eq!(&near, &names::load(load));
        }
        if let Some(load) = names::parse_opening(near.as_ref()) {
            prop_assert_eq!(&near, &names::opening(load));
        }
        if names::is_part(near.as_ref()) {
            let (number, token) = near
                .strip_suffix(".part")
                .and_then(|rest| rest.split_once('.'))
                .expect("a staging name");
            let number = number.parse().expect("a number");
            let token = u64::from_str_radix(token, 16).expect("a token");
            let written = names::part(number, token);
            prop_assert_eq!(&near, &written);
        }
    }
}
