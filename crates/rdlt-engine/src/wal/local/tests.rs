use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::{LoadId, PipelineId};

use super::LocalWal;
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

#[tokio::test]
async fn a_log_reads_back_by_range_what_was_appended_to_its_chunks() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    assert_eq!(wal.loads(&orders).await.expect("loads list"), []);
    for (number, text) in [(0, &b"first chunk"[..]), (1, b"second")] {
        wal.append(&orders, chunk(1, number), Bytes::from_static(&text[..5]))
            .await
            .expect("appends");
        wal.append(
            &orders,
            chunk(1, number),
            Bytes::copy_from_slice(&text[5..]),
        )
        .await
        .expect("appends");
        wal.sync(&orders, chunk(1, number)).await.expect("syncs");
    }
    wal.append(&orders, chunk(2, 0), Bytes::from_static(b"x"))
        .await
        .expect("appends");
    let loads = wal.loads(&orders).await.expect("loads list");
    assert_eq!(loads, [chunk(1, 0).load, chunk(2, 0).load]);
    let chunks = wal
        .chunks(&orders, chunk(1, 0).load)
        .await
        .expect("chunks list");
    assert_eq!(chunks, [(0, 11), (1, 6)]);
    let read = wal.read(&orders, chunk(1, 0), 6, 5).await.expect("reads");
    assert_eq!(&read[..], b"chunk");
    // A read past the end returns what there is.
    let tail = wal.read(&orders, chunk(1, 1), 2, 100).await.expect("reads");
    assert_eq!(&tail[..], b"cond");
    // Another pipeline's logs are its own.
    assert_eq!(wal.loads(&pipeline("other")).await.expect("loads list"), []);
    // A log goes with its last chunk.
    wal.remove(&orders, chunk(1, 0)).await.expect("removes");
    assert_eq!(
        wal.chunks(&orders, chunk(1, 0).load).await.expect("chunks"),
        [(1, 6)]
    );
    wal.remove(&orders, chunk(1, 1)).await.expect("removes");
    wal.remove(&orders, chunk(1, 1))
        .await
        .expect("removing again changes nothing");
    assert_eq!(
        wal.loads(&orders).await.expect("loads list"),
        [chunk(2, 0).load]
    );
}

#[test]
fn pipelines_whose_names_sanitize_alike_keep_directories_of_their_own() {
    let wal = LocalWal::new("/base");
    let [a, b] = ["orders.eu", "orders_eu"].map(|name| wal.pipeline_dir(&pipeline(name)));
    assert_ne!(a, b);
    assert!(a.starts_with("/base"), "{}", a.display());
    let name = a
        .file_name()
        .and_then(|name| name.to_str())
        .expect("a name");
    assert!(name.starts_with("orders_eu-"), "{name}");
}

#[cfg(unix)]
#[tokio::test]
async fn a_log_s_directories_are_readable_by_their_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path().join("wal"));
    let orders = pipeline("orders");
    wal.append(&orders, chunk(1, 0), Bytes::from_static(b"x"))
        .await
        .expect("appends");
    let dir = wal.pipeline_dir(&orders);
    for dir in [dir.clone(), dir.join(chunk(1, 0).load.to_string())] {
        let mode = std::fs::metadata(&dir)
            .expect("the directory exists")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700, "{}", dir.display());
    }
}

/// A store's claims: one holder per log until it lets go, and a removed log gone whole.
async fn claims_exclude_one_another(wal: &dyn WalStore) {
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    let held = wal
        .claim(&orders, load)
        .await
        .expect("claims")
        .expect("free");
    assert!(
        wal.claim(&orders, load).await.expect("claims").is_none(),
        "a claimed log is refused, even to its own process"
    );
    let other = wal.claim(&orders, chunk(2, 0).load).await.expect("claims");
    assert!(other.is_some(), "claims are per log");
    wal.append(&orders, chunk(1, 0), Bytes::from_static(b"frame"))
        .await
        .expect("appends");
    wal.append(&orders, chunk(2, 0), Bytes::from_static(b"other"))
        .await
        .expect("appends");
    assert!(
        wal.loads(&orders)
            .await
            .expect("loads list")
            .contains(&load)
    );
    drop(held);
    let again = wal.claim(&orders, load).await.expect("claims");
    assert!(again.is_some(), "a claim is free once let go");
    wal.remove_log(&orders, load).await.expect("removes");
    assert!(
        !wal.loads(&orders)
            .await
            .expect("loads list")
            .contains(&load)
    );
    assert_eq!(wal.chunks(&orders, load).await.expect("chunks"), []);
    let other = wal.chunks(&orders, chunk(2, 0).load).await.expect("chunks");
    assert_eq!(other, [(0, 5)], "another log stays");
    wal.remove_log(&orders, load)
        .await
        .expect("removing again changes nothing");
}

#[tokio::test]
async fn a_local_log_has_one_claimant_at_a_time() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    claims_exclude_one_another(&wal).await;
    let dir = wal.pipeline_dir(&pipeline("orders"));
    let mut left: Vec<String> = std::fs::read_dir(&dir)
        .expect("the pipeline's directory stays")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    left.sort();
    let other = chunk(2, 0).load.to_string();
    assert_eq!(
        left,
        [other.clone(), format!("{other}.lock")],
        "only the other log is left"
    );
}

#[tokio::test]
async fn a_log_in_memory_has_one_claimant_at_a_time() {
    claims_exclude_one_another(&super::super::memory::MemoryWal::default()).await;
}

#[tokio::test]
async fn a_log_that_never_wrote_a_frame_is_still_listed_so_replay_removes_its_claim() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(3, 0).load;
    // A load that claimed its log and failed before its first frame leaves only the claim's mark.
    drop(wal.claim(&orders, load).await.expect("claims"));
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
async fn a_synced_chunk_lets_its_file_go_and_reopens_where_appended_again() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    for number in 0..3 {
        wal.append(&orders, chunk(1, number), Bytes::from_static(b"frame"))
            .await
            .expect("appends");
        wal.sync(&orders, chunk(1, number)).await.expect("syncs");
    }
    assert_eq!(
        wal.open.lock().len(),
        0,
        "no finished chunk holds a file open"
    );
    wal.append(&orders, chunk(1, 2), Bytes::from_static(b"more"))
        .await
        .expect("appends");
    let read = wal.read(&orders, chunk(1, 2), 0, 100).await.expect("reads");
    assert_eq!(&read[..], b"framemore");
}

#[test]
fn a_private_directory_names_what_it_created_and_nothing_that_was_there() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let deep = base.path().join("a").join("b").join("c");
    let created = super::private_dir(&deep).expect("creates");
    assert_eq!(
        created,
        [
            deep.clone(),
            base.path().join("a").join("b"),
            base.path().join("a")
        ]
    );
    assert_eq!(
        super::private_dir(&deep).expect("exists"),
        Vec::<std::path::PathBuf>::new()
    );
}

#[test]
fn a_pipeline_s_directory_keeps_the_characters_paths_take_and_a_short_hash() {
    let wal = LocalWal::new("/base");
    let dir = wal.pipeline_dir(&pipeline("orders-eu_1.v2"));
    let name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .expect("a name");
    let (safe, hash) = name.rsplit_once('-').expect("a hash after the name");
    assert_eq!(safe, "orders-eu_1_v2");
    assert_eq!(hash.len(), 8, "{hash}");
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()), "{hash}");
}

#[tokio::test]
async fn a_log_s_paths_taken_by_what_the_store_does_not_expect_are_errors_not_absences() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    let load = chunk(1, 0).load;
    // The pipeline's directory is a file: nothing can be listed in it.
    std::fs::write(wal.pipeline_dir(&orders), b"").expect("writes");
    assert!(wal.loads(&orders).await.is_err());
    assert!(wal.chunks(&orders, load).await.is_err());
    std::fs::remove_file(wal.pipeline_dir(&orders)).expect("removes");
    // The load's directory is a file; then its claim's mark is a directory.
    let dir = wal.pipeline_dir(&orders);
    std::fs::create_dir_all(&dir).expect("creates");
    std::fs::write(dir.join(load.to_string()), b"").expect("writes");
    assert!(
        wal.remove_log(&orders, load).await.is_err(),
        "the log cannot go"
    );
    std::fs::remove_file(dir.join(load.to_string())).expect("removes");
    std::fs::create_dir_all(dir.join(format!("{load}.lock"))).expect("creates");
    assert!(
        wal.remove_log(&orders, load).await.is_err(),
        "the mark cannot go"
    );
    // A chunk that is a directory cannot be removed as a file.
    std::fs::create_dir_all(dir.join(load.to_string()).join("00000000.wal")).expect("creates");
    assert!(wal.remove(&orders, chunk(1, 0)).await.is_err());
}

#[tokio::test]
async fn removing_a_log_lets_its_files_go_and_keeps_every_other_log_s() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal = LocalWal::new(base.path());
    let orders = pipeline("orders");
    for load in [1, 2] {
        wal.append(&orders, chunk(load, 0), Bytes::from_static(b"frame"))
            .await
            .expect("appends");
    }
    wal.remove_log(&orders, chunk(1, 0).load)
        .await
        .expect("removes");
    let open: Vec<_> = wal.open.lock().keys().cloned().collect();
    assert_eq!(open.len(), 1, "{open:?}");
    assert!(open[0].starts_with(wal.pipeline_dir(&orders).join(chunk(2, 0).load.to_string())));
}

#[test]
fn a_private_directory_under_a_relative_base_is_created_the_first_time() {
    let base = tempfile::tempdir().expect("a temporary directory");
    // Each test runs in a process of its own, whose working directory this one may move.
    std::env::set_current_dir(base.path()).expect("moves");
    let created = super::private_dir(std::path::Path::new(".rdlt/orders")).expect("creates");
    assert_eq!(
        created,
        [
            std::path::PathBuf::from(".rdlt/orders"),
            std::path::PathBuf::from(".rdlt")
        ]
    );
    assert!(base.path().join(".rdlt/orders").is_dir());
}
