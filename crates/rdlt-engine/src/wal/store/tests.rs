use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::{LoadId, PipelineId};

use super::{Chunk, LocalWal, WalStore};

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
    assert_eq!(wal.loads(&orders).await.expect("loads list"), [load]);
    drop(held);
    let again = wal.claim(&orders, load).await.expect("claims");
    assert!(again.is_some(), "a claim is free once let go");
    wal.remove_log(&orders, load).await.expect("removes");
    assert_eq!(wal.loads(&orders).await.expect("loads list"), []);
    assert_eq!(wal.chunks(&orders, load).await.expect("chunks"), []);
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
    let left: Vec<_> = std::fs::read_dir(&dir)
        .expect("the pipeline's directory stays")
        .map(|entry| entry.expect("an entry").file_name())
        .collect();
    assert_eq!(
        left.len(),
        1,
        "only the other load's lock is left: {left:?}"
    );
}

#[tokio::test]
async fn a_log_in_memory_has_one_claimant_at_a_time() {
    claims_exclude_one_another(&super::super::memory::MemoryWal::default()).await;
}
