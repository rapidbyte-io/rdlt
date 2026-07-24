//! Probe pinning the object_store list-prefix semantics the destination
//! relies on: prefixes match on a path-segment basis, so listing table
//! `a` can never return keys of a sibling table `ab` whose name shares a
//! byte prefix. The S3 backend enforces this server-side by appending
//! the `/` delimiter to the requested prefix; if this probe ever fails,
//! per-table listing (row counting, replace truncation, staging cleanup)
//! must grow its own `"{table}/"` ownership guard.

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;

#[tokio::test]
async fn list_prefix_is_segment_based_not_byte_based() {
    let store = InMemory::new();
    for key in [
        "out/a/part-0.parquet",
        "out/a/date=2024/part-1.parquet",
        "out/ab/part-0.parquet",
        "out/abc/part-0.parquet",
    ] {
        store
            .put(&Path::from(key), bytes::Bytes::from_static(b"x").into())
            .await
            .expect("seed");
    }
    let listed: Vec<String> = store
        .list(Some(&Path::from("out/a")))
        .map(|entry| entry.expect("list").location.to_string())
        .collect()
        .await;
    assert_eq!(
        listed.len(),
        2,
        "prefix `out/a` leaked sibling-table keys: {listed:?}"
    );
    assert!(
        listed.iter().all(|k| k.starts_with("out/a/")),
        "non-owned key in listing: {listed:?}"
    );
}
