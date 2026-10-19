use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::multipart::MultipartStore as _;
use object_store::path::Path;
use object_store::{ObjectStoreExt as _, PutPayload};

use super::Disk;
use crate::oracle::charged;

fn payload(parts: &[&[u8]]) -> PutPayload {
    parts
        .iter()
        .map(|part| Bytes::copy_from_slice(part))
        .collect()
}

#[tokio::test]
async fn an_object_counts_as_the_worlds_until_it_is_deleted() {
    let disk = Disk::new(InMemory::new());
    let before = charged();
    let key = Path::from("logs/a");
    disk.put(&key, payload(&[b"abc", b"defg"])).await.unwrap();
    assert_eq!(charged() - before, 7);
    let read = disk.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(&read[..], b"abcdefg");
    disk.delete(&key).await.unwrap();
    // The bytes read still hold the copy, which counts until they go.
    assert_eq!(charged() - before, 7);
    drop(read);
    assert_eq!(charged(), before);
}

#[tokio::test]
async fn an_upload_counts_its_parts_then_the_object_they_make() {
    let disk = Disk::new(InMemory::new());
    let before = charged();
    let key = Path::from("logs/b");
    let id = disk.create_multipart(&key).await.unwrap();
    let first = disk
        .put_part(&key, &id, 0, payload(&[b"ab"]))
        .await
        .unwrap();
    let second = disk
        .put_part(&key, &id, 1, payload(&[b"cde"]))
        .await
        .unwrap();
    assert_eq!(charged() - before, 5);
    disk.complete_multipart(&key, &id, vec![first, second])
        .await
        .unwrap();
    assert_eq!(charged() - before, 5);
    let read = disk.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(&read[..], b"abcde");
    drop(read);
    disk.delete(&key).await.unwrap();
    assert_eq!(charged(), before);
}

#[tokio::test]
async fn an_upload_missing_a_part_or_aborted_keeps_nothing() {
    let disk = Disk::new(InMemory::new());
    let before = charged();
    let key = Path::from("logs/c");
    let id = disk.create_multipart(&key).await.unwrap();
    disk.put_part(&key, &id, 1, payload(&[b"late"]))
        .await
        .unwrap();
    assert!(
        disk.complete_multipart(&key, &id, Vec::new())
            .await
            .is_err()
    );
    assert_eq!(charged(), before);
    let id = disk.create_multipart(&key).await.unwrap();
    disk.put_part(&key, &id, 0, payload(&[b"gone"]))
        .await
        .unwrap();
    disk.abort_multipart(&key, &id).await.unwrap();
    assert_eq!(charged(), before);
}
