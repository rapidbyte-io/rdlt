//! What every [`WalStore`] does, as tests any store runs: the memory and the local store here, an
//! object store as it is built.

use std::io;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::{LoadId, PipelineId};

use super::{Chunk, WalStore};

fn pipeline(name: &str) -> PipelineId {
    PipelineId::parse(name).expect("a valid pipeline")
}

fn chunk(load: u128, number: u64) -> Chunk {
    Chunk {
        load: LoadId::from_parts(UNIX_EPOCH, load),
        number,
    }
}

/// Stages `bytes` as `chunk` of `pipeline`'s log and publishes it.
async fn published(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    chunk: Chunk,
    bytes: &'static [u8],
) -> io::Result<()> {
    let mut staged = store.stage(pipeline, chunk).await?;
    for part in bytes.chunks(3) {
        staged.append(Bytes::from_static(part)).await?;
    }
    staged.publish().await
}

/// Runs every check of the contract on `store`, which must hold nothing yet.
pub(crate) async fn conforms(store: &dyn WalStore) {
    a_staged_chunk_is_seen_by_no_reader_until_published(store).await;
    a_published_chunk_is_whole_read_by_range_and_never_replaced(store).await;
    the_first_of_two_chunks_of_one_name_published_is_kept(store).await;
    chunks_and_loads_list_in_order_whatever_order_they_came_in(store).await;
    a_deletion_is_seen_at_once_and_deleting_again_changes_nothing(store).await;
    removing_a_log_removes_it_whole_and_nothing_else(store).await;
}

async fn a_staged_chunk_is_seen_by_no_reader_until_published(store: &dyn WalStore) {
    let orders = pipeline("staged");
    let at = chunk(1, 0);
    let mut staged = store.stage(&orders, at).await.expect("stages");
    staged
        .append(Bytes::from_static(b"frames"))
        .await
        .expect("appends");
    assert_eq!(store.chunks(&orders, at.load).await.expect("lists"), []);
    assert!(
        store.read(&orders, at, 0, 6).await.is_err(),
        "nothing to read"
    );
    staged.publish().await.expect("publishes");
    assert_eq!(
        store.chunks(&orders, at.load).await.expect("lists"),
        [(0, 6)]
    );
    assert_eq!(store.loads(&orders).await.expect("lists"), [at.load]);
}

async fn a_published_chunk_is_whole_read_by_range_and_never_replaced(store: &dyn WalStore) {
    let orders = pipeline("whole");
    let at = chunk(1, 0);
    published(store, &orders, at, b"first chunk")
        .await
        .expect("publishes");
    let read = store.read(&orders, at, 6, 5).await.expect("reads");
    assert_eq!(&read[..], b"chunk");
    // A read past the end returns what there is, and holds no more than that.
    let tail = store.read(&orders, at, 2, u64::MAX).await.expect("reads");
    assert_eq!(&tail[..], b"rst chunk");
    let empty = store.read(&orders, at, 100, 5).await.expect("reads");
    assert!(empty.is_empty());
    let again = published(store, &orders, at, b"other").await;
    let refused = again.expect_err("the name is taken");
    assert_eq!(refused.kind(), io::ErrorKind::AlreadyExists, "{refused}");
    let read = store.read(&orders, at, 0, 100).await.expect("reads");
    assert_eq!(&read[..], b"first chunk", "the chunk is unchanged");
}

async fn the_first_of_two_chunks_of_one_name_published_is_kept(store: &dyn WalStore) {
    let orders = pipeline("raced");
    let at = chunk(1, 4);
    let mut writer = store.stage(&orders, at).await.expect("stages");
    let mut fence = store.stage(&orders, at).await.expect("stages");
    writer
        .append(Bytes::from_static(b"commit"))
        .await
        .expect("appends");
    fence
        .append(Bytes::from_static(b"fence"))
        .await
        .expect("appends");
    fence.publish().await.expect("publishes first");
    let refused = writer.publish().await.expect_err("fenced");
    assert_eq!(refused.kind(), io::ErrorKind::AlreadyExists, "{refused}");
    let read = store.read(&orders, at, 0, 100).await.expect("reads");
    assert_eq!(&read[..], b"fence");
}

async fn chunks_and_loads_list_in_order_whatever_order_they_came_in(store: &dyn WalStore) {
    let orders = pipeline("ordered");
    for (load, number) in [(3, 2), (1, 7), (3, 0), (2, 1), (3, 10)] {
        published(store, &orders, chunk(load, number), b"x")
            .await
            .expect("publishes");
    }
    let loads = store.loads(&orders).await.expect("lists");
    assert_eq!(
        loads,
        [chunk(1, 0).load, chunk(2, 0).load, chunk(3, 0).load]
    );
    let chunks = store
        .chunks(&orders, chunk(3, 0).load)
        .await
        .expect("lists");
    assert_eq!(chunks, [(0, 1), (2, 1), (10, 1)]);
    assert_eq!(store.loads(&pipeline("none")).await.expect("lists"), []);
    let unknown = store
        .chunks(&orders, chunk(9, 0).load)
        .await
        .expect("lists");
    assert_eq!(unknown, []);
}

async fn a_deletion_is_seen_at_once_and_deleting_again_changes_nothing(store: &dyn WalStore) {
    let orders = pipeline("deleted");
    for number in [0, 1] {
        published(store, &orders, chunk(1, number), b"chunk")
            .await
            .expect("publishes");
    }
    store.remove(&orders, chunk(1, 0)).await.expect("deletes");
    assert_eq!(
        store
            .chunks(&orders, chunk(1, 0).load)
            .await
            .expect("lists"),
        [(1, 5)]
    );
    assert!(store.read(&orders, chunk(1, 0), 0, 5).await.is_err());
    store
        .remove(&orders, chunk(1, 0))
        .await
        .expect("deleting again changes nothing");
    store
        .remove(&orders, chunk(7, 0))
        .await
        .expect("deleting what never was changes nothing");
}

async fn removing_a_log_removes_it_whole_and_nothing_else(store: &dyn WalStore) {
    let (orders, other) = (pipeline("removed"), pipeline("kept"));
    let load = chunk(1, 0).load;
    for number in [0, 1, 2] {
        published(store, &orders, chunk(1, number), b"chunk")
            .await
            .expect("publishes");
    }
    let mut staged = store.stage(&orders, chunk(1, 3)).await.expect("stages");
    staged
        .append(Bytes::from_static(b"staged"))
        .await
        .expect("appends");
    published(store, &orders, chunk(2, 0), b"another load")
        .await
        .expect("publishes");
    published(store, &other, chunk(1, 0), b"another pipeline")
        .await
        .expect("publishes");
    store.remove_log(&orders, load).await.expect("removes");
    assert_eq!(store.chunks(&orders, load).await.expect("lists"), []);
    assert_eq!(
        store.loads(&orders).await.expect("lists"),
        [chunk(2, 0).load]
    );
    assert_eq!(store.chunks(&other, load).await.expect("lists"), [(0, 16)]);
    store
        .remove_log(&orders, load)
        .await
        .expect("removing again changes nothing");
}
