//! What every [`WalStore`] does, as tests any store runs: the memory, local and object stores
//! here, and an object store on a real server in another crate's tests.

use std::io;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::{LoadId, PipelineId};

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

/// Opens `load`'s log of `pipeline` where it was not opened yet.
async fn opened(store: &dyn WalStore, pipeline: &PipelineId, load: LoadId) {
    match store.open_log(pipeline, load).await {
        Err(error) if error.kind() != io::ErrorKind::AlreadyExists => panic!("opens: {error}"),
        _ => {}
    }
}

/// Stages `bytes` as `chunk` of `pipeline`'s log, opening it where it is not, and publishes it.
async fn published(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    chunk: Chunk,
    bytes: &'static [u8],
) -> io::Result<()> {
    opened(store, pipeline, chunk.load).await;
    let mut staged = store.stage(pipeline, chunk).await?;
    for part in bytes.chunks(3) {
        staged.append(Bytes::from_static(part)).await?;
    }
    staged.publish().await
}

/// Runs every check of the contract on `store`, which must hold nothing yet.
///
/// # Panics
///
/// Panics, naming what it found, where the store breaks the contract.
pub async fn conforms(store: &dyn WalStore) {
    a_chunk_is_staged_only_in_a_log_opened_once(store).await;
    no_chunk_is_published_once_its_log_is_removed(store).await;
    what_was_staged_and_deleted_is_never_published(store).await;
    a_publish_racing_a_removal_is_never_left_behind(store).await;
    a_staged_chunk_is_seen_by_no_reader_until_published(store).await;
    a_published_chunk_is_whole_read_by_range_and_never_replaced(store).await;
    the_first_of_two_chunks_of_one_name_published_is_kept(store).await;
    chunks_and_loads_list_in_order_whatever_order_they_came_in(store).await;
    a_deletion_is_seen_at_once_and_deleting_again_changes_nothing(store).await;
    removing_a_log_removes_it_whole_and_nothing_else(store).await;
}

async fn a_chunk_is_staged_only_in_a_log_opened_once(store: &dyn WalStore) {
    let orders = pipeline("opened");
    let at = chunk(1, 0);
    let refused = store
        .stage(&orders, at)
        .await
        .err()
        .expect("no log is open");
    assert_eq!(refused.kind(), io::ErrorKind::NotFound, "{refused}");
    store.open_log(&orders, at.load).await.expect("opens");
    assert_eq!(store.loads(&orders).await.expect("lists"), [at.load]);
    assert_eq!(store.chunks(&orders, at.load).await.expect("lists"), []);
    let again = store
        .open_log(&orders, at.load)
        .await
        .expect_err("opened once");
    assert_eq!(again.kind(), io::ErrorKind::AlreadyExists, "{again}");
    published(store, &orders, at, b"chunk")
        .await
        .expect("publishes");
    store.remove_log(&orders, at.load).await.expect("removes");
    let refused = store
        .stage(&orders, chunk(1, 1))
        .await
        .err()
        .expect("removed");
    assert_eq!(refused.kind(), io::ErrorKind::NotFound, "{refused}");
}

async fn no_chunk_is_published_once_its_log_is_removed(store: &dyn WalStore) {
    let orders = pipeline("fenced");
    let load = chunk(1, 0).load;
    published(store, &orders, chunk(1, 0), b"first")
        .await
        .expect("publishes");
    // A writer stalls with a chunk staged while its log is taken over and removed.
    let mut stalled = store.stage(&orders, chunk(1, 1)).await.expect("stages");
    stalled
        .append(Bytes::from_static(b"late"))
        .await
        .expect("appends");
    store.remove_log(&orders, load).await.expect("removes");
    let refused = stalled.publish().await.expect_err("the log is gone");
    assert_eq!(refused.kind(), io::ErrorKind::NotFound, "{refused}");
    assert_eq!(store.loads(&orders).await.expect("lists"), []);
    assert_eq!(store.leftovers(&orders).await.expect("lists"), []);
    assert_eq!(store.chunks(&orders, load).await.expect("lists"), []);
    assert!(store.read(&orders, chunk(1, 1), 0, 4).await.is_err());
}

async fn what_was_staged_and_deleted_is_never_published(store: &dyn WalStore) {
    let orders = pipeline("unstaged");
    let load = chunk(1, 0).load;
    opened(store, &orders, load).await;
    let mut discarded = store.stage(&orders, chunk(1, 0)).await.expect("stages");
    discarded
        .append(Bytes::from_static(b"failed"))
        .await
        .expect("appends");
    discarded.discard().await.expect("discards");
    // A load that crashed left a staging, which a replay deletes before it fences the log.
    let mut crashed = store.stage(&orders, chunk(1, 0)).await.expect("stages");
    crashed
        .append(Bytes::from_static(b"crashed"))
        .await
        .expect("appends");
    store
        .remove_staged(&orders, load)
        .await
        .expect("deletes what was staged");
    let refused = crashed.publish().await.expect_err("its staging is gone");
    assert_eq!(refused.kind(), io::ErrorKind::NotFound, "{refused}");
    assert_eq!(store.chunks(&orders, load).await.expect("lists"), []);
    published(store, &orders, chunk(1, 0), b"after")
        .await
        .expect("a staging begun after publishes");
}

/// Publishes run alongside the removal of their log: each is refused, or goes with the log; none
/// is left behind it, as a store that asked whether the log is open before it created the chunk
/// would leave one.
///
/// # Panics
///
/// Panics where a publish is left behind its log's removal.
pub async fn a_publish_racing_a_removal_is_never_left_behind(store: &dyn WalStore) {
    let orders = pipeline("racing");
    for round in 0..16 {
        let load = chunk(100 + round, 0).load;
        published(store, &orders, chunk(100 + round, 0), b"first")
            .await
            .expect("publishes");
        let mut stagings = Vec::new();
        for number in 1..=4 {
            let at = Chunk { load, number };
            let mut staged = store.stage(&orders, at).await.expect("stages");
            staged
                .append(Bytes::from_static(b"racing"))
                .await
                .expect("appends");
            stagings.push(staged);
        }
        let mut stagings = stagings.into_iter();
        let mut next = || stagings.next().expect("four stagings").publish();
        // The removal begins once the first publishes have begun.
        let removal = async {
            tokio::task::yield_now().await;
            store.remove_log(&orders, load).await
        };
        let (one, two, removed, three, four) =
            tokio::join!(next(), next(), removal, next(), next());
        removed.expect("removes");
        for published in [one, two, three, four] {
            if let Err(refused) = published {
                assert_eq!(refused.kind(), io::ErrorKind::NotFound, "{refused}");
            }
        }
        assert_eq!(store.chunks(&orders, load).await.expect("lists"), []);
        let left = store.leftovers(&orders).await.expect("lists");
        assert!(!left.contains(&load), "round {round}: {left:?}");
    }
}

async fn a_staged_chunk_is_seen_by_no_reader_until_published(store: &dyn WalStore) {
    let orders = pipeline("staged");
    let at = chunk(1, 0);
    opened(store, &orders, at.load).await;
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
    opened(store, &orders, at.load).await;
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
