use std::io;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::{BoxFuture, Epoch, LoadId, PipelineId};

use super::super::frame::{self, End, Frame, Header};
use super::super::memory::MemoryWal;
use super::super::store::{Chunk, StagedChunk, WalStore};
use super::{Taken, take};

fn pipeline() -> PipelineId {
    PipelineId::parse("orders").expect("a valid pipeline")
}

fn load() -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, 5)
}

/// A store whose log's load publishes the chunk a replay stages before the replay can, as a load
/// that commits faster than it is fenced does.
#[derive(Debug, Default)]
struct Busy {
    inner: MemoryWal,
}

impl Busy {
    /// The load's chunk `number`: its header, its close and its end.
    fn written(number: u64) -> Bytes {
        let mut bytes = frame::preamble().to_vec();
        let header = Frame::Header(Header {
            pipeline: pipeline(),
            load: load(),
            chunk: number,
            epoch: Epoch(1),
            opened: None,
            origin: load(),
        });
        for frame in [header, Frame::Closed, Frame::End(End::default())] {
            bytes.extend_from_slice(&frame.encode().expect("encodes"));
        }
        Bytes::from(bytes)
    }
}

impl WalStore for Busy {
    fn identity(&self, proposed: LoadId) -> BoxFuture<'_, io::Result<LoadId>> {
        self.inner.identity(proposed)
    }
    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.open_log(pipeline, load)
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        Box::pin(async move {
            let mut written = self.inner.stage(pipeline, chunk).await?;
            written.append(Self::written(chunk.number)).await?;
            written.publish().await?;
            self.inner.stage(pipeline, chunk).await
        })
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        self.inner.loads(pipeline)
    }

    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        self.inner.leftovers(pipeline)
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        self.inner.chunks(pipeline, load)
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        self.inner.read(pipeline, chunk, offset, len)
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.remove_staged(pipeline, load)
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.remove(pipeline, chunk)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.remove_log(pipeline, load)
    }
}

#[tokio::test]
async fn a_log_whose_load_keeps_publishing_is_never_taken_as_fenced() {
    let store = Busy::default();
    store.inner.open(&pipeline(), load());
    let taken = take(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect("reads");
    assert_eq!(taken, Taken::Running);
}

#[tokio::test]
async fn a_log_that_needs_nothing_is_fenced_before_it_is_called_finished() {
    let store = MemoryWal::default();
    store.open(&pipeline(), load());
    let mut written = store.stage(&pipeline(), chunk(0)).await.expect("stages");
    written.append(Busy::written(0)).await.expect("appends");
    written.publish().await.expect("publishes");
    let taken = take(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect("reads");
    assert_eq!(taken, Taken::Finished);
    // The load's next chunk is taken: had the load committed after its close, it finds it so.
    let chunks: Vec<u64> = store
        .chunks(&pipeline(), load())
        .await
        .expect("lists")
        .into_iter()
        .map(|(number, _)| number)
        .collect();
    assert_eq!(chunks, [0, 1]);
}

#[tokio::test]
async fn a_disk_a_crashed_load_filled_is_freed_before_its_log_is_fenced() {
    let store = MemoryWal {
        disk: std::sync::Arc::new(super::super::memory::Disk::of(4_096)),
        ..MemoryWal::default()
    };
    store.open(&pipeline(), load());
    let mut written = store.stage(&pipeline(), chunk(0)).await.expect("stages");
    written.append(Busy::written(0)).await.expect("appends");
    written.publish().await.expect("publishes");
    // The load staged its next chunk until the disk was full, and crashed.
    let mut crashed = store.stage(&pipeline(), chunk(1)).await.expect("stages");
    while crashed.append(Bytes::from_static(&[0; 64])).await.is_ok() {}
    drop(crashed);
    let taken = take(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect("the fence finds room");
    assert_eq!(taken, Taken::Finished);
    assert_eq!(store.disk.staged(), 0);
}

#[tokio::test]
async fn a_log_removed_as_it_is_read_is_gone_and_one_damaged_is_refused() {
    let store = MemoryWal::default();
    store.open(&pipeline(), load());
    let mut written = store.stage(&pipeline(), chunk(0)).await.expect("stages");
    written.append(Busy::written(0)).await.expect("appends");
    written.publish().await.expect("publishes");
    let mut damaged = store.stage(&pipeline(), chunk(1)).await.expect("stages");
    damaged
        .append(Bytes::from_static(b"no chunk"))
        .await
        .expect("appends");
    damaged.publish().await.expect("publishes");
    let Err(refused) = super::scanned(&store, &pipeline(), load(), 1 << 20).await else {
        panic!("an open log that does not read");
    };
    assert_eq!(refused.code(), Some("wal_unreadable"));
    store
        .remove_log(&pipeline(), load())
        .await
        .expect("removes");
    let gone = super::scanned(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect("a log another replay removed");
    assert!(gone.is_none());
}

fn chunk(number: u64) -> Chunk {
    Chunk {
        load: load(),
        number,
    }
}

#[tokio::test]
async fn a_fence_the_disk_has_no_room_for_leaves_nothing_staged() {
    let store = MemoryWal {
        disk: std::sync::Arc::new(super::super::memory::Disk::of(0)),
        ..MemoryWal::default()
    };
    store.open(&pipeline(), load());
    let refused = take(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect_err("no room for a fence");
    assert_eq!(refused.code(), Some("wal_storage_full"), "{refused}");
    assert_eq!(store.disk.stagings(), 0, "the fence's staging is gone");
}

#[tokio::test]
async fn a_log_another_replay_removed_since_it_was_listed_is_gone_not_a_failure() {
    let store = MemoryWal::default();
    store.open(&pipeline(), load());
    let mut written = store.stage(&pipeline(), chunk(0)).await.expect("stages");
    written.append(Busy::written(0)).await.expect("appends");
    written.publish().await.expect("publishes");
    // Two replays listed the log; the first takes it and removes it.
    assert_eq!(store.loads(&pipeline()).await.expect("lists"), [load()]);
    let first = take(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect("takes");
    assert_eq!(first, Taken::Finished);
    store
        .remove_log(&pipeline(), load())
        .await
        .expect("removes");
    let second = take(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect("no failure");
    assert_eq!(second, Taken::Gone);
}

/// A store in which a rival replay deletes every staging of the log just after it is begun.
#[derive(Debug, Default)]
struct Rival {
    inner: MemoryWal,
}

impl WalStore for Rival {
    fn identity(&self, proposed: LoadId) -> BoxFuture<'_, io::Result<LoadId>> {
        self.inner.identity(proposed)
    }
    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.open_log(pipeline, load)
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        Box::pin(async move {
            let staged = self.inner.stage(pipeline, chunk).await?;
            self.inner.remove_staged(pipeline, chunk.load).await?;
            Ok(staged)
        })
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        self.inner.loads(pipeline)
    }

    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        self.inner.leftovers(pipeline)
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        self.inner.chunks(pipeline, load)
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        self.inner.read(pipeline, chunk, offset, len)
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.remove_staged(pipeline, load)
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.remove(pipeline, chunk)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.inner.remove_log(pipeline, load)
    }
}

#[tokio::test]
async fn a_fence_whose_staging_a_rival_deletes_is_tried_again_and_never_fails_untyped() {
    let store = Rival::default();
    store.inner.open(&pipeline(), load());
    let mut written = store
        .inner
        .stage(&pipeline(), chunk(0))
        .await
        .expect("stages");
    written.append(Busy::written(0)).await.expect("appends");
    written.publish().await.expect("publishes");
    let taken = take(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect("no failure");
    assert_eq!(
        taken,
        Taken::Running,
        "a rival that never stops holds the log"
    );
    // A release whose staging a rival deletes leaves the log to the rival.
    let released = super::release(&store, &pipeline(), load(), 3)
        .await
        .expect("no failure");
    assert!(!released);
}
