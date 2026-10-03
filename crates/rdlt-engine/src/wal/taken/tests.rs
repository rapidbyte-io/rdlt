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
        });
        for frame in [header, Frame::Closed, Frame::End(End::default())] {
            bytes.extend_from_slice(&frame.encode().expect("encodes"));
        }
        Bytes::from(bytes)
    }
}

impl WalStore for Busy {
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

fn chunk(number: u64) -> Chunk {
    Chunk {
        load: load(),
        number,
    }
}
