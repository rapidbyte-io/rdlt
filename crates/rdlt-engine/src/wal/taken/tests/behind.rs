//! A replay whose listing the log's load outruns before the replay's fence is published.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

use super::super::super::memory::MemoryWal;
use super::super::super::store::{Chunk, StagedChunk, WalStore};
use super::super::{Taken, take};
use super::{Busy, chunk, load, pipeline};

/// A store whose log's load, as the replay stages its first fence, publishes its chunks 0 and 1
/// and deletes chunk 0, which chunk 1 no longer needs: the replay listed the log before any.
#[derive(Debug, Default)]
struct Behind {
    inner: MemoryWal,
    outran: AtomicBool,
}

impl Behind {
    /// The load publishes chunks 0 and 1, then deletes chunk 0.
    async fn outrun(&self, pipeline: &PipelineId) -> io::Result<()> {
        for number in [0, 1] {
            let mut written = self.inner.stage(pipeline, chunk(number)).await?;
            written.append(Busy::written(number)).await?;
            written.publish().await?;
        }
        self.inner.remove(pipeline, chunk(0)).await
    }
}

impl WalStore for Behind {
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
            if !self.outran.swap(true, Ordering::SeqCst) {
                self.outrun(pipeline).await?;
            }
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
async fn a_fence_below_the_load_s_last_chunk_fences_nothing_and_is_taken_past_it() {
    let store = Behind::default();
    store.inner.open(&pipeline(), load());
    // The replay listed no chunk and fences at 0, a number the load freed as it went on.
    let taken = take(&store, &pipeline(), load(), 1 << 20)
        .await
        .expect("takes");
    assert_eq!(taken, Taken::Finished);
    let chunks: Vec<u64> = store
        .chunks(&pipeline(), load())
        .await
        .expect("lists")
        .into_iter()
        .map(|(number, _)| number)
        .collect();
    // The fence is the load's next chunk, and the fence below its last chunk is gone.
    assert_eq!(chunks, [1, 2]);
}
