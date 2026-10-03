use std::io;
use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

use super::MemoryWal;
use crate::wal::store::{Chunk, StagedChunk, WalStore, conformance};

#[tokio::test]
async fn a_log_in_memory_keeps_the_store_s_contract() {
    conformance::conforms(&MemoryWal::default()).await;
}

/// A store that asks whether a log is open before it creates the chunk, and creates it after a
/// few turns of the scheduler whatever happened meanwhile.
#[derive(Debug, Default)]
struct Hasty {
    inner: MemoryWal,
}

struct HastyStaged {
    inner: Arc<parking_lot::Mutex<std::collections::BTreeMap<(PipelineId, Chunk), Bytes>>>,
    logs: super::Logs,
    key: (PipelineId, Chunk),
    bytes: Vec<u8>,
}

impl StagedChunk for HastyStaged {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        self.bytes.extend_from_slice(&bytes);
        Box::pin(async { Ok(()) })
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        Box::pin(async move {
            let open = self.logs.lock().get(&(self.key.0.clone(), self.key.1.load)) == Some(&true);
            if !open {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            self.inner.lock().insert(self.key, Bytes::from(self.bytes));
            Ok(())
        })
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl WalStore for Hasty {
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
        let staged = HastyStaged {
            inner: Arc::clone(&self.inner.chunks),
            logs: Arc::clone(&self.inner.logs),
            key: (pipeline.clone(), chunk),
            bytes: Vec::new(),
        };
        Box::pin(async { Ok(Box::new(staged) as Box<dyn StagedChunk>) })
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
async fn the_contract_refuses_a_store_that_asks_whether_a_log_is_open_before_it_publishes() {
    let store = Hasty::default();
    let raced = crate::scope::contained(
        conformance::a_publish_racing_a_removal_is_never_left_behind(&store),
    )
    .await;
    assert!(raced.is_err(), "the suite let it pass");
}
