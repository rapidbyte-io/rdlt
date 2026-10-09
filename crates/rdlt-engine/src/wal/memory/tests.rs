use std::io;
use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};

use super::MemoryWal;
use crate::conformance;
use crate::wal::store::{Chunk, StagedChunk, WalStore};

#[tokio::test]
async fn a_log_in_memory_keeps_the_store_s_contract() {
    conformance::conforms(&MemoryWal::default()).await;
}

/// How a [`Flawed`] store breaks the contract when it publishes.
#[derive(Clone, Copy, Debug, Default)]
enum Flaw {
    /// It asks whether a log is open before it creates the chunk, and creates it after a few
    /// turns of the scheduler whatever happened meanwhile.
    #[default]
    Hasty,
    /// It takes a chunk whose name holds the same bytes for its own, as a store that compares
    /// what it reads back would.
    TakesAlike,
}

/// A store in memory but for its publishes, which `flaw` breaks.
#[derive(Debug, Default)]
struct Flawed {
    inner: MemoryWal,
    flaw: Flaw,
}

struct FlawedStaged {
    inner: Arc<parking_lot::Mutex<std::collections::BTreeMap<(PipelineId, Chunk), Bytes>>>,
    logs: super::Logs,
    key: (PipelineId, Chunk),
    bytes: Vec<u8>,
    flaw: Flaw,
}

impl StagedChunk for FlawedStaged {
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
            if let Flaw::TakesAlike = self.flaw {
                let mut chunks = self.inner.lock();
                return match chunks.get(&self.key) {
                    Some(held) if held[..] == self.bytes[..] => Ok(()),
                    Some(_) => Err(io::Error::from(io::ErrorKind::AlreadyExists)),
                    None => {
                        chunks.insert(self.key, Bytes::from(self.bytes));
                        Ok(())
                    }
                };
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

impl WalStore for Flawed {
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
        let staged = FlawedStaged {
            inner: Arc::clone(&self.inner.chunks),
            logs: Arc::clone(&self.inner.logs),
            key: (pipeline.clone(), chunk),
            bytes: Vec::new(),
            flaw: self.flaw,
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
    let store = Flawed::default();
    let raced = crate::scope::contained(
        conformance::a_publish_racing_a_removal_is_never_left_behind(&store),
    )
    .await;
    assert!(raced.is_err(), "the suite let it pass");
}

#[tokio::test]
async fn the_contract_refuses_a_store_that_takes_a_chunk_of_the_same_bytes_for_its_own() {
    let store = Flawed {
        flaw: Flaw::TakesAlike,
        ..Flawed::default()
    };
    let alike = crate::scope::contained(
        conformance::the_first_of_two_chunks_of_one_name_published_is_kept(&store),
    )
    .await;
    assert!(alike.is_err(), "the suite let it pass");
}

fn orders() -> PipelineId {
    PipelineId::parse("orders").expect("a valid pipeline")
}

fn load(n: u128) -> LoadId {
    LoadId::from_parts(std::time::UNIX_EPOCH, n)
}

#[tokio::test]
async fn every_call_fails_as_the_store_is_told_to_while_it_is_failing_or_interrupted() {
    let (pipeline, chunk) = (
        orders(),
        Chunk {
            load: load(1),
            number: 0,
        },
    );
    for (interrupted, expected) in [
        (false, io::ErrorKind::Other),
        (true, io::ErrorKind::Interrupted),
    ] {
        let store = MemoryWal::default();
        store.open(&pipeline, load(1));
        let fault = if interrupted {
            &store.interrupted
        } else {
            &store.failing
        };
        *fault.lock() = true;
        let failed = |result: io::Result<()>| result.expect_err("the call fails").kind();
        let calls = [
            failed(store.open_log(&pipeline, load(2)).await),
            failed(store.stage(&pipeline, chunk).await.map(drop)),
            failed(store.loads(&pipeline).await.map(drop)),
            failed(store.leftovers(&pipeline).await.map(drop)),
            failed(store.chunks(&pipeline, load(1)).await.map(drop)),
            failed(store.read(&pipeline, chunk, 0, 1).await.map(drop)),
            failed(store.remove_staged(&pipeline, load(1)).await),
            failed(store.remove(&pipeline, chunk).await),
            failed(store.remove_log(&pipeline, load(1)).await),
        ];
        assert_eq!(calls, [expected; 9]);
    }
}

#[tokio::test]
async fn leftovers_are_the_logs_no_longer_open_that_still_hold_chunks() {
    let (pipeline, store) = (orders(), MemoryWal::default());
    // An open log holding a chunk, and one a removal a crash cut short left closed with two.
    store.open(&pipeline, load(1));
    let mut staged = store
        .stage(
            &pipeline,
            Chunk {
                load: load(1),
                number: 0,
            },
        )
        .await
        .expect("stages");
    staged
        .append(Bytes::from_static(b"held"))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
    store.logs.lock().insert((pipeline.clone(), load(2)), false);
    for number in [0, 1] {
        let left = (
            pipeline.clone(),
            Chunk {
                load: load(2),
                number,
            },
        );
        store
            .chunks
            .lock()
            .insert(left, Bytes::from_static(b"left"));
    }
    let leftovers = store.leftovers(&pipeline).await.expect("lists");
    assert_eq!(leftovers, [load(2)]);
}

#[tokio::test]
async fn a_store_takes_what_exactly_fills_its_capacity_and_refuses_a_byte_more() {
    let (pipeline, store) = (
        orders(),
        MemoryWal {
            disk: Arc::new(super::Disk::of(10)),
            ..MemoryWal::default()
        },
    );
    store.open(&pipeline, load(1));
    let chunk = |number| Chunk {
        load: load(1),
        number,
    };
    let mut published = store.stage(&pipeline, chunk(0)).await.expect("stages");
    published
        .append(Bytes::from_static(&[0; 4]))
        .await
        .expect("appends");
    published.publish().await.expect("publishes");
    let mut held = store.stage(&pipeline, chunk(1)).await.expect("stages");
    held.append(Bytes::from_static(&[0; 3]))
        .await
        .expect("appends");
    // Four bytes published and three staged leave room for three more, and no more.
    let mut filling = store.stage(&pipeline, chunk(2)).await.expect("stages");
    filling
        .append(Bytes::from_static(&[0; 3]))
        .await
        .expect("the store has room for it");
    let refused = filling
        .append(Bytes::from_static(&[0; 1]))
        .await
        .expect_err("the store is full");
    assert_eq!(refused.kind(), io::ErrorKind::StorageFull);
    assert_eq!(store.disk.staged(), 6);
}
