//! A write-ahead log store that counts what it is asked to do, over a local one, and logs
//! rewritten as whoever can write their directory rewrites them.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use rdlt_connector::{BoxFuture, LoadId, PipelineId};
use rdlt_engine::{Chunk, LocalWal, StagedChunk, WalStore};

/// A [`LocalWal`] counting the bytes appended to the chunks it stages, and the chunks published.
#[derive(Debug)]
pub(crate) struct Counted {
    pub(crate) local: LocalWal,
    pub(crate) appends: Arc<AtomicUsize>,
    pub(crate) publishes: Arc<AtomicUsize>,
}

/// A chunk a [`Counted`] stages, counting as it goes.
struct Counting {
    staged: Box<dyn StagedChunk>,
    appends: Arc<AtomicUsize>,
    publishes: Arc<AtomicUsize>,
}

impl StagedChunk for Counting {
    fn append(&mut self, bytes: Bytes) -> BoxFuture<'_, io::Result<()>> {
        self.appends.fetch_add(1, Ordering::SeqCst);
        self.staged.append(bytes)
    }

    fn publish(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        self.publishes.fetch_add(1, Ordering::SeqCst);
        self.staged.publish()
    }

    fn discard(self: Box<Self>) -> BoxFuture<'static, io::Result<()>> {
        self.staged.discard()
    }
}

impl Counted {
    pub(crate) fn new(local: LocalWal) -> Self {
        Self {
            local,
            appends: Arc::default(),
            publishes: Arc::default(),
        }
    }

    pub(crate) fn appends(&self) -> usize {
        self.appends.load(Ordering::SeqCst)
    }

    pub(crate) fn publishes(&self) -> usize {
        self.publishes.load(Ordering::SeqCst)
    }
}

impl WalStore for Counted {
    fn identity(&self, proposed: LoadId) -> BoxFuture<'_, io::Result<LoadId>> {
        self.local.identity(proposed)
    }
    fn open_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.local.open_log(pipeline, load)
    }

    fn stage<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<Box<dyn StagedChunk>>> {
        Box::pin(async move {
            let staged = self.local.stage(pipeline, chunk).await?;
            Ok(Box::new(Counting {
                staged,
                appends: Arc::clone(&self.appends),
                publishes: Arc::clone(&self.publishes),
            }) as Box<dyn StagedChunk>)
        })
    }

    fn loads<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        self.local.loads(pipeline)
    }

    fn leftovers<'a>(&'a self, pipeline: &'a PipelineId) -> BoxFuture<'a, io::Result<Vec<LoadId>>> {
        self.local.leftovers(pipeline)
    }

    fn chunks<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<Vec<(u64, u64)>>> {
        self.local.chunks(pipeline, load)
    }

    fn read<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'a, io::Result<Bytes>> {
        self.local.read(pipeline, chunk, offset, len)
    }

    fn remove_staged<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.local.remove_staged(pipeline, load)
    }

    fn remove<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        chunk: Chunk,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.local.remove(pipeline, chunk)
    }

    fn remove_log<'a>(
        &'a self,
        pipeline: &'a PipelineId,
        load: LoadId,
    ) -> BoxFuture<'a, io::Result<()>> {
        self.local.remove_log(pipeline, load)
    }
}

/// Rewrites every chunk of the logs under `base`, each frame's payload as `rewrite` says of its
/// kind, its checksum made to match again, as whoever can write the logs' directory can.
pub(crate) fn rewritten(
    base: &std::path::Path,
    rewrite: &dyn Fn(u8, &mut serde_json::Value),
) -> usize {
    let mut chunks = 0;
    for entry in std::fs::read_dir(base).expect("a directory") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            chunks += rewritten(&path, rewrite);
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("wal") {
            continue;
        }
        let bytes = std::fs::read(&path).expect("a chunk");
        let mut out = bytes[..14].to_vec();
        let mut at = 14;
        while at + 9 <= bytes.len() {
            let kind = bytes[at];
            let len = usize::try_from(u32::from_le_bytes(
                bytes[at + 1..at + 5].try_into().expect("four bytes"),
            ))
            .expect("a length");
            let mut payload = bytes[at + 9..at + 9 + len].to_vec();
            if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&payload) {
                rewrite(kind, &mut value);
                payload = serde_json::to_vec(&value).expect("JSON encodes");
            }
            let len = u32::try_from(payload.len())
                .expect("a short payload")
                .to_le_bytes();
            let mut head = vec![kind];
            head.extend_from_slice(&len);
            let check = crc32c::crc32c_append(crc32c::crc32c(&head), &payload);
            out.extend_from_slice(&head);
            out.extend_from_slice(&check.to_le_bytes());
            out.extend_from_slice(&payload);
            at += 9 + len_of(&bytes[at..]);
        }
        std::fs::write(&path, out).expect("the rewritten chunk");
        chunks += 1;
    }
    chunks
}

/// The length of the payload of the frame `bytes` starts with.
fn len_of(bytes: &[u8]) -> usize {
    usize::try_from(u32::from_le_bytes(
        bytes[1..5].try_into().expect("four bytes"),
    ))
    .expect("a length")
}
