//! Taking a log over for replay: a fence published as the log's next chunk, which its load, if
//! it still runs, then finds taken when it publishes, so it adds nothing to what is replayed.

use std::io;

use bytes::Bytes;
use rdlt_connector::{LoadId, PipelineId};

use crate::crash::crash_point;
use crate::error::Error;
use crate::wal::frame::{self, End, Fence, Frame};
use crate::wal::scan;
use crate::wal::{Chunk, WalStore};

/// How many times a replay tries to fence a log whose load publishes a chunk first, before it
/// leaves the log to a later replay: a load still running publishes one a commit.
const TRIES: usize = 8;

/// What a replay may do with a log.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Taken {
    /// The log needs nothing: its load closed it, or a replay released it.
    Finished,
    /// The replay fenced the log with chunk `number`, and replays it.
    Fenced { number: u64 },
    /// Its load kept publishing: it runs, and the log is left to a later replay.
    Running,
}

/// Takes `load`'s log of `pipeline` in `store` over, its chunks read for frames of at most
/// `frame_bytes`.
pub(super) async fn take(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    frame_bytes: u64,
) -> Result<Taken, Error> {
    for _ in 0..TRIES {
        let tail = scan::tail(store, pipeline, load, frame_bytes).await?;
        let (number, end) = match tail {
            Some(tail) if tail.needed().is_empty() => return Ok(Taken::Finished),
            Some(tail) => {
                let end = End {
                    live: tail.needed(),
                    received: tail.end.received.clone(),
                };
                (tail.number + 1, end)
            }
            None => (0, End::default()),
        };
        if fence(store, pipeline, Chunk { load, number }, end).await? {
            crash_point!("engine.replay.fenced");
            return Ok(if number == 0 {
                Taken::Finished
            } else {
                Taken::Fenced { number }
            });
        }
    }
    Ok(Taken::Running)
}

/// Releases `load`'s log, which the replay fenced with chunk `number`: a fence after it needing
/// nothing; whether the replay was the last to take the log, and may delete it.
pub(super) async fn release(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    number: u64,
) -> Result<bool, Error> {
    let chunk = Chunk {
        load,
        number: number + 1,
    };
    fence(store, pipeline, chunk, End::default()).await
}

/// Publishes a fence as `chunk` of `pipeline`'s log, `end` saying what of the log is needed;
/// whether the chunk was free.
async fn fence(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    chunk: Chunk,
    end: End,
) -> Result<bool, Error> {
    let mut bytes = frame::preamble().to_vec();
    let fence = Frame::Fence(Fence {
        pipeline: pipeline.clone(),
        load: chunk.load,
        chunk: chunk.number,
    });
    bytes.extend_from_slice(&fence.encode()?);
    bytes.extend_from_slice(&Frame::End(end).encode()?);
    let mut staged = store
        .stage(pipeline, chunk)
        .await
        .map_err(Error::from_wal)?;
    staged
        .append(Bytes::from(bytes))
        .await
        .map_err(Error::from_wal)?;
    match staged.publish().await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(Error::from_wal(error)),
    }
}
