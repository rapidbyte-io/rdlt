//! Taking a log over for replay: a fence published as the log's next chunk, which its load, if
//! it still runs, then finds taken when it publishes, so it adds nothing to what is replayed; once
//! the log is removed, the load finds it gone.

#[cfg(test)]
mod tests;

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

/// What a replay may do with a log, once it is fenced.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Taken {
    /// The log needs nothing: its load closed it, a replay released it, or it holds no chunk;
    /// it is removed.
    Finished,
    /// The replay fenced the log with chunk `number`, and replays it.
    Fenced { number: u64 },
    /// Its load kept publishing, or a rival replay kept deleting the fence before it was
    /// published, so the log is not fenced: another attempt holds it.
    Running,
    /// Another replay removed the log since it was listed, having replayed what it needed.
    Gone,
}

/// Checks that `ours`, the store the attempt keeps its log in, is `named`, the store the
/// destination names for the pipeline's logs, where both are known.
///
/// # Errors
///
/// `wal_store_other` where they differ: another store may hold logs of the pipeline that no
/// replay of this one sees, and a load that read on would move past rows they hold.
pub(crate) fn one_store(ours: Option<LoadId>, named: Option<LoadId>) -> Result<(), Error> {
    match (ours, named) {
        (Some(ours), Some(named)) if ours != named => Err(Error::wal_store_other(ours, named)),
        _ => Ok(()),
    }
}

/// Opens `load`'s own log of `pipeline` in `store`, before it reads any other.
///
/// A disk too full for it is given back what loads of the pipeline staged and did not publish,
/// and what removals a crash cut short left, which needs no room, and the log opened once more:
/// a load that crashed filling the disk leaves no attempt after it unable to begin. A load whose
/// staging goes so finds it gone, as a take would make it find.
///
/// # Errors
///
/// `wal_storage_full` where the disk is full still; `wal_running`, retryably, where another
/// attempt's replay took the log as it was opened.
pub(crate) async fn open_own(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
) -> Result<LoadId, Error> {
    let identity = store.identity(load).await.map_err(Error::from_wal)?;
    let opened = store.open_log(pipeline, load).await;
    let opened = match opened {
        Err(error) if full(&error) => {
            for leftover in store.leftovers(pipeline).await.map_err(Error::from_wal)? {
                store
                    .remove_log(pipeline, leftover)
                    .await
                    .map_err(Error::from_wal)?;
            }
            for other in store.loads(pipeline).await.map_err(Error::from_wal)? {
                store
                    .remove_staged(pipeline, other)
                    .await
                    .map_err(Error::from_wal)?;
            }
            store.open_log(pipeline, load).await
        }
        opened => opened,
    };
    opened.map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => Error::wal_opening_taken(load),
        _ => Error::from_wal(error),
    })?;
    Ok(identity)
}

/// Whether `error` says the disk, or the user's share of it, is full.
fn full(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    )
}

/// Whether a fence was published.
enum Fenced {
    /// It was: the replay holds the log.
    Published,
    /// The name was taken first, or a rival replay deleted the fence's staging: the log is
    /// taken again.
    Lost,
    /// The log is no longer open.
    Gone,
}

/// Takes `load`'s log of `pipeline` in `store` over, its chunks read for frames of at most
/// `frame_bytes`.
///
/// What the log's load staged and did not publish is deleted first, which takes no room: a load
/// that crashed with a disk full gives it back, so the fence fits, and one still running finds
/// its staging gone. A log that another replay removes meanwhile is [`Taken::Gone`], whatever
/// failed of the take as it went.
pub(crate) async fn take(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    frame_bytes: u64,
) -> Result<Taken, Error> {
    let taken = fenced(store, pipeline, load, frame_bytes).await;
    if matches!(taken, Err(_) | Ok(Taken::Gone)) && !open(store, pipeline, load).await? {
        return Ok(Taken::Gone);
    }
    taken
}

/// What `load`'s log of `pipeline` in `store` holds, read as [`scan::scan`] reads it; none where
/// another replay removed the log as it was read, which leaves it to that replay.
///
/// # Errors
///
/// As [`scan::scan`], for a log still open.
pub(crate) async fn scanned(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    frame_bytes: u64,
) -> Result<Option<scan::Scanned>, Error> {
    let scanned = scan::scan(store, pipeline, load, frame_bytes).await;
    if !open(store, pipeline, load).await? {
        return Ok(None);
    }
    scanned.map(Some)
}

/// Whether `load`'s log of `pipeline` in `store` is still open.
pub(crate) async fn open(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
) -> Result<bool, Error> {
    let loads = store.loads(pipeline).await.map_err(Error::from_wal)?;
    Ok(loads.contains(&load))
}

/// Takes the log over as [`take`] does, a log found gone or failing as it went left to it.
async fn fenced(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    frame_bytes: u64,
) -> Result<Taken, Error> {
    store
        .remove_staged(pipeline, load)
        .await
        .map_err(Error::from_wal)?;
    for _ in 0..TRIES {
        let tail = scan::tail(store, pipeline, load, frame_bytes).await?;
        // A log that needs nothing is fenced all the same: its load may still publish a commit
        // after its last chunk, which a removal that went first would drop.
        let (number, end) = match tail {
            Some(tail) => {
                let end = End {
                    live: tail.needed(),
                    received: tail.end.received.clone(),
                };
                (tail.number + 1, end)
            }
            None => (0, End::default()),
        };
        let finished = end.live.is_empty();
        match fence(store, pipeline, Chunk { load, number }, end).await? {
            Fenced::Published => {
                crash_point!("engine.replay.fenced");
                return Ok(if finished {
                    Taken::Finished
                } else {
                    Taken::Fenced { number }
                });
            }
            Fenced::Lost => {}
            Fenced::Gone => return Ok(Taken::Gone),
        }
    }
    Ok(Taken::Running)
}

/// Releases `load`'s log, which the replay fenced with chunk `number`: a fence after it needing
/// nothing; whether the replay was the last to take the log, and may delete it.
///
/// A log another replay took or removed since is left to it.
pub(crate) async fn release(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    number: u64,
) -> Result<bool, Error> {
    let chunk = Chunk {
        load,
        number: number + 1,
    };
    let fenced = fence(store, pipeline, chunk, End::default()).await?;
    Ok(matches!(fenced, Fenced::Published))
}

/// Publishes a fence as `chunk` of `pipeline`'s log, `end` saying what of the log is needed.
async fn fence(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    chunk: Chunk,
    end: End,
) -> Result<Fenced, Error> {
    let mut bytes = frame::preamble().to_vec();
    let fence = Frame::Fence(Fence {
        pipeline: pipeline.clone(),
        load: chunk.load,
        chunk: chunk.number,
    });
    bytes.extend_from_slice(&fence.encode()?);
    bytes.extend_from_slice(&Frame::End(end).encode()?);
    let mut staged = match store.stage(pipeline, chunk).await {
        Ok(staged) => staged,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Fenced::Gone),
        Err(error) => return Err(Error::from_wal(error)),
    };
    if let Err(error) = staged.append(Bytes::from(bytes)).await {
        // A fence that failed to write gives back what it staged.
        drop(staged.discard().await);
        return missing(store, pipeline, chunk.load, error).await;
    }
    match staged.publish().await {
        Ok(()) => Ok(Fenced::Published),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(Fenced::Lost),
        Err(error) => missing(store, pipeline, chunk.load, error).await,
    }
}

/// What a fence's `error` means: a staging or log found missing, a rival replay deleted the
/// staging as it took the log, or the log was removed; anything else fails the take.
async fn missing(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    error: io::Error,
) -> Result<Fenced, Error> {
    if error.kind() != io::ErrorKind::NotFound {
        return Err(Error::from_wal(error));
    }
    if open(store, pipeline, load).await? {
        Ok(Fenced::Lost)
    } else {
        Ok(Fenced::Gone)
    }
}
