//! Where a planned partition's read starts: from its committed position, from its beginning, or
//! not at all.

use rdlt_connector::{Cursor, Partition, PartitionId, PartitionState, ReadMode, StreamName};

use crate::error::{Error, codes};

/// What a refusal of a read that has no position to resume from says a reset does: it helps
/// only a source that ends each incremental read on a checkpoint, since an incremental read of a
/// partition that never ends lands only the rows before its last checkpoint.
pub(crate) const RESET_REMEDY: &str = "reset it with its tables so that incremental reads start \
                                       from its beginning, which lands every row only where its \
                                       source ends each incremental read on a checkpoint";

/// Where `stream`'s planned `partition`, read as `read` says, is read from, given its committed
/// `position`: its cursor, or its beginning where it has none; `None` for one a read finished.
///
/// # Errors
///
/// `cursor_missing`, a Config error no retry mends, for a partition that never ends which a read
/// finished, unless the stream is read in full: the rows it read with no checkpoint after them
/// landed, so a read from before them would land them twice, and passing over it would miss
/// every row it gains.
pub(crate) fn resumed(
    stream: &StreamName,
    partition: Partition,
    position: Option<&PartitionState>,
    read: ReadMode,
) -> Result<Option<(Partition, Option<Cursor>)>, Error> {
    match position {
        Some(PartitionState::Done) if partition.is_unbounded() && read != ReadMode::Full => {
            Err(cursor_missing(stream, &partition, read))
        }
        Some(PartitionState::Done) => Ok(None),
        Some(PartitionState::Cursor(cursor)) => Ok(Some((partition, Some(cursor.clone())))),
        None => Ok(Some((partition, None))),
    }
}

/// A read of `mode`, as a refusal names it.
pub(crate) fn read_named(mode: ReadMode) -> &'static str {
    match mode {
        ReadMode::Full => "a full read",
        ReadMode::Incremental => "an incremental read",
        ReadMode::Cdc => "a change read",
    }
}

/// The refusal of `stream`'s `partition` to a `read` that is not full, where the partition never
/// ends and a read finished it on rows with no checkpoint after them: in the user's terms, saying
/// only what state shows, with what the user can do, and naming the partition only where the
/// source declared it.
fn cursor_missing(stream: &StreamName, partition: &Partition, read: ReadMode) -> Error {
    let finished = if *partition.id() == PartitionId::whole() {
        "the stream".to_owned()
    } else {
        format!("its partition {}", partition.id())
    };
    Error::config(format!(
        "stream {stream}: a read finished {finished} on rows with no checkpoint after them, and \
         its source says it never ends, so {} has no position to resume it from; read the \
         stream in full, or {RESET_REMEDY}",
        read_named(read)
    ))
    .with_code(codes::CURSOR_MISSING)
    .with_stream(stream)
}

#[cfg(test)]
mod tests;
