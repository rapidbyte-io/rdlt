//! Where a planned partition's read starts: from its committed position, from its beginning, or
//! not at all.

use rdlt_connector::{Cursor, Partition, PartitionId, PartitionState, ReadMode, StreamName};

use crate::error::{Error, codes};

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
            Err(cursor_missing(stream, &partition))
        }
        Some(PartitionState::Done) => Ok(None),
        Some(PartitionState::Cursor(cursor)) => Ok(Some((partition, Some(cursor.clone())))),
        None => Ok(Some((partition, None))),
    }
}

/// The refusal of a read that is not full of `stream`'s `partition`, which never ends and which a
/// read finished on rows with no checkpoint after them, as a full read does, whether it
/// checkpointed before them or never: in the user's terms, with what the user can do, and naming
/// the partition only where the source declared it.
fn cursor_missing(stream: &StreamName, partition: &Partition) -> Error {
    let which = if *partition.id() == PartitionId::whole() {
        String::new()
    } else {
        format!(" of partition {}", partition.id())
    };
    Error::config(format!(
        "stream {stream}: its last full read{which} ended on rows with no checkpoint after them, \
         so an incremental read has no position to resume from; keep reading the stream in full, \
         or reset it with its tables so that incremental reads start from its beginning"
    ))
    .with_code(codes::CURSOR_MISSING)
    .with_stream(stream)
}

#[cfg(test)]
mod tests;
