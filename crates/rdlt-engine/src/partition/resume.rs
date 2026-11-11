//! Where a planned partition's read starts: from its committed position, from its beginning, or
//! not at all.

use rdlt_connector::{Cursor, Partition, PartitionState, ReadMode, StreamName};

use crate::error::{Error, ErrorKind, codes};

/// Where `stream`'s planned `partition`, read as `read` says, is read from, given its committed
/// `position`: its cursor, or its beginning where it has none; `None` for one a read finished.
///
/// # Errors
///
/// `cursor_missing`, a Source error no retry mends, for a partition that never ends which a read
/// finished, unless the stream is read in full: the rows after its last checkpoint landed, so a
/// read from that checkpoint would land them twice, and passing over it would miss every row it
/// gains.
pub(crate) fn resumed(
    stream: &StreamName,
    partition: Partition,
    position: Option<&PartitionState>,
    read: ReadMode,
) -> Result<Option<(Partition, Option<Cursor>)>, Error> {
    match position {
        Some(PartitionState::Done) if partition.is_unbounded() && read != ReadMode::Full => {
            Err(Error::new(
                ErrorKind::Source,
                format!(
                    "stream {stream}: partition {} never ends, and a read finished it with rows \
                     after its last checkpoint, so no cursor resumes it past them; read the stream \
                     in full once more, ending on a checkpoint, or reset it",
                    partition.id()
                ),
            )
            .with_code(codes::CURSOR_MISSING)
            .with_stream(stream))
        }
        Some(PartitionState::Done) => Ok(None),
        Some(PartitionState::Cursor(cursor)) => Ok(Some((partition, Some(cursor.clone())))),
        None => Ok(Some((partition, None))),
    }
}

#[cfg(test)]
mod tests;
