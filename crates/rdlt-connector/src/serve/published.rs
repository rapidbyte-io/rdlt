//! Reading back what a served destination published, for certification: the table's rows as a
//! read's frames, each within the host's limits, and done.
//!
//! The table is read a batch at a time, each sent before the next is read, so the connector
//! holds a few batches however large the table is, and reads no further than its host takes.

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_wire::Limits;
use rdlt_wire::plane::Answers;
use rdlt_wire::tonic::Status;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::read::Outbox;
use crate::destination::{PublishedReader, PublishedRows, TableRef};
use crate::wire::{status, v1};

/// Frames ready for the host ahead of those it has taken.
const FRAMES: usize = 4;

/// Every row `reader` published to `table`, as frames: a schema frame opening each epoch, batch
/// frames, and the done frame.
pub(super) fn serve(
    reader: Arc<dyn PublishedReader>,
    table: TableRef,
    host: Limits,
) -> Answers<v1::ReadFrame> {
    let (frames, answer) = mpsc::channel(FRAMES);
    tokio::spawn(pump(reader, table, host, frames));
    Box::pin(ReceiverStream::new(answer))
}

/// Reads `table` back and sends its frames until it is read, or the host goes.
async fn pump(
    reader: Arc<dyn PublishedReader>,
    table: TableRef,
    host: Limits,
    frames: mpsc::Sender<Result<v1::ReadFrame, Status>>,
) {
    let (rows, batches) = PublishedRows::channel();
    // Dropping the batches' receiver, as a failed or left send does, ends the read-back.
    let reading = reader.published(&table, rows);
    let (read, sent) = tokio::join!(reading, send(batches, host, &frames));
    let ended = match (sent, read) {
        (Err(Left::Host), _) => return,
        (Err(Left::Refused(refused)), _) => Err(refused),
        (Ok(()), Err(error)) => Err(status(&error)),
        (Ok(()), Ok(())) => Ok(v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Done(v1::Done {})),
        }),
    };
    frames.send(ended).await.ok();
}

/// Why the frames of a read-back stopped before its batches did.
enum Left {
    /// The host left.
    Host,
    /// A batch does not fit the host's limits.
    Refused(Status),
}

/// Sends each of `batches` as its frames, within `host`'s limits.
async fn send(
    mut batches: mpsc::Receiver<RecordBatch>,
    host: Limits,
    frames: &mpsc::Sender<Result<v1::ReadFrame, Status>>,
) -> Result<(), Left> {
    let mut outbox = Outbox::new(host);
    while let Some(batch) = batches.recv().await {
        outbox
            .batch(&batch, v1::BatchKind::Arrow)
            .map_err(Left::Refused)?;
        // A piece of the batch at a time: the next is cut once the host has taken the last.
        loop {
            for frame in outbox.take_frames() {
                frames.send(Ok(frame)).await.map_err(|_| Left::Host)?;
            }
            if !outbox.cutting() {
                break;
            }
            outbox.refill().map_err(Left::Refused)?;
        }
    }
    Ok(())
}
