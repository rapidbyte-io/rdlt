//! Reading back what a served destination published, for certification: the table's rows as a
//! read's frames, each within the host's limits, and done.
//!
//! The table is read a batch at a time, each written before the next is read, so the connector
//! holds a few batches however large the table is, and reads no further than its host takes.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_wire::Limits;
use rdlt_wire::stream::{Call, Outgoing, Received, Rows};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use super::read::Outbox;
use super::service::Service;
use super::stream::{Frames, failed, opened, send};
use crate::destination::{PublishedReader, PublishedRows, TableRef};
use crate::error::ConnectorError;
use crate::wire::v1;

/// Serves the read-back `opening` asks for: every row the destination published to its table,
/// written to `io` within the host's limits, while `frames` shows whether the host is there.
pub(super) async fn serve<R, W>(service: &Service, opening: &Received, frames: Frames<R>, mut io: W)
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let asked = frames
        .start(Call::ReadPublished, opening)
        .and_then(|request| service.probes().read_published(request));
    let (reader, table) = match asked {
        Ok(asked) => asked,
        Err(error) => {
            send(&mut io, failed(Call::ReadPublished, &error)).await;
            return;
        }
    };
    if send(&mut io, opened(Call::ReadPublished)).await {
        pump(reader, table, service.host(), frames, io).await;
    }
}

/// Reads `table` back and writes its frames until it is read, or the host goes.
async fn pump<R, W>(
    reader: Arc<dyn PublishedReader>,
    table: TableRef,
    host: Limits,
    mut frames: Frames<R>,
    mut io: W,
) where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let (rows, batches) = PublishedRows::channel();
    // Dropping the batches' receiver, as a failed or left write does, ends the read-back.
    let reading = reader.published(&table, rows);
    let (read, written) = tokio::select! {
        biased;
        // A host that leaves, or sends anything, ends the read-back, whatever its reader waits
        // for: a read-back's host sends nothing after its start.
        _ = frames.next() => return,
        both = async { tokio::join!(reading, write(batches, host, &mut io)) } => both,
    };
    let ended = match (written, read) {
        (Err(Left::Host), _) => return,
        (Err(Left::Refused(refused)), _) => Err(refused),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    };
    let last = match ended {
        Ok(()) => {
            let done = v1::ReadFrame {
                frame: Some(v1::read_frame::Frame::Done(v1::Done {})),
            };
            Outgoing::message(None, &done).expect("a done frame fits a head")
        }
        Err(error) => failed(Call::ReadPublished, &error),
    };
    send(&mut io, last).await;
}

/// Why the frames of a read-back stopped before its batches did.
enum Left {
    /// The host left.
    Host,
    /// A batch does not fit the host's limits.
    Refused(ConnectorError),
}

/// Writes each of `batches` as its frames, within `host`'s limits.
async fn write<W: AsyncWrite + Unpin>(
    mut batches: mpsc::Receiver<RecordBatch>,
    host: Limits,
    io: &mut W,
) -> Result<(), Left> {
    let mut outbox = Outbox::new(host);
    while let Some(batch) = batches.recv().await {
        outbox.batch(&batch, Rows::Arrow).map_err(Left::Refused)?;
        // A piece of the batch at a time: the next is cut once the host has taken the last.
        loop {
            while let Some(queued) = outbox.next() {
                if !send(io, queued.frame).await {
                    return Err(Left::Host);
                }
            }
            if !outbox.cutting() {
                break;
            }
            outbox.refill().map_err(Left::Refused)?;
        }
    }
    Ok(())
}
