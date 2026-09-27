//! Reading back what a served destination published, for certification: the table's rows as a
//! read's frames, each within the host's limits, and done.

use std::sync::Arc;

use rdlt_wire::Limits;
use rdlt_wire::tonic::Status;

use super::read::Outbox;
use super::service::Answer;
use crate::destination::{PublishedReader, TableRef};
use crate::wire::{status, v1};

/// Every row `reader` published to `table`, as frames: a schema frame opening each epoch, batch
/// frames, and the done frame.
pub(super) async fn serve(
    reader: Arc<dyn PublishedReader>,
    table: TableRef,
    host: Limits,
) -> Result<Answer<v1::ReadFrame>, Status> {
    let batches = reader
        .published(&table)
        .await
        .map_err(|error| status(&error))?;
    let mut outbox = Outbox::new(host);
    for batch in &batches {
        outbox.batch(batch, v1::BatchKind::Arrow)?;
    }
    outbox.push(v1::read_frame::Frame::Done(v1::Done {}));
    let frames = outbox.into_frames().into_iter().map(Ok);
    Ok(Box::pin(tokio_stream::iter(frames)))
}
