//! A served read: the source reads a partition into a channel, its pushes within a window of the
//! host's frame limit, and the read's pump writes each event it sends as frames, one at a time,
//! while it reads the host's controls.

mod outbox;
#[cfg(test)]
mod tests;
mod window;

use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_wire::stream::{Call, Outgoing, Received};
use tokio::io::{AsyncRead, AsyncWrite};

use super::noted::{Note, Noting, Read};
use super::service::Service;
use super::stream::{Frames, failed, opened, send};
use crate::Cursor;
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::id::{PartitionId, StreamName};
use crate::sink::{Admission, PartitionFeed, SourceEvent, admitted_partition_channel};
use crate::source::{Partition, ReadRequest, Source};
use crate::wire::{Invalid, frame_error, v1};
pub(super) use outbox::Outbox;
use window::Window;

/// Events the source may send ahead of the frame being written.
const EVENTS: NonZeroUsize = NonZeroUsize::new(16).expect("sixteen is not zero");

/// Serves the read `opening` starts, whose cursor keeps within the connection's own limits; what
/// it sends keeps within the host's, and goes to `io` while `controls` brings the host's.
pub(super) async fn serve<R, W>(
    service: &Service,
    opening: &Received,
    controls: Frames<R>,
    mut io: W,
) where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let started = started(service, opening, &controls);
    let (source, request, barrier) = match started {
        Ok(started) => started,
        Err(error) => {
            send(&mut io, failed(Call::Read, &error)).await;
            return;
        }
    };
    if !send(&mut io, opened(Call::Read)).await {
        return;
    }
    let read = Read {
        stream: request.stream.clone(),
        partition: request.partition.id().clone(),
        start: request.cursor.clone(),
    };
    let noting = Noting::new(Arc::clone(&service.served), service.host_name.clone(), read);
    let window = Arc::new(Window::new(service.host()));
    let pumping = Pumping {
        source,
        request,
        barrier,
        noting,
    };
    pump(pumping, controls, io, window).await;
}

/// The source, request and pending barrier of the read `opening` starts.
fn started<R: AsyncRead + Unpin>(
    service: &Service,
    opening: &Received,
    controls: &Frames<R>,
) -> Result<(Arc<dyn Source>, ReadRequest, u64), ConnectorError> {
    let start: v1::ReadStart = controls.start(Call::Read, opening)?;
    let source = service.source()?;
    if let Some(cursor) = &start.cursor {
        service
            .limits
            .admit_cursor(cursor.bytes.len())
            .map_err(refused)?;
    }
    let barrier = start.barrier;
    let request = request(start).map_err(|error| super::service::invalid(&error))?;
    Ok((source, request, barrier))
}

fn request(start: v1::ReadStart) -> Result<ReadRequest, Invalid> {
    let stream = StreamName::try_from(start.stream.ok_or(Invalid::Missing("stream"))?)?;
    let partition = PartitionId::parse(start.partition)
        .map_err(|error| Invalid::rejected("partition id", error))?;
    let partition = if start.unbounded {
        Partition::new(partition).unbounded()
    } else {
        Partition::new(partition)
    };
    Ok(ReadRequest::new(
        stream,
        partition,
        start.cursor.map(Cursor::try_from).transpose()?,
    )
    .following(start.follow))
}

/// A frame the host's limits refuse, as the error the read fails with.
fn refused(refusal: rdlt_wire::Refusal) -> ConnectorError {
    frame_error(&rdlt_wire::WireError::Refused(refusal))
}

/// A spawned read, aborted once dropped: a read the host leaves, which may wait for data that
/// never comes, ends with the pump.
struct Reading(tokio::task::JoinHandle<crate::error::Result<()>>);

impl Drop for Reading {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// What a pump reads, and for whom.
struct Pumping {
    source: Arc<dyn Source>,
    request: ReadRequest,
    barrier: u64,
    noting: Noting,
}

/// What a pump's turn came to.
enum Turn {
    Control(Result<Option<Received>, ConnectorError>),
    Written(bool),
    Event(Option<SourceEvent>),
    Ended(Result<crate::error::Result<()>, tokio::task::JoinError>),
}

/// What a pump does after a turn.
enum Then {
    /// Goes on.
    Pumps,
    /// Ends the read: its host went.
    Ends,
    /// Fails the read with this error, then ends it.
    Fails(ConnectorError),
}

/// Where a pump stands: what it sends next, and how far the source's read went.
struct Pumped {
    outbox: Outbox,
    /// Whether the source's channel has ended.
    exhausted: bool,
    /// Whether the source's read has returned.
    done: bool,
}

impl Pumped {
    /// Takes `turn`, the host's `control` read from `controls` among them.
    fn take<R: AsyncRead + Unpin>(
        &mut self,
        turn: Turn,
        controls: &Frames<R>,
        feed: &PartitionFeed,
    ) -> Then {
        let queued = match turn {
            Turn::Control(control) => {
                if !applied(controls, control, feed) {
                    return Then::Ends;
                }
                Ok(())
            }
            Turn::Written(true) => Ok(()),
            Turn::Written(false) => return Then::Ends,
            Turn::Event(Some(event)) => self.outbox.event(event),
            Turn::Event(None) => {
                self.exhausted = true;
                Ok(())
            }
            Turn::Ended(ended) => {
                self.done = true;
                finish(&mut self.outbox, ended)
            }
        };
        match queued {
            Ok(()) => Then::Pumps,
            Err(error) => Then::Fails(error),
        }
    }
}

/// Reads the partition and writes its frames, one at a time, until the read is done, or the
/// host goes; `admission` admits what the source sends into the channel, within the host's
/// limits.
async fn pump<R, W>(
    pumping: Pumping,
    mut controls: Frames<R>,
    mut io: W,
    admission: Arc<dyn Admission>,
) where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let Pumping {
        source,
        request,
        barrier,
        mut noting,
    } = pumping;
    let mut pumped = Pumped {
        outbox: Outbox::new(admission.limits()),
        exhausted: false,
        done: false,
    };
    let (sink, mut feed) = admitted_partition_channel(EVENTS, admission);
    // What the source pushes is cut to the host's frames as it is sent.
    let sink = sink.cut();
    // A barrier pending when the read started is the source's to answer from its first push.
    if let Some(barrier) = std::num::NonZeroU64::new(barrier) {
        feed.request_checkpoint(barrier.get());
    }
    let mut reading = Reading(tokio::spawn(
        async move { source.read(request, sink).await },
    ));
    let mut writing: Option<Outgoing> = None;
    let failure = loop {
        if writing.is_none()
            && let Some(queued) = pumped.outbox.next()
        {
            noting.note(&queued.note);
            writing = Some(queued.frame);
        }
        // The next piece of a batch being cut is encoded once the last was written, and before
        // anything else the source sends is taken.
        if writing.is_none() && pumped.outbox.cutting() {
            if let Err(error) = pumped.outbox.refill() {
                break Some(error);
            }
            tokio::task::yield_now().await;
            continue;
        }
        if writing.is_none() && pumped.done {
            break None;
        }
        let idle = writing.is_none();
        let (exhausted, done) = (pumped.exhausted, pumped.done);
        let turn = tokio::select! {
            biased;
            // The host's controls win over the frame being written, which goes on where it was.
            control = controls.next() => Turn::Control(control),
            written = write(&mut writing, &mut io), if !idle => Turn::Written(written),
            event = feed.recv(), if idle && !exhausted => Turn::Event(event),
            // A source may clean up after its sink is gone, while the host's controls are read.
            ended = &mut reading.0, if idle && exhausted && !done => Turn::Ended(ended),
        };
        match pumped.take(turn, &controls, &feed) {
            Then::Pumps => {}
            Then::Ends => break None,
            Then::Fails(error) => break Some(error),
        }
    };
    // The host may have gone; the read ends either way.
    if let Some(error) = failure {
        send(&mut io, failed(Call::Read, &error)).await;
    }
    feed.stop();
}

/// Writes what is left of the frame `writing` holds to `io`; whether the host took it.
async fn write<W: AsyncWrite + Unpin>(writing: &mut Option<Outgoing>, io: &mut W) -> bool {
    let Some(frame) = writing else {
        return true;
    };
    let written = frame.write(io).await.is_ok();
    *writing = None;
    written
}

/// Applies the host's `control`; whether the host is still there.
fn applied<R: AsyncRead + Unpin>(
    controls: &Frames<R>,
    control: Result<Option<Received>, ConnectorError>,
    feed: &PartitionFeed,
) -> bool {
    use v1::read_control::Control;
    let Ok(Some(received)) = control else {
        return false;
    };
    let Ok(control) = controls.message::<v1::ReadControl>(&received) else {
        return false;
    };
    match control.control {
        Some(Control::Checkpoint(request)) => feed.request_checkpoint(request.barrier),
        Some(Control::Stop(_)) => feed.stop(),
        None => {}
    }
    true
}

/// Queues the frame ending the read, from how the source's read ended.
fn finish(
    outbox: &mut Outbox,
    ended: Result<crate::error::Result<()>, tokio::task::JoinError>,
) -> Result<(), ConnectorError> {
    match ended {
        Ok(Ok(())) => outbox.message(v1::read_frame::Frame::Done(v1::Done {}), Note::Data),
        Ok(Err(error)) => Err(error),
        // The join error's text carries a process-wide task number and the panic's message,
        // which may quote what was read; neither is this connector's to tell its host.
        Err(join) => Err(ConnectorError::new(
            ConnectorErrorKind::Internal,
            if join.is_panic() {
                "the read panicked"
            } else {
                "the read was cancelled"
            },
        )),
    }
}
