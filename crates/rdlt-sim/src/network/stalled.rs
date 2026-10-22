//! A peer that stalls: it accepts the engine's connection over mutual TLS, answers its handshake
//! and its configuration, and never sends the response headers of any later call, its
//! heartbeat's among them, while it answers HTTP/2's pings and settings, so only the host's
//! heartbeat can notice.

#[cfg(test)]
mod tests;

use std::io;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::destination_factory;
use rdlt_connector::serve::{Served, serve_connection};
use rdlt_wire::Limits;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::runtime::Handle;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;

use super::{DRAIN, Net, PORT};
use crate::destination::SimDestination;

/// The first stream whose answers are withheld: a client's streams are odd, the handshake's 1
/// and the configuration's 3.
const WITHHELD_FROM: u32 = 5;

/// The frame types that answer a call: its data, its headers, their continuation, and its reset.
const ANSWERS: [u8; 4] = [0x0, 0x1, 0x9, 0x3];

/// Bytes: the head of an HTTP/2 frame.
const HEAD: usize = 9;

/// What every stalled connection shares: the patience of the engine's heartbeat, and each
/// connection the engine kept open beyond it.
#[derive(Debug, Default)]
pub(super) struct Stalls {
    patience: Mutex<Option<Duration>>,
    violations: Mutex<Vec<String>>,
    /// How many stalled connections have been judged.
    judged: AtomicUsize,
}

impl Stalls {
    /// Judges each stalled connection by `patience`, the engine's heartbeat's.
    pub(super) fn judge_by(&self, patience: Duration) {
        *self.patience.lock() = Some(patience);
    }

    /// How many connections that stalled have been judged, kept open too long or not.
    #[cfg(test)]
    pub(super) fn judged(&self) -> usize {
        self.judged.load(Ordering::Relaxed)
    }

    /// Every stalled connection the engine kept open too long.
    pub(super) fn violations(&self) -> Vec<String> {
        self.violations.lock().clone()
    }
}

/// Serves both connectors on the stalled host, each connection behind a [`Withheld`] transport,
/// until the simulation ends.
pub(super) async fn stall(net: Arc<Net>) -> turmoil::Result {
    net.served.lock().push(Handle::current());
    let (tls, _) = super::mutual(&net)?;
    let acceptor = TlsAcceptor::from(tls);
    let served = Arc::new(
        Served::new()
            .with_source(net.serving.factory())
            .with_destination(destination_factory::<SimDestination>()),
    );
    let listener = turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let (acceptor, served) = (acceptor.clone(), Arc::clone(&served));
        let stalls = Arc::clone(&net.stalls);
        tokio::spawn(async move {
            let tls = super::handshake(&acceptor, stream).await;
            let Some(tls) = tls else {
                return;
            };
            // A transport that fails ends the connection, which is all the peer does about it.
            serve_connection(served, Withheld::new(tls, stalls), Limits::default())
                .await
                .ok();
        });
    }
}

/// A served connection's transport that drops every frame it is given to write that answers a
/// stream from [`WITHHELD_FROM`] on, writes every other frame as given, and notes how long the
/// engine kept the connection open once it withheld the first.
pub(super) struct Withheld<IO> {
    io: IO,
    stalls: Arc<Stalls>,
    /// The head of the frame being written, as far as it has been given.
    head: [u8; HEAD],
    headed: usize,
    /// Bytes: what of the frame's payload is still to come.
    left: usize,
    /// Whether the frame being written is withheld.
    withholding: bool,
    /// What was given to write and kept, not yet written on.
    kept: Vec<u8>,
    /// When it first withheld a frame.
    since: Option<Instant>,
    /// Whether the engine's end of the connection has closed.
    ended: bool,
}

impl<IO> Withheld<IO> {
    /// `io`, its answers withheld from [`WITHHELD_FROM`] on, judged as `stalls` says.
    pub(super) fn new(io: IO, stalls: Arc<Stalls>) -> Self {
        Self {
            io,
            stalls,
            head: [0; HEAD],
            headed: 0,
            left: 0,
            withholding: false,
            kept: Vec::new(),
            since: None,
            ended: false,
        }
    }

    /// Takes `given`, keeping what is not withheld.
    fn sift(&mut self, mut given: &[u8]) {
        while !given.is_empty() {
            if self.headed < HEAD {
                let taken = (HEAD - self.headed).min(given.len());
                self.head[self.headed..self.headed + taken].copy_from_slice(&given[..taken]);
                self.headed += taken;
                given = &given[taken..];
                if self.headed == HEAD {
                    self.begin();
                }
                continue;
            }
            let taken = self.left.min(given.len());
            if !self.withholding {
                self.kept.extend_from_slice(&given[..taken]);
            }
            self.left -= taken;
            given = &given[taken..];
            if self.left == 0 {
                self.headed = 0;
            }
        }
    }

    /// Begins the frame whose head is whole: withheld where it answers a stream from
    /// [`WITHHELD_FROM`] on, kept otherwise.
    fn begin(&mut self) {
        let [high, middle, low, kind, _flags, s0, s1, s2, s3] = self.head;
        let length = (usize::from(high) << 16) | (usize::from(middle) << 8) | usize::from(low);
        let stream = u32::from_be_bytes([s0 & 0x7f, s1, s2, s3]);
        self.withholding = stream >= WITHHELD_FROM && ANSWERS.contains(&kind);
        if self.withholding {
            self.since.get_or_insert_with(Instant::now);
        } else {
            self.kept.extend_from_slice(&self.head);
        }
        self.left = length;
        if length == 0 {
            self.headed = 0;
        }
    }

    /// Notes that the engine's end closed: past its heartbeat's patience, and the time a close
    /// takes to arrive, after the first answer was withheld, the engine never noticed the stall.
    fn end(&mut self) {
        if std::mem::replace(&mut self.ended, true) {
            return;
        }
        let (Some(since), Some(patience)) = (self.since, *self.stalls.patience.lock()) else {
            return;
        };
        self.stalls.judged.fetch_add(1, Ordering::Relaxed);
        let open = since.elapsed();
        if open > patience + DRAIN {
            self.stalls.violations.lock().push(format!(
                "a connection whose answers stalled stayed open {open:?}, beyond its heartbeat's \
                 patience of {patience:?}"
            ));
        }
    }
}

impl<IO> Drop for Withheld<IO> {
    /// A connection that ends any way but a read's end is judged as it goes.
    fn drop(&mut self) {
        self.end();
    }
}

impl<IO: AsyncWrite + Unpin> Withheld<IO> {
    /// Writes on what was kept.
    fn drain(&mut self, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        while !self.kept.is_empty() {
            let written = ready!(Pin::new(&mut self.io).poll_write(context, &self.kept))?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.kept.drain(..written);
        }
        Poll::Ready(Ok(()))
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for Withheld<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let (before, room) = (buffer.filled().len(), buffer.remaining());
        let read = Pin::new(&mut self.io).poll_read(context, buffer);
        let ended = match &read {
            Poll::Ready(Ok(())) => room > 0 && buffer.filled().len() == before,
            Poll::Ready(Err(_)) => true,
            Poll::Pending => false,
        };
        if ended {
            self.end();
        }
        read
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Withheld<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        given: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        ready!(this.drain(context))?;
        this.sift(given);
        // What was kept goes on now where it can, else at the next write or flush.
        if let Poll::Ready(Err(error)) = this.drain(context) {
            return Poll::Ready(Err(error));
        }
        Poll::Ready(Ok(given.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        ready!(this.drain(context))?;
        Pin::new(&mut this.io).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        ready!(this.drain(context))?;
        Pin::new(&mut this.io).poll_shutdown(context)
    }
}
