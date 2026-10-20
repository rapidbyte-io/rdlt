//! Writes whose bodies stop within a message, as no client of the protocol sends them: some take
//! a connection's whole window, and others wait behind them for room.

use std::future::Future as _;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rdlt_wire::Limits;
use rdlt_wire::limits::Class;
use tokio::sync::oneshot;

use super::{Attacker, PATIENCE};
use crate::network::{PORT, Side};
use crate::rng::SplitMix64;

/// Bytes: a gRPC message's prefix, its flag and its length.
pub(super) const PREFIX: usize = 5;

/// Writes that each take room for the largest message a write may hold: one more than the four
/// such messages a connection's window holds, so it is full whatever room is left.
pub(super) const HOLDERS: usize = 5;

/// How long the network takes to carry a write's first frame and serving to take its room.
pub(super) const SETTLE: Duration = Duration::from_millis(200);

/// A body that sends `data`, then nothing more until `cut` fires or is dropped, when it fails.
struct Stalled {
    data: Option<Bytes>,
    cut: oneshot::Receiver<()>,
}

impl http_body::Body for Stalled {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        if let Some(data) = self.data.take() {
            return Poll::Ready(Some(Ok(http_body::Frame::data(data))));
        }
        match Pin::new(&mut self.cut).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Some(Err(std::io::Error::other(
                "the hostile host cut its write",
            )))),
        }
    }
}

/// An HTTP/2 connection to a connector, whose calls are written by hand.
pub(super) struct Raw {
    sender: SendRequest<Stalled>,
    host: &'static str,
}

/// A write in flight: cut when dropped, and told when serving answered it.
pub(super) struct Write {
    _cut: oneshot::Sender<()>,
    answered: oneshot::Receiver<()>,
}

impl Raw {
    /// HTTP/2 over `wire`, to the connector listening on `host`.
    pub(super) async fn over(wire: rdlt_host::Wire, host: &'static str) -> Option<Self> {
        let handshaking =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(wire));
        let (sender, connection) = handshaking.await.ok()?;
        tokio::spawn(connection);
        Some(Self { sender, host })
    }

    /// A write whose first message declares `length` bytes, and whose body carries `carried`
    /// bytes past the prefix, all zero, and then stops.
    pub(super) fn write(&mut self, length: usize, carried: usize) -> Write {
        let mut data = vec![0; PREFIX + carried];
        data[1..PREFIX].copy_from_slice(&u32::try_from(length).unwrap_or(u32::MAX).to_be_bytes());
        let (cut, held) = oneshot::channel();
        let body = Stalled {
            data: Some(Bytes::from(data)),
            cut: held,
        };
        let uri = format!("https://{}:{PORT}{}", self.host, rdlt_wire::plane::WRITE);
        let request = http::Request::post(uri)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header(http::header::TE, "trailers")
            .body(body)
            .expect("a request of known parts builds");
        let responding = self.sender.send_request(request);
        let (ended, answered) = oneshot::channel();
        tokio::spawn(async move {
            responding.await.ok();
            ended.send(()).ok();
        });
        Write {
            _cut: cut,
            answered,
        }
    }
}

impl Write {
    /// Whether serving has answered it; the answer stays for [`Write::ends_within`].
    pub(super) fn answered(&self) -> bool {
        !self.answered.is_empty()
    }

    /// Whether serving answers it within `patience`; it is held, not cut, until then.
    pub(super) async fn ends_within(self, patience: Duration) -> bool {
        let Self {
            _cut: cut,
            answered,
        } = self;
        let ended = tokio::time::timeout(patience, answered).await.is_ok();
        drop(cut);
        ended
    }
}

impl Attacker<'_> {
    /// Writes that take the destination connection's whole window, and writes behind them that each
    /// bring a whole message and a byte past it in one frame: those wait, unanswered, and are
    /// answered once the first are cut and their room comes back.
    pub(super) async fn window(&self, rng: &mut SplitMix64) {
        let Some(wire) = self.placing.wire(Side::Destination).await else {
            self.lost("dial");
            return;
        };
        let Some(mut raw) = Raw::over(wire, Side::Destination.host()).await else {
            self.lost("HTTP/2's handshake");
            return;
        };
        let largest = Limits::default().decoding(Class::Data) - PREFIX;
        let holders: Vec<Write> = (0..HOLDERS).map(|_| raw.write(largest, 0)).collect();
        tokio::time::sleep(SETTLE).await;
        let count = 1 + rng.below(4);
        let waiting: Vec<Write> = (0..count)
            .map(|_| {
                let length = 16 * 1024 + usize::try_from(rng.below(48 * 1024)).unwrap_or(0);
                raw.write(length, length + 1)
            })
            .collect();
        tokio::time::sleep(SETTLE).await;
        if self.strict && waiting.iter().any(Write::answered) {
            self.found(
                "window",
                "a write was answered while writes before it held the whole window",
            );
        }
        drop(holders);
        for write in waiting {
            if !write.ends_within(PATIENCE).await {
                self.lost("write once the window's room came back");
            }
        }
    }
}
