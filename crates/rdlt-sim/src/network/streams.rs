//! Connectors reached as a spawned connector is: a stream for the control connection and one for
//! each data call, each a pipe between the engine's host and the connector's, as a socket pair is
//! between a host and the connector it spawned.
//!
//! A pipe holds a socket buffer's bytes each way and closes as a socket pair does: what one end
//! sent stays for the other to read, then the other reads the stream's end, and its writes fail.
//! No partition or hold of the network reaches a pipe, as none reaches a socket pair; a
//! connector's crash closes its ends.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use rdlt_connector::BoxFuture;
use rdlt_connector::serve::{Opening, Served, serve_streams};
use rdlt_host::{Opener, Stream};
use rdlt_wire::Limits;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Bytes: what a pipe holds of one direction of a stream, as a socket's buffer does.
pub(crate) const BUFFERED: u64 = 64 * 4096;

/// A stream opened to a connector: what it is, its connection's number, and the connector's end.
type Passed = (Opening, u64, DuplexStream);

/// Where each connector's host takes the streams the engine's host opens, while it serves them.
#[derive(Default)]
pub(crate) struct Pipes {
    listening: Mutex<BTreeMap<&'static str, mpsc::Sender<Passed>>>,
    /// The number the next connection takes.
    numbers: AtomicU64,
}

/// Opens the streams of one connection to the connector on `host`.
pub(crate) struct Dialing {
    pipes: Arc<Pipes>,
    host: &'static str,
    connection: u64,
}

impl Dialing {
    /// The streams of a new connection, through `pipes`, to the connector on `host`.
    pub(crate) fn new(pipes: Arc<Pipes>, host: &'static str) -> Self {
        let connection = pipes.numbers.fetch_add(1, Ordering::Relaxed);
        Self {
            pipes,
            host,
            connection,
        }
    }
}

impl Opener for Dialing {
    fn open(&self, opening: Opening) -> BoxFuture<'_, std::io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let refused = || {
                std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "no connector takes streams there",
                )
            };
            let passing = self.pipes.listening.lock().get(self.host).cloned();
            let passing = passing.ok_or_else(refused)?;
            let size = usize::try_from(BUFFERED).unwrap_or(usize::MAX);
            let (host, connector) = tokio::io::duplex(size);
            passing
                .send((opening, self.connection, connector))
                .await
                .map_err(|_| refused())?;
            Ok(Box::new(host) as Box<dyn Stream>)
        })
    }
}

/// `host`'s place in `pipes`, given up once dropped: a crash gives it up with the rest.
struct Listening {
    pipes: Arc<Pipes>,
    host: &'static str,
    passing: mpsc::Sender<Passed>,
}

impl Drop for Listening {
    fn drop(&mut self) {
        let mut listening = self.pipes.listening.lock();
        // A connector started again on the host may have taken the place since.
        if listening
            .get(self.host)
            .is_some_and(|passing| passing.same_channel(&self.passing))
        {
            listening.remove(self.host);
        }
    }
}

/// Serves `served` on the streams the engine's host opens, through `pipes`, to `host`, within
/// `limits`, until `stop` ends; then takes no more, and stops the connections it serves, which
/// drain as a stopping connector's do.
pub(crate) async fn serve<F>(
    pipes: Arc<Pipes>,
    host: &'static str,
    served: Arc<Served>,
    limits: Limits,
    stop: F,
) where
    F: Future<Output = ()> + Clone + Send + 'static,
{
    let (passing, mut passed) = mpsc::channel(4);
    pipes.listening.lock().insert(host, passing.clone());
    let listening = Listening {
        pipes,
        host,
        passing,
    };
    let mut connections: BTreeMap<u64, mpsc::Sender<(Opening, DuplexStream)>> = BTreeMap::new();
    let mut serving = JoinSet::new();
    let stopping = stop.clone();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            biased;
            () = &mut stop => break,
            Some((opening, connection, stream)) = passed.recv() => {
                connections.retain(|_, passing| !passing.is_closed());
                let passing = match opening {
                    Opening::Control => {
                        let (passing, passed) = mpsc::channel(4);
                        serving.spawn(serve_streams(
                            Arc::clone(&served),
                            passed,
                            limits,
                            stopping.clone(),
                        ));
                        connections.insert(connection, passing.clone());
                        passing
                    }
                    Opening::Data => match connections.get(&connection) {
                        Some(passing) => passing.clone(),
                        // A stream of a connection this host does not serve ends unanswered.
                        None => continue,
                    },
                };
                passing.send((opening, stream)).await.ok();
            }
            Some(_) = serving.join_next() => {}
        }
    }
    drop((listening, passed, connections));
    while serving.join_next().await.is_some() {}
}
