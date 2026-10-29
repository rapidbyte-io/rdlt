//! A played connector's process: what it serves and says, and how it ends.

use std::ffi::OsString;
use std::os::unix::process::ExitStatusExt as _;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::serve::{Opening, Served, serve_streams};
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorSpec, Destination, DestinationFactory, Source,
    SourceFactory, destination_factory,
};
use rdlt_host::limits::TAIL_BYTES;
use rdlt_host::{Ended, Opener, Played, Playing, Stream};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, DuplexStream};
use tokio::sync::{Notify, mpsc};
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

use crate::destination::SimDestination;
use crate::network::Serving;
use crate::rng::SplitMix64;

/// Bytes a played socket or standard stream holds before its writer waits, as a pipe's buffer.
const PIPE: usize = 64 * 1024;

/// Bytes: what is carried at once between the host's end of a stream and the served connector.
const CARRY: usize = 16 * 1024;

/// Bytes: what a played process's pipes hold of one direction of a stream: two pipes, and what
/// is being carried between them.
pub(super) const CARRIED: u64 = 2 * PIPE as u64 + CARRY as u64;

/// What passes a played process's streams to it: each a pipe to the host, carried to the served
/// connector while the process is not stalled, and cut once the process ends.
struct Passing {
    streams: mpsc::Sender<(Opening, DuplexStream)>,
    control: Arc<Control>,
    dead: CancellationToken,
}

impl Opener for Passing {
    fn open(&self, opening: Opening) -> BoxFuture<'_, std::io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let (host, connector) = tokio::io::duplex(PIPE);
            let (served_end, serving_end) = tokio::io::duplex(PIPE);
            let (control, dead) = (Arc::clone(&self.control), self.dead.clone());
            tokio::spawn(async move {
                tokio::select! {
                    biased;
                    () = dead.cancelled() => {}
                    () = relay(connector, served_end, control) => {}
                }
            });
            self.streams
                .send((opening, serving_end))
                .await
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::BrokenPipe))?;
            Ok(Box::new(host) as Box<dyn Stream>)
        })
    }
}

/// What plays one side's processes, and what the simulation does to those still running.
pub(super) struct Script {
    rng: Mutex<SplitMix64>,
    #[cfg(test)]
    first: u32,
    pids: AtomicU32,
    /// The source each process serves.
    serving: Serving,
    /// The processes played, the latest last; those that ended are let go.
    playing: Mutex<Vec<Weak<Control>>>,
}

/// What the simulation does to one played process while it runs.
#[derive(Default)]
struct Control {
    stalled: Mutex<bool>,
    /// Wakes the relay once the stall ends.
    resumed: Notify,
    /// Ends the process as a connector that exits by itself does.
    ends: Notify,
}

impl Control {
    fn stall(&self, stalled: bool) {
        *self.stalled.lock() = stalled;
        if !stalled {
            self.resumed.notify_waiters();
        }
    }

    /// Resolves once the process is not stalled.
    async fn going(&self) {
        loop {
            let resumed = self.resumed.notified();
            tokio::pin!(resumed);
            resumed.as_mut().enable();
            if !*self.stalled.lock() {
                return;
            }
            resumed.await;
        }
    }
}

/// How one played process behaves, drawn as it starts.
struct Draws {
    /// Lines it writes to its standard output as it starts, more than the host logs at once.
    flood: u64,
    /// Whether it also says its canary at the end of a line longer than the host reads at once.
    long: bool,
    /// How long it takes to end once asked to stop.
    lingers: Duration,
}

impl Draws {
    fn draw(rng: &mut SplitMix64) -> Self {
        Self {
            flood: if rng.chance(125) {
                256 + rng.below(1024)
            } else {
                0
            },
            long: rng.chance(250),
            lingers: Duration::from_millis(rng.below(300)),
        }
    }
}

impl Script {
    /// Plays processes drawn from `seed`, their ids counted from `first`, each serving the source
    /// `serving` names and the simulated destination.
    pub(super) fn new(seed: u64, first: u32, serving: Serving) -> Self {
        Self {
            rng: Mutex::new(SplitMix64::new(seed)),
            #[cfg(test)]
            first,
            pids: AtomicU32::new(first),
            serving,
            playing: Mutex::new(Vec::new()),
        }
    }

    /// How many processes it has played.
    #[cfg(test)]
    pub(super) fn played(&self) -> u32 {
        self.pids.load(Ordering::Relaxed) - self.first
    }

    /// Stalls every process still running, or lets them go on.
    pub(super) fn stall(&self, stalled: bool) {
        for control in self.running() {
            control.stall(stalled);
        }
    }

    /// Ends the latest process still running, as a connector that exits by itself.
    pub(super) fn end_latest(&self) {
        if let Some(control) = self.running().last() {
            control.ends.notify_one();
        }
    }

    fn running(&self) -> Vec<Arc<Control>> {
        let mut playing = self.playing.lock();
        playing.retain(|control| control.strong_count() > 0);
        playing.iter().filter_map(Weak::upgrade).collect()
    }

    /// Plays the process `playing` starts: the simulated connector, served on the streams the
    /// opener the host is given passes it.
    pub(super) fn play(&self, playing: Playing) -> Played {
        let draws = Draws::draw(&mut self.rng.lock());
        let id = self.pids.fetch_add(1, Ordering::Relaxed);
        let control = Arc::new(Control::default());
        self.playing.lock().push(Arc::downgrade(&control));
        let (passing, passed) = mpsc::channel(4);
        let dead = CancellationToken::new();
        let opener = Passing {
            streams: passing,
            control: Arc::clone(&control),
            dead: dead.clone(),
        };
        let (stdout, stdout_written) = tokio::io::duplex(PIPE);
        let (stderr, stderr_written) = tokio::io::duplex(PIPE);
        let exit = Arc::new(Ended::default());
        let (said, saying) = mpsc::unbounded_channel();
        let source = Saying {
            inner: self.serving.factory(),
            said: said.clone(),
        };
        let destination = Saying {
            inner: destination_factory::<SimDestination>(),
            said,
        };
        let served = Served::new()
            .with_source(Box::new(source))
            .with_destination(Box::new(destination));
        let limits = limits(&playing.args);
        let serving = tokio::spawn(serve_streams(
            Arc::new(served),
            passed,
            limits,
            std::future::pending(),
        ));
        tokio::spawn(life(Life {
            playing,
            control,
            draws,
            serving: serving.abort_handle(),
            dead,
            stdout: stdout_written,
            stderr: stderr_written,
            saying,
            exited: Arc::clone(&exit),
        }));
        Played {
            id,
            opener: Box::new(opener),
            stdout: Box::new(stdout),
            stderr: Box::new(stderr),
            exit,
        }
    }
}

/// The limits a spawned connector serves within, as its arguments state them.
fn limits(args: &[OsString]) -> rdlt_wire::Limits {
    let state = args.iter().find_map(|arg| {
        let bytes = arg.to_str()?.strip_prefix("--max-state-bytes=")?;
        bytes.parse().ok()
    });
    match state {
        Some(state_bytes) => rdlt_wire::Limits {
            state_bytes,
            ..rdlt_wire::Limits::default()
        },
        None => rdlt_wire::Limits::default(),
    }
}

/// A simulated connector's factory that says, on its process's standard error, the canary its
/// configuration's `told.canary` resolved to, as a connector logging its configuration does.
struct Saying<F: ?Sized> {
    inner: Box<F>,
    said: mpsc::UnboundedSender<String>,
}

impl<F: ?Sized> Saying<F> {
    fn say(&self, config: &serde_json::Value) {
        let told = config
            .pointer("/told/canary")
            .and_then(serde_json::Value::as_str);
        if let Some(canary) = told {
            self.said.send(canary.to_owned()).ok();
        }
    }
}

impl SourceFactory for Saying<dyn SourceFactory> {
    fn spec(&self) -> &ConnectorSpec {
        self.inner.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        self.say(&config);
        self.inner.connect(config, context)
    }
}

impl DestinationFactory for Saying<dyn DestinationFactory> {
    fn spec(&self) -> &ConnectorSpec {
        self.inner.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        self.say(&config);
        self.inner.connect(config, context)
    }
}

/// Carries what `from` reads to `to` until either ends, holding it while the process is stalled,
/// as a stopped process reads and writes nothing.
async fn carry(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    control: &Control,
) {
    let mut buffer = vec![0; CARRY];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        control.going().await;
        if to.write_all(&buffer[..read]).await.is_err() {
            break;
        }
    }
    to.shutdown().await.ok();
}

/// Carries bytes both ways between the connector's end of one of the host's streams and the
/// served connector.
async fn relay(host: DuplexStream, served: DuplexStream, control: Arc<Control>) {
    let (from_host, to_host) = tokio::io::split(host);
    let (from_served, to_served) = tokio::io::split(served);
    tokio::join!(
        carry(from_host, to_served, &control),
        carry(from_served, to_host, &control)
    );
}

/// A played process from its start to its end.
struct Life {
    playing: Playing,
    control: Arc<Control>,
    draws: Draws,
    serving: AbortHandle,
    /// Cancelled as the process ends, which cuts every stream it was passed.
    dead: CancellationToken,
    stdout: DuplexStream,
    stderr: DuplexStream,
    saying: mpsc::UnboundedReceiver<String>,
    exited: Arc<Ended<ExitStatus>>,
}

/// How a played process ended.
#[derive(Clone, Copy)]
enum End {
    Killed,
    Stopped,
    Exited,
}

impl End {
    /// The status a process that ended so is reaped with.
    fn status(self) -> ExitStatus {
        match self {
            // Killed by SIGKILL.
            Self::Killed => ExitStatus::from_raw(9),
            Self::Stopped => ExitStatus::from_raw(0),
            Self::Exited => ExitStatus::from_raw(3 << 8),
        }
    }
}

/// Resolves when `killed` is cancelled; never, where nothing may kill the process.
async fn killed(killed: Option<&CancellationToken>) {
    match killed {
        Some(killed) => killed.cancelled().await,
        None => std::future::pending().await,
    }
}

/// Says `canary` on `stderr`, at the end of a line as a connector logging its configuration
/// does, and where `long`, also where the host's read of a long line stops within it.
async fn say(stderr: &mut DuplexStream, canary: &str, long: bool) {
    let mut said = format!("configured with {canary}\n");
    if long {
        said.push_str(&"x".repeat(TAIL_BYTES - 3));
        said.push_str(canary);
        said.push('\n');
    }
    stderr.write_all(said.as_bytes()).await.ok();
}

async fn life(life: Life) {
    let Life {
        playing,
        control,
        draws,
        serving,
        dead,
        mut stdout,
        mut stderr,
        mut saying,
        exited,
    } = life;
    for _ in 0..draws.flood {
        // A connector serves on its streams: the host logs what it writes here as a fault.
        if stdout
            .write_all(b"a connector's own noise\n")
            .await
            .is_err()
        {
            break;
        }
    }
    let (mut canary, mut listening) = (None::<String>, true);
    let end = loop {
        tokio::select! {
            biased;
            // A kill ends the process at once, whatever it was doing; a stop comes next.
            () = killed(playing.killed.as_ref()) => break End::Killed,
            () = playing.stopped.cancelled() => break End::Stopped,
            () = control.ends.notified() => break End::Exited,
            said = saying.recv(), if listening => match said {
                Some(said) => {
                    say(&mut stderr, &said, draws.long).await;
                    canary = Some(said);
                }
                None => listening = false,
            },
        }
    };
    match end {
        End::Killed => {}
        End::Stopped => tokio::time::sleep(draws.lingers).await,
        End::Exited => {
            let said = canary.as_deref().unwrap_or("nothing said");
            stderr
                .write_all(format!("exiting with {said}\n").as_bytes())
                .await
                .ok();
        }
    }
    serving.abort();
    dead.cancel();
    drop((stdout, stderr));
    exited.set(end.status());
}
