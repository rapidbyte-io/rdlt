//! Served connectors in processes of their own, as in production: spawned by the host over a
//! socket pair, or listening for it over mutual TLS on loopback; and each process's CPU time, whole
//! and as user and system time.
//!
//! Each process is the bench's own binary, which serves the replay source and the IPC sink when
//! started as a connector, held to the connectors' cores where the bench's environment names them.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Once};
use std::time::Duration;

use rdlt_connector::serve::Served;
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorSpec, Destination, Result, Source, SourceFactory,
};
use rdlt_engine::bench::{Replayed, TablesCell, register, replay_factory, sink_factory};
use rdlt_host::{ConnectorRef, Local, Options, Provider as _};
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};

use crate::connector::CORES;
use crate::shaped::Shaped;
use crate::{Frames, Split, Tls, Transport};

/// How long a listening connector has to say where it listens.
const STARTING: Duration = Duration::from_secs(60);

/// What the bench's binary serves when started as a connector: the replay source, whose pushes
/// it makes the first time a host asks for them, and the IPC sink.
pub(crate) fn served() -> Served {
    Served::new()
        .with_source(Box::new(Generating(replay_factory())))
        .with_destination(sink_factory())
}

/// The replay source, its pushes made in this process the first time a host asks for them.
struct Generating(Box<dyn SourceFactory>);

/// A replay a host may ask a source served here for.
#[derive(Clone, Copy)]
enum Asked {
    Frames(Frames),
    Shaped(Shaped),
    Tables(TablesCell),
}

impl Asked {
    /// The replay named `name`.
    fn named(name: &str) -> Option<Self> {
        let frames = Frames::named(name).map(Self::Frames);
        let shaped = || Shaped::named(name).map(Self::Shaped);
        frames
            .or_else(shaped)
            .or_else(|| TablesCell::named(name).map(Self::Tables))
    }

    /// The replay's place in [`MADE`]: each frame size's, then each other workload's, then each
    /// many-table cell's.
    fn index(self) -> usize {
        let after_frames = Frames::ALL.len();
        let after_shaped = after_frames + Shaped::ALL.len();
        match self {
            Self::Frames(frames) => frames.index(),
            Self::Shaped(shaped) => after_frames + shaped.index(),
            Self::Tables(cell) => after_shaped + cell.index(),
        }
    }

    /// Makes the replay's pushes and registers them under its name.
    fn register(self) {
        match self {
            Self::Frames(frames) => register(frames.replay(), Replayed::Batches(frames.batches())),
            Self::Shaped(shaped) => register(shaped.replay(), shaped.replayed()),
            Self::Tables(cell) => register(&cell.replay(), cell.replayed()),
        }
    }
}

/// Each replay's pushes, made once in this process.
static MADE: [Once; Frames::ALL.len() + Shaped::ALL.len() + TablesCell::ALL.len()] =
    [const { Once::new() }; Frames::ALL.len() + Shaped::ALL.len() + TablesCell::ALL.len()];

impl SourceFactory for Generating {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Source>>> {
        let name = config.get("name").and_then(serde_json::Value::as_str);
        let asked = name.and_then(Asked::named);
        Box::pin(async move {
            if let Some(asked) = asked {
                let making = tokio::task::spawn_blocking(move || {
                    MADE[asked.index()].call_once(|| asked.register());
                });
                making.await.expect("the pushes are made");
            }
            self.0.connect(config, context).await
        })
    }
}

/// A connector served in a process of its own, and that process.
pub(crate) struct Placed<T> {
    pub(crate) connector: T,
    pub(crate) pid: u32,
}

/// Places the bench's connectors in processes of their own.
pub(crate) struct Processes {
    binary: PathBuf,
    local: Local,
    spawned: Receiver<u32>,
    listeners: Option<Listeners>,
}

impl Processes {
    /// Spawns connectors from this bench's own binary, passing them the connectors' cores.
    pub(crate) fn new() -> Self {
        let binary = std::env::current_exe().expect("the bench's binary has a path");
        let (told, spawned) = channel();
        let local = Local::trusting_binaries()
            .env_passthrough(CORES)
            .on_spawn(move |pid| told.send(pid).expect("the bench hears of each spawn"));
        Self {
            binary,
            local,
            spawned,
            listeners: None,
        }
    }

    /// A source configured as `config`, reached over `transport`.
    pub(crate) async fn source(
        &mut self,
        transport: Transport,
        config: &serde_json::Value,
        options: &Options,
    ) -> Placed<Arc<dyn Source>> {
        let id = replay_factory().spec().id.clone();
        match transport {
            Transport::Socket => {
                let reference = ConnectorRef::new(id).path(&self.binary);
                let local = self.local.clone().options(*options);
                let placed = local.source(&reference, config).await;
                let connector = Arc::from(placed.expect("the source is spawned").connector);
                let pid = self.spawned();
                Placed { connector, pid }
            }
            Transport::Tls => {
                let listeners = self.listeners().await;
                let (endpoint, pid) = (&listeners.source.endpoint, listeners.source.pid);
                let reference = ConnectorRef::new(id).endpoint(endpoint);
                let placed = listeners
                    .tls
                    .remote(options)
                    .source(&reference, config)
                    .await;
                let connector = Arc::from(placed.expect("the source is placed").connector);
                Placed { connector, pid }
            }
        }
    }

    /// A sink configured as `config`, reached over `transport`.
    pub(crate) async fn destination(
        &mut self,
        transport: Transport,
        config: &serde_json::Value,
        options: &Options,
    ) -> Placed<Arc<dyn Destination>> {
        let id = sink_factory().spec().id.clone();
        match transport {
            Transport::Socket => {
                let reference = ConnectorRef::new(id).path(&self.binary);
                let local = self.local.clone().options(*options);
                let placed = local.destination(&reference, config).await;
                let connector = Arc::from(placed.expect("the destination is spawned").connector);
                let pid = self.spawned();
                Placed { connector, pid }
            }
            Transport::Tls => {
                let listeners = self.listeners().await;
                let (endpoint, pid) = (&listeners.destination.endpoint, listeners.destination.pid);
                let reference = ConnectorRef::new(id).endpoint(endpoint);
                let remote = listeners.tls.remote(options);
                let placed = remote.destination(&reference, config).await;
                let connector = Arc::from(placed.expect("the destination is placed").connector);
                Placed { connector, pid }
            }
        }
    }

    /// The process of the connector spawned last.
    fn spawned(&self) -> u32 {
        self.spawned
            .try_iter()
            .last()
            .expect("a placed connector was spawned")
    }

    /// The listening source and sink, started the first time a case asks for them.
    async fn listeners(&mut self) -> &Listeners {
        if self.listeners.is_none() {
            let tls = Tls::new();
            let source = Listening::start(&self.binary, &tls).await;
            let destination = Listening::start(&self.binary, &tls).await;
            self.listeners = Some(Listeners {
                tls,
                source,
                destination,
            });
        }
        self.listeners.as_ref().expect("the listeners started")
    }
}

/// A source and a sink, each listening in a process of its own over mutual TLS.
struct Listeners {
    tls: Tls,
    source: Listening,
    destination: Listening,
}

/// A connector listening over mutual TLS on loopback in a process of its own.
struct Listening {
    /// Killed when dropped.
    _child: Child,
    pid: u32,
    endpoint: String,
}

impl Listening {
    /// Starts `binary` listening on a free loopback port, accepting the host of `tls`.
    async fn start(binary: &Path, tls: &Tls) -> Self {
        let mut child = Command::new(binary)
            .args(["--listen", "127.0.0.1:0"])
            .arg("--tls-cert")
            .arg(&tls.server.cert)
            .arg("--tls-key")
            .arg(&tls.server.key)
            .arg("--tls-client-ca")
            .arg(tls.pki.ca())
            .args(["--tls-allow-host", "host"])
            .stdout(Stdio::piped())
            // It notes each host it serves, a line a run.
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0)
            .spawn()
            .expect("the listening connector starts");
        let pid = child.id().expect("the listening connector runs");
        rdlt_testkit::process::guard(pid).expect("the listening connector is guarded");
        let stdout = child.stdout.take().expect("its output is piped");
        let line = tokio::time::timeout(STARTING, BufReader::new(stdout).lines().next_line())
            .await
            .expect("the connector says where it listens in time")
            .expect("its output reads");
        let address = line
            .as_deref()
            .and_then(|line| line.strip_prefix("listening on "));
        let address: std::net::SocketAddr = address
            .and_then(|address| address.parse().ok())
            .expect("the connector says where it listens");
        Self {
            _child: child,
            pid,
            endpoint: format!("grpcs://localhost:{}", address.port()),
        }
    }
}

/// The CPU time, user and system, that the process `pid` has taken so far; none where the
/// platform does not tell one process another's.
#[cfg(target_os = "linux")]
#[expect(
    clippy::unnecessary_wraps,
    reason = "it has the signature of the other platforms', which tell none"
)]
pub(crate) fn cpu(pid: u32) -> Option<Duration> {
    let pid = i32::try_from(pid).expect("a process id fits a pid_t");
    let clock = nix::time::ClockId::pid_cpu_clock_id(nix::unistd::Pid::from_raw(pid));
    let clock = clock.expect("a connector's process has a CPU clock while it runs");
    let time = nix::time::clock_gettime(clock).expect("a connector's CPU clock reads");
    Some(Duration::from(time))
}

/// None: only Linux tells one process another's CPU time without `unsafe`.
#[cfg(not(target_os = "linux"))]
pub(crate) fn cpu(_pid: u32) -> Option<Duration> {
    None
}

/// The user and system time, in clock ticks, that the process `pid` has taken so far; none where
/// the platform does not tell one process another's, or its stat cannot be read.
#[cfg(target_os = "linux")]
pub(crate) fn split(pid: u32) -> Option<Split> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // After the command's name, which may hold spaces, the state is the first field, the user
    // time the twelfth and the system time the thirteenth.
    let (_, fields) = stat.rsplit_once(')')?;
    let mut fields = fields.split_whitespace().skip(11);
    let user = fields.next()?.parse().ok()?;
    let system = fields.next()?.parse().ok()?;
    Some(Split { user, system })
}

/// None: the platform does not tell one process another's CPU time.
#[cfg(not(target_os = "linux"))]
pub(crate) fn split(_pid: u32) -> Option<Split> {
    None
}
