//! The bench's own binary run as a served connector: the replay source and the bench's sink in a
//! process of their own, held to the connectors' cores where the bench names them.

use std::process::ExitCode;
use std::sync::Once;

use rdlt_connector::serve::Served;
use rdlt_connector::{BoxFuture, ConnectContext, ConnectorSpec, Result, Source, SourceFactory};
use rdlt_engine::bench::{Replayed, register, replay_factory, sink_factory};

use crate::Frames;
use crate::shaped::Shaped;

/// The variable naming the CPUs a served connector's process runs on, a list such as `4-11`; a
/// connector started without it runs where its host does.
pub(crate) const CORES: &str = "RDLT_BENCH_CONNECTOR_CORES";

/// Whether this process was started as a connector: by a host that passed it a socket, or to
/// listen for hosts.
pub(crate) fn asked() -> bool {
    std::env::args().skip(1).any(|arg| {
        let flag = arg.split_once('=').map_or(arg.as_str(), |(flag, _)| flag);
        flag == "--rdlt-fd" || flag == "--listen"
    })
}

/// Serves the replay source and the sink as a connector binary's `main` does, on the
/// connectors' cores.
pub(crate) fn serve() -> ExitCode {
    // Before the runtime starts, so its workers are as many as the connectors' cores.
    pin();
    Served::serve(|| {
        Served::new()
            .with_source(Box::new(Generating(replay_factory())))
            .with_destination(sink_factory())
    })
}

/// Holds this process to the CPUs [`CORES`] names, where it names any.
#[cfg(target_os = "linux")]
fn pin() {
    let Ok(list) = std::env::var(CORES) else {
        return;
    };
    let mut cpus = nix::sched::CpuSet::new();
    for cpu in cpu_list(&list) {
        cpus.set(cpu)
            .expect("each of the connectors' CPUs is one Linux can name");
    }
    let this = nix::unistd::Pid::from_raw(0);
    nix::sched::sched_setaffinity(this, &cpus).expect("the connectors' CPUs are online");
}

/// Runs where its host does: only Linux holds a process to chosen CPUs.
#[cfg(not(target_os = "linux"))]
fn pin() {}

/// The CPUs of a list such as `0-3,8`.
#[cfg(target_os = "linux")]
fn cpu_list(list: &str) -> Vec<usize> {
    let number = |text: &str| -> usize {
        text.parse()
            .expect("the connectors' CPUs are a list such as 4-11")
    };
    list.split(',')
        .flat_map(|part| match part.split_once('-') {
            Some((low, high)) => number(low)..=number(high),
            None => number(part)..=number(part),
        })
        .collect()
}

/// The replay source, its pushes made in this process the first time a host asks for them.
struct Generating(Box<dyn SourceFactory>);

/// A replay a host may ask a source served here for.
#[derive(Clone, Copy)]
enum Asked {
    Frames(Frames),
    Shaped(Shaped),
}

impl Asked {
    /// The replay named `name`.
    fn named(name: &str) -> Option<Self> {
        let frames = Frames::named(name).map(Self::Frames);
        frames.or_else(|| Shaped::named(name).map(Self::Shaped))
    }

    /// The replay's place in [`MADE`]: each frame size's, then each other workload's.
    fn index(self) -> usize {
        match self {
            Self::Frames(frames) => frames.index(),
            Self::Shaped(shaped) => Frames::ALL.len() + shaped.index(),
        }
    }

    /// Makes the replay's pushes and registers them under its name.
    fn register(self) {
        match self {
            Self::Frames(frames) => register(frames.replay(), Replayed::Batches(frames.batches())),
            Self::Shaped(shaped) => register(shaped.replay(), shaped.replayed()),
        }
    }
}

/// Each replay's pushes, made once in this process.
static MADE: [Once; 5] = [const { Once::new() }; 5];

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
