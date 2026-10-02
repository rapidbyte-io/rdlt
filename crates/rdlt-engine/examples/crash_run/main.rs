//! A pipeline run in a process of its own, as the CLI will run one: the failpoint sweep crashes it
//! at the engine's durability steps, and the kill matrix kills it or its spawned connectors, then
//! each runs it again and checks every row landed once.
//!
//! `crash_run <config.json>` runs the pipeline the file describes once, retrying, and exits 0
//! where the run succeeded and 1 where it failed; `FAILPOINTS` crashes it where it names, and it
//! leaves no core dump when it does. It tells each connector it spawns by its process id (`connector 4321`), each read as it begins
//! (`read 2`), each commit as it lands (`commit 3`), a kill as it makes it (`killed reading 1`,
//! the source's reads then in flight), and last its report, as JSON; told to pause after a read
//! or commit, it waits there to be killed. Before it exits it stops what it spawned.

#![forbid(unsafe_code)]

mod config;
mod watch;

use std::io::Write as _;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use rdlt_connector::{ConnectContext, Destination, Source, destination_factory, source_factory};
use rdlt_connector_reference::{
    ChangesSource, FilesDestination, GeneratorSource, LogSource, SqliteDestination,
};
use rdlt_engine::{Engine, LocalWal, RayonPool, RunStatus, SystemEnv};
use rdlt_host::{ConnectorRef, Kills, Local, Options, Provider as _};

use config::{Config, Victim};
use watch::Watch;

fn main() -> ExitCode {
    undumped();
    let _failpoints = fail::FailScenario::setup();
    let Some(path) = std::env::args().nth(1) else {
        writeln!(std::io::stderr(), "usage: crash_run <config.json>").ok();
        return ExitCode::from(2);
    };
    let config = std::fs::read(&path)
        .map_err(|error| error.to_string())
        .and_then(|bytes| {
            serde_json::from_slice::<Config>(&bytes).map_err(|error| error.to_string())
        });
    let config = match config {
        Ok(config) => config,
        Err(error) => {
            writeln!(std::io::stderr(), "crash_run: {path}: {error}").ok();
            return ExitCode::from(2);
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime starts");
    let ran = runtime.block_on(run(&config));
    drop(runtime);
    // A host owns the process groups of the connectors it spawned: each is stopped, and seen
    // empty, before the host exits.
    let stopped = rdlt_host::stop_spawned(std::time::Duration::from_secs(20));
    match ran.and(stopped.map_err(|lingering| lingering.to_string())) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            writeln!(std::io::stderr(), "crash_run: {error}").ok();
            ExitCode::FAILURE
        }
    }
}

/// Turns core dumps of this process off, and says so once they are.
///
/// The harness aborts at a failpoint by design, hundreds of times a sweep: each abort would
/// otherwise be handed to the system's core-dump handler, which costs seconds of processor
/// time and fills its journal. The abort itself is untouched: nothing unwinds or is flushed.
/// On Linux the process is marked non-dumpable, which a handler fed through a pipe heeds where
/// it ignores a size limit; elsewhere, as on macOS, the core size limit is set to nothing.
fn undumped() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let none = Rlimit {
        current: Some(0),
        maximum: Some(0),
    };
    setrlimit(Resource::Core, none).ok();
    let limited = getrlimit(Resource::Core).current == Some(0);
    #[cfg(target_os = "linux")]
    let off = {
        use rustix::process::{DumpableBehavior, dumpable_behavior, set_dumpable_behavior};
        set_dumpable_behavior(DumpableBehavior::NotDumpable).ok();
        limited && dumpable_behavior().is_ok_and(|now| now == DumpableBehavior::NotDumpable)
    };
    #[cfg(not(target_os = "linux"))]
    let off = limited;
    if off {
        writeln!(std::io::stdout(), "core dumps off").ok();
    }
}

/// Runs the pipeline `config` describes once.
async fn run(config: &Config) -> Result<(), String> {
    let kills = Kills::new();
    let killed = config.kill.map(|kill| kill.victim);
    let source = source(config, (killed == Some(Victim::Source)).then_some(&kills)).await?;
    let destination = destination(
        config,
        (killed == Some(Victim::Destination)).then_some(&kills),
    )
    .await?;
    let kill = config.kill.map(|kill| (kills, kill));
    let watch = Arc::new(Watch::new(kill, config.pause));
    let source = watch::source(source, Arc::clone(&watch));
    let destination = watch::destination(destination, watch);
    let pool =
        RayonPool::new(NonZeroUsize::new(2).expect("two")).map_err(|error| error.to_string())?;
    let mut env = SystemEnv::new(pool);
    if let Some(wal) = &config.wal {
        env = env.with_wal(Arc::new(LocalWal::new(wal)));
    }
    let engine = Engine::new(config.engine()?, Arc::new(env));
    let outcome = engine.run(config.plan()?, source, destination).await;
    let report = serde_json::to_string(&outcome.report).map_err(|error| error.to_string())?;
    writeln!(std::io::stdout(), "{report}").ok();
    match (outcome.report.status, outcome.error) {
        (RunStatus::Succeeded, _) => Ok(()),
        (status, error) => Err(format!("the run ended {status:?}: {error:?}")),
    }
}

/// The run's source, in this process or a process of its own, killed by `kills` where given.
async fn source(config: &Config, kills: Option<&Kills>) -> Result<Arc<dyn Source>, String> {
    let place = &config.source;
    let (id, served) = match place.kind.as_str() {
        "generator" => ("io.rapidbyte.generator", "serve_generator"),
        "changes" => ("io.rapidbyte.changes", "serve_changes"),
        "log" => ("io.rapidbyte.log", "serve_log"),
        other => return Err(format!("no source {other}")),
    };
    if place.spawned {
        let placed = host(kills)
            .source(&reference(id, served, place)?, &place.config)
            .await
            .map_err(|error| error.to_string())?;
        return Ok(Arc::from(placed.connector));
    }
    let factory = match place.kind.as_str() {
        "generator" => source_factory::<GeneratorSource>(),
        "log" => source_factory::<LogSource>(),
        _ => source_factory::<ChangesSource>(),
    };
    let connected = factory
        .connect(place.config.clone(), ConnectContext::new())
        .await
        .map_err(|error| error.to_string())?;
    Ok(Arc::from(connected))
}

/// The run's destination, in this process or a process of its own, killed by `kills` where given.
async fn destination(
    config: &Config,
    kills: Option<&Kills>,
) -> Result<Arc<dyn Destination>, String> {
    let place = &config.destination;
    let (id, served) = match place.kind.as_str() {
        "sqlite" => ("io.rapidbyte.sqlite", "serve_sqlite"),
        "files" => ("io.rapidbyte.files", "serve_files"),
        other => return Err(format!("no destination {other}")),
    };
    if place.spawned {
        let placed = host(kills)
            .destination(&reference(id, served, place)?, &place.config)
            .await
            .map_err(|error| error.to_string())?;
        return Ok(Arc::from(placed.connector));
    }
    let factory = match place.kind.as_str() {
        "sqlite" => destination_factory::<SqliteDestination>(),
        _ => destination_factory::<FilesDestination>(),
    };
    let connected = factory
        .connect(place.config.clone(), ConnectContext::new())
        .await
        .map_err(|error| error.to_string())?;
    Ok(Arc::from(connected))
}

/// Bytes of credit a spawned source reads within: less than a batch, so a source is never
/// more than a frame ahead of what the engine took, and one with more to send than the run
/// holds is still reading when a commit is asked, however slowly the host runs.
const READ_WINDOW: u64 = 512;

/// The host spawning connectors, killing them by `kills` where given.
fn host(kills: Option<&Kills>) -> Local {
    // Each connector is told as it is spawned, by its process id: the id of the process group
    // it leads, which what watches the run checks is gone once the run has ended.
    let options = Options {
        read_window: READ_WINDOW,
        ..Options::default()
    };
    let local = Local::trusting_binaries()
        .env_passthrough("LLVM_PROFILE_FILE")
        .options(options)
        .on_spawn(|connector| {
            writeln!(std::io::stdout(), "connector {connector}").ok();
        });
    match kills {
        Some(kills) => local.kills(kills),
        None => local,
    }
}

/// The reference to the connector `id`, served by the example `served` beside this one, or
/// started by `place`'s launcher where it names one.
fn reference(id: &str, served: &str, place: &config::Place) -> Result<ConnectorRef, String> {
    let id = rdlt_connector::ConnectorId::parse(id).map_err(|error| error.to_string())?;
    let here = std::env::current_exe().map_err(|error| error.to_string())?;
    let beside: PathBuf = here
        .parent()
        .ok_or("this example has no directory")?
        .join(served);
    let path = place.launcher.clone().unwrap_or(beside);
    Ok(ConnectorRef::new(id).path(path))
}
