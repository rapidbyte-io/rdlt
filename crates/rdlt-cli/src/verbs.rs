//! Each verb, run with the output mode and the host the command line names.

mod init;
mod inspect;
mod run;
mod state;
#[cfg(test)]
mod tests;

use std::panic::{AssertUnwindSafe, catch_unwind};

use clap::CommandFactory as _;
use rdlt::{Interrupts, StopsSpawned};
use tokio::runtime::{Builder, Runtime};

use crate::Exit;
use crate::args::{Cli, ConnectorAction, HostArgs, Lang, StateAction, Verb};
use crate::output::{self, Failure, Mode};
use crate::stopping::{self, STOPPING};

/// Runs `cli`'s verb and answers how it ended.
pub(crate) fn dispatch(cli: Cli) -> Exit {
    let mode = Mode::of(cli.output);
    let runtime = match Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let failure = Failure::io("starting the runtime", &error);
            output::failed_at_start(mode, &failure);
            return failure.exit;
        }
    };
    // The export needs a runtime, and flushes on it: it installs once one runs, and ends first.
    let telemetry = {
        let _entered = runtime.enter();
        output::logging(&cli.log_level)
    };
    on_runtime(runtime, telemetry, |runtime| match &cli.verb {
        // Certification runs its own runtimes, on this thread, which is on none.
        Verb::Certify(args) => crate::certify::certify(args, &cli.host, mode),
        _ => ran(runtime, cli, mode),
    })
}

/// What `verb` answers, run with `runtime`: a panic is a defect in rdlt, and the process leaves
/// no task behind it, so what runs on the runtime's blocking pool, a read past its wait among
/// it, cannot hold the exit.
fn on_runtime(
    runtime: Runtime,
    telemetry: Option<rdlt::telemetry::Telemetry>,
    verb: impl FnOnce(&Runtime) -> Exit,
) -> Exit {
    let contained = catch_unwind(AssertUnwindSafe(|| verb(&runtime)));
    // The export flushes through the runtime, so it ends before the runtime does.
    drop(telemetry);
    runtime.shutdown_background();
    contained.unwrap_or(Exit::Internal)
}

/// Runs `cli`'s verb on `runtime`: however it ends, each connector it spawned is stopped with its
/// process group, and a signal that comes while they stop stops them at once.
fn ran(runtime: &Runtime, cli: Cli, mode: Mode) -> Exit {
    let listening = {
        let _entered = runtime.enter();
        Interrupts::listen()
    };
    let mut interrupts = match listening {
        Ok(interrupts) => interrupts,
        Err(error) => {
            return runtime.block_on(ended(mode, Failure::io("listening for signals", &error)));
        }
    };
    let stops = StopsSpawned::within(STOPPING);
    let exit = runtime.block_on(async {
        match verb(cli, mode, &mut interrupts).await {
            Ok(exit) => exit,
            Err(failure) => ended(mode, failure).await,
        }
    });
    let heard = matches!(exit, Exit::Signalled(_)).then_some(i32::from(exit.code()));
    let (stopped, _) = stopping::stopped(runtime, stops, &mut interrupts, heard);
    if let Err(lingering) = stopped {
        output::note(&format!("rdlt: {lingering}"));
    }
    exit
}

/// Prints `failure`, and answers how the verb ended.
async fn ended(mode: Mode, failure: Failure) -> Exit {
    output::failed(mode, &failure).await;
    failure.exit
}

async fn verb(cli: Cli, mode: Mode, interrupts: &mut Interrupts) -> Result<Exit, Failure> {
    let Cli { verb, host, .. } = cli;
    if let Verb::Run {
        document,
        events,
        report,
    } = verb
    {
        let events = events.as_deref();
        let report = report.as_deref();
        return run::run(&document, events, report, &host, mode, interrupts).await;
    }
    tokio::select! {
        biased;
        // A signal wins, so a verb still placing connectors ends at once.
        status = interrupts.heard() => Err(Failure::signalled(status)),
        exit = other(verb, host, mode) => exit,
    }
}

async fn other(verb: Verb, host: HostArgs, mode: Mode) -> Result<Exit, Failure> {
    match verb {
        Verb::Init { path } => init::init(&path, mode).await,
        Verb::Check { document } => inspect::check(&document, &host, mode).await,
        Verb::Discover { document } => inspect::discover(&document, &host, mode).await,
        Verb::Schema {
            document,
            connector,
            role,
        } => inspect::schema(document, connector.as_deref(), role, &host).await,
        Verb::Connectors => inspect::connectors(&host, mode).await,
        Verb::State {
            action:
                StateAction::Reset {
                    document,
                    streams,
                    tables,
                },
        } => state::reset(&document, &streams, tables, &host, mode).await,
        Verb::Completions { shell } => {
            let mut text = Vec::new();
            clap_complete::generate(shell, &mut Cli::command(), "rdlt", &mut text);
            output::stdout(&String::from_utf8_lossy(&text)).await?;
            Ok(Exit::Success)
        }
        Verb::Connector {
            action:
                ConnectorAction::New {
                    name,
                    lang: Lang::Rust,
                    into,
                },
        } => crate::connector::new(&name, &into, mode).await,
        Verb::Run { .. } => unreachable!("a run is dispatched before the other verbs"),
        Verb::Certify(_) => unreachable!("certification is dispatched before any verb runs"),
    }
}
