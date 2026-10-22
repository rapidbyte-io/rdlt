//! Connectors spawned through `Local` as real processes, beyond what a simulated seed runs: loads
//! whose source is killed, stalled with `SIGSTOP`, crashing and flooding its output, saying a
//! secret that ends in whitespace; and starts racing on real threads for the last of the groups a
//! process may own.
//!
//! These run real processes on the real clock, so a failure names its round and does not replay.

use std::convert::Infallible;
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use rdlt_connector::{ConnectorId, PipelineId, StreamName};
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, PipelinePlan, RetryPolicy, RunOutcome, RunStatus,
    StreamPlan,
};
use rdlt_host::{ConnectorRef, Kills, Local, Options, Provider as _, ProviderError, Wire};
use tokio::task::JoinSet;

use crate::canary::{Logged, Vault, clean, texts};
use crate::process::{local, published_ids, scripted};
use crate::support::{memory_destination, system_env};

/// The groups a process may own at once, those still starting among them (ADR 0050).
const OWNED: usize = 1024;

/// How many starts race for the last group in each round.
const RACERS: usize = 32;

/// How many rounds of racing starts the test runs.
const ROUNDS: usize = 10;

/// How long a stopped connector's group may take to end.
const ENDING: Duration = Duration::from_secs(20);

/// A connector that serves nothing and ends at its stop: a shell that becomes `sleep`.
fn sleeper(directory: &Path) -> ConnectorRef {
    let path = directory.join("rdlt-connector-sleeper");
    std::fs::write(&path, "#!/bin/sh\nexec sleep 600\n").expect("the script writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
        .expect("the script is executable");
    ConnectorRef::new(ConnectorId::parse("test.sleeper").expect("a valid id")).path(path)
}

/// Raises this process's limit on open descriptors to its hard limit: each connector holds four.
fn descriptors_raised() {
    use nix::sys::resource::{Resource, getrlimit, setrlimit};
    let (_, hard) = getrlimit(Resource::RLIMIT_NOFILE).expect("the limit reads");
    setrlimit(Resource::RLIMIT_NOFILE, hard, hard).expect("the limit rises");
}

/// Starts `count` connectors of `reference`, sixty-four at a time.
async fn started(local: &Local, reference: &ConnectorRef, count: usize) -> Vec<Wire> {
    let mut wires = Vec::with_capacity(count);
    while wires.len() < count {
        let mut starting = JoinSet::new();
        for _ in 0..(count - wires.len()).min(64) {
            let (local, reference) = (local.clone(), reference.clone());
            starting.spawn(async move { local.wire(&reference).await });
        }
        while let Some(joined) = starting.join_next().await {
            wires.push(joined.expect("a start ends").expect("a connector starts"));
        }
    }
    wires
}

/// Waits, for [`ENDING`] at most, until this process owns `count` groups.
async fn owning(count: usize) -> usize {
    let until = tokio::time::Instant::now() + ENDING;
    while rdlt_host::spawned().len() != count && tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    rdlt_host::spawned().len()
}

// The real clock: the race is between real threads, and the groups end as real processes do.
#[test]
#[ignore = "starts a thousand processes; run it with `just soak`"]
fn of_starts_racing_for_the_last_group_of_a_process_one_alone_starts() {
    descriptors_raised();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a runtime");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let (local, sleeper) = (local(), sleeper(directory.path()));
    runtime.block_on(async {
        let held = started(&local, &sleeper, OWNED - 1).await;
        for round in 0..ROUNDS {
            let mut racing = JoinSet::new();
            for _ in 0..RACERS {
                let (local, sleeper) = (local.clone(), sleeper.clone());
                racing.spawn(async move { local.wire(&sleeper).await });
            }
            let (mut won, mut refused) = (Vec::new(), 0);
            while let Some(joined) = racing.join_next().await {
                match joined.expect("a start ends") {
                    Ok(wire) => won.push(wire),
                    Err(ProviderError::SpawnFailed { .. }) => refused += 1,
                    Err(other) => panic!("round {round}: a start failed otherwise: {other}"),
                }
            }
            assert_eq!(
                won.len(),
                1,
                "round {round}: {} of {RACERS} starts started and {refused} were refused",
                won.len()
            );
            drop(won);
            // The winner's group ends before the next round races for its place.
            assert_eq!(owning(OWNED - 1).await, OWNED - 1, "round {round}");
        }
        drop(held);
    });
    assert_eq!(rdlt_host::stop_spawned(ENDING), Ok(()));
}

/// How long a round's run may take.
const ROUND: Duration = Duration::from_secs(120);

/// A round's options: a heartbeat every 100 ms, three missed losing a connector, so that a stall
/// of a few hundred milliseconds loses one and a shorter one does not.
fn options() -> Options {
    Options {
        heartbeat: Duration::from_millis(100),
        missed: NonZeroU32::new(3).expect("three is not zero"),
        ..Options::default()
    }
}

/// An engine that commits every 500 rows and tries a failed attempt again a hundred times, never
/// waiting a second for one: a streak of attempts killed before a commit would otherwise back off
/// for minutes, past the round's bound, and read as a hang.
fn engine() -> Engine {
    let policy = CommitPolicy::new(None, Some(500), None).expect("a row threshold is valid");
    let retry = RetryPolicy::default()
        .max_attempts(100)
        .initial(Duration::from_millis(10))
        .max_delay(Duration::from_secs(1));
    let config = EngineConfig::builder()
        .commit(policy)
        .retry(retry)
        .build()
        .expect("the engine's configuration is valid");
    Engine::new(config, Arc::new(system_env()))
}

/// Draws from a seed a failure names.
struct Draws(u64);

impl Draws {
    /// A value in `0..bound`.
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = rdlt_testkit::draw::mix(self.0);
        self.0 % bound
    }
}

/// How long the soak loads: `RDLT_SOAK_SECONDS`, or a minute where it is unset.
fn soaking() -> Duration {
    let seconds = match std::env::var("RDLT_SOAK_SECONDS") {
        Err(std::env::VarError::NotPresent) => 60,
        seconds => seconds
            .ok()
            .and_then(|seconds| seconds.parse().ok())
            .expect("RDLT_SOAK_SECONDS is a whole number of seconds"),
    };
    Duration::from_secs(seconds)
}

/// Kills and stalls the round's connector as `seed` draws, until dropped: a stall is `SIGSTOP`
/// for up to 600 ms, twice the host's patience, then `SIGCONT`.
async fn disrupting(seed: u64, kills: &Kills) -> Infallible {
    let mut draws = Draws(seed);
    loop {
        tokio::time::sleep(Duration::from_millis(100 + draws.below(900))).await;
        match draws.below(3) {
            0 => kills.kill(),
            1 => {
                // The group with the highest id this process lists: the host reaps a connector a
                // moment before it leaves the list, and a signal sent in between is lost.
                let Some(leader) = rdlt_host::spawned().last().copied() else {
                    continue;
                };
                let leader = Pid::from_raw(i32::try_from(leader).expect("a process id"));
                kill(leader, Signal::SIGSTOP).ok();
                tokio::time::sleep(Duration::from_millis(draws.below(600))).await;
                kill(leader, Signal::SIGCONT).ok();
            }
            _ => {}
        }
    }
}

/// Waits, for twenty seconds at most, until every connector round `round` spawned has ended.
async fn ended(round: u64) {
    for _ in 0..1000 {
        if rdlt_host::spawned().is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "round {round}: groups {:?} outlived their run",
        rdlt_host::spawned()
    );
}

/// Fails unless round `round` loaded `rows` rows of store `store` once each, and its error, report
/// and log hold no secret, the log showing that one was scrubbed.
async fn landed(round: u64, rows: u64, store: &str, outcome: &RunOutcome, logged: &Logged) {
    let error = outcome
        .error
        .as_ref()
        .map(|error| texts(error))
        .unwrap_or_default();
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "round {round}: {error}"
    );
    let expected: Vec<i64> = (0..i64::try_from(rows).expect("a row count")).collect();
    assert_eq!(
        published_ids(store),
        expected,
        "round {round}: a row did not land once"
    );
    let report = serde_json::to_string(&outcome.report).expect("a report serializes");
    clean(&format!("round {round}'s error"), &error, false);
    clean(&format!("round {round}'s report"), &report, false);
    // The drain logs what the source said as it connected a moment after the run may have ended.
    clean(
        &format!("round {round}'s log"),
        &scrubbed(logged).await,
        true,
    );
}

/// Waits, for five seconds at most, until `logged` holds a scrubbed line, and takes what it holds.
async fn scrubbed(logged: &Logged) -> String {
    for _ in 0..500 {
        if logged.text().contains("configured with ***") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    logged.take()
}

/// Round `round`: a scripted source of a drawn number of rows, crashing once where the draws
/// say, and killed and stalled while the engine loads it into the memory destination.
async fn loading(round: u64, logged: &Logged) {
    let mut draws = Draws(round);
    let directory = tempfile::tempdir().expect("a temporary directory");
    let rows = 2_000 + draws.below(20_000);
    let mut script = serde_json::json!({
        "rows": rows,
        "says": "configured with ${secret:trailing}",
        "marker": directory.path().join("crashed"),
    });
    if draws.below(2) == 0 {
        script["crash_once_at"] = serde_json::json!(draws.below(rows));
    }
    if draws.below(4) == 0 {
        script["stdout_bytes"] = serde_json::json!(64 * (256 + draws.below(1024)));
    }
    let kills = Kills::new();
    let local = local()
        .grace(Duration::from_millis(500))
        .options(options())
        .kills(&kills)
        .secrets(Vault);
    let source = local
        .source(&scripted(), &script)
        .await
        .expect("the source starts")
        .connector;
    let store = format!("soak-{round}");
    let destination = memory_destination(&store, &options()).await;
    let stream = StreamPlan::new(StreamName::new("rows").expect("a valid stream name"));
    let plan = PipelinePlan::new(PipelineId::parse("soak").expect("a valid id"), [stream])
        .expect("a valid plan");
    let running = engine().run(plan, Arc::from(source), Arc::new(destination));
    let disrupted = draws.below(u64::MAX);
    let outcome = tokio::time::timeout(ROUND, async {
        tokio::select! {
            biased;
            // The run wins: the disruptions end with it.
            outcome = running => outcome,
            never = disrupting(disrupted, &kills) => match never {},
        }
    })
    .await
    .unwrap_or_else(|_| panic!("round {round}: the run did not end within {ROUND:?}"));
    landed(round, rows, &store, &outcome, logged).await;
    ended(round).await;
}

// The real clock: the processes are real, and so are their kills and stalls.
#[test]
#[ignore = "loads through real connectors for minutes; run it with `just soak`"]
fn connectors_killed_stalled_and_crashing_load_every_row_once_and_say_no_secret() {
    let logged = Logged::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let until = std::time::Instant::now() + soaking();
    let mut round = 0;
    let mut longest = Duration::ZERO;
    tracing::subscriber::with_default(logged.clone(), || {
        while std::time::Instant::now() < until {
            let began = std::time::Instant::now();
            runtime.block_on(loading(round, &logged));
            longest = longest.max(began.elapsed());
            round += 1;
        }
    });
    eprintln!("soak: {round} rounds, the longest {longest:?}");
    assert!(round > 0, "no round ran");
    assert_eq!(rdlt_host::stop_spawned(Duration::from_secs(20)), Ok(()));
    // The drains log the last round's late lines while the runtime runs.
    tracing::subscriber::with_default(logged.clone(), || {
        runtime.block_on(async { tokio::time::sleep(Duration::from_millis(500)).await });
    });
    clean("the soak's last log", &logged.take(), false);
}
