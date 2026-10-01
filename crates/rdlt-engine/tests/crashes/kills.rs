//! The kill matrix (spec §20.6): the process running a pipeline, its spawned source or its spawned
//! destination is killed as it loads, the pipeline runs again, every row lands once, and no
//! connector outlives the run that spawned it.

use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::sys::signal::killpg;
use nix::unistd::Pid;
use serde_json::json;

use super::harness;
use super::scenarios::{self, Scenario};

/// How long a killed run's connectors may take to end.
const ENDING: Duration = Duration::from_secs(10);

/// The seed the matrix draws from: `RDLT_KILL_SEED`, or the clock.
fn seed() -> u64 {
    std::env::var("RDLT_KILL_SEED")
        .ok()
        .and_then(|seed| seed.parse().ok())
        .unwrap_or_else(|| {
            let since = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            u64::try_from(since.as_nanos() % u128::from(u64::MAX)).unwrap_or(1)
        })
}

/// The next draw of `state`, as `SplitMix64` draws.
fn draw(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut mixed = *state;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

/// The harness on `config`, in a process group of its own its connectors join.
fn spawn(config: &Path) -> Child {
    Command::new(harness())
        .arg(config)
        .env_remove("FAILPOINTS")
        .process_group(0)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the harness starts")
}

/// Whether any process of the group `group` lives.
fn lives(group: u32) -> bool {
    let group = Pid::from_raw(i32::try_from(group).expect("process ids fit"));
    killpg(group, None).is_ok()
}

/// Waits for every process of the group `group` to end; kills what outlives `ENDING` and says
/// whether anything did.
fn orphans(group: u32) -> bool {
    let deadline = Instant::now() + ENDING;
    while Instant::now() < deadline {
        if !lives(group) {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let group = Pid::from_raw(i32::try_from(group).expect("process ids fit"));
    killpg(group, nix::sys::signal::Signal::SIGKILL).ok();
    true
}

/// Runs the harness on `config` until a run succeeds, at most three times; whether one did, and
/// whether a run left a connector behind.
fn converge(config: &Path) -> (bool, bool) {
    let mut orphaned = false;
    for _ in 0..3 {
        let mut child = spawn(config);
        let status = child.wait().expect("the harness ends");
        orphaned |= orphans(child.id());
        if status.success() {
            return (true, orphaned);
        }
    }
    (false, orphaned)
}

/// Kills the harness running `scenario` at a drawn moment, for each of `draws` draws, then runs it
/// again; most kills must land as it loads.
fn engine_killed(scenario: &Scenario, draws: u32) {
    let mut state = seed();
    let seed = state;
    let mut interrupted = 0;
    for draw_index in 0..draws {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let config = scenario.write_spawned(dir.path(), None);
        let delay = Duration::from_millis(30 + draw(&mut state) % 500);
        let mut child = spawn(&config);
        std::thread::sleep(delay);
        let ended = child
            .try_wait()
            .expect("the harness can be asked")
            .is_some();
        child.kill().expect("the harness is killed, or has ended");
        child.wait().expect("the harness ends");
        interrupted += u32::from(!ended);
        let context = format!(
            "{} seed {seed} draw {draw_index}, killed at {delay:?}",
            scenario.name
        );
        assert!(
            !orphans(child.id()),
            "{context}: a connector outlived the killed run"
        );
        let (succeeded, orphaned) = converge(&config);
        assert!(
            succeeded,
            "{context}: the pipeline never ran to its end again"
        );
        assert!(!orphaned, "{context}: a connector outlived a run");
        scenario.verify(dir.path(), &context);
    }
    assert!(
        interrupted * 2 >= draws,
        "{} seed {seed}: only {interrupted} of {draws} kills landed as the pipeline loaded",
        scenario.name
    );
}

/// Kills the spawned `victim` of the harness running `scenario` before each of `commits`, then
/// runs it again where the run failed.
fn connector_killed(scenario: &Scenario, victim: &str, commits: &[u64]) {
    for commit in commits {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let kill = json!({ "victim": victim, "commit": commit });
        let config = scenario.write_spawned(dir.path(), Some(kill));
        let context = format!(
            "{}: the {victim} killed before commit {commit}",
            scenario.name
        );
        let (succeeded, orphaned) = converge(&config);
        assert!(succeeded, "{context}: the pipeline never ran to its end");
        assert!(!orphaned, "{context}: a connector outlived a run");
        scenario.verify(dir.path(), &context);
    }
}

#[test]
fn a_run_killed_as_it_loads_a_forgetting_log_loads_it_once_when_it_runs_again() {
    engine_killed(&scenarios::forgetting_log(), 6);
}

#[test]
fn a_run_killed_as_it_replaces_a_table_replaces_it_once_when_it_runs_again() {
    engine_killed(&scenarios::replaced(), 6);
}

#[test]
fn a_spawned_source_killed_as_a_run_loads_is_spawned_again_and_loses_nothing() {
    connector_killed(&scenarios::forgetting_log(), "source", &[2, 5]);
    connector_killed(&scenarios::forgetting_changes(), "source", &[2, 5]);
}

#[test]
fn a_spawned_destination_killed_as_a_run_loads_is_spawned_again_and_doubles_nothing() {
    connector_killed(&scenarios::forgetting_log(), "destination", &[2, 5]);
    connector_killed(&scenarios::replaced(), "destination", &[2, 5]);
}
