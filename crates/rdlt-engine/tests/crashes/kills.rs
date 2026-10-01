//! The kill matrix: the process running a pipeline, its spawned source or its spawned
//! destination is killed as it loads, the pipeline runs again, every row lands once, and no
//! connector outlives the run that spawned it.
//!
//! Each connector leads a process group of its own, which the harness tells by its id as it
//! spawns it; a run has left nothing behind once every group it told is empty.

use std::io::{BufRead as _, BufReader};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::sys::signal::killpg;
use nix::unistd::Pid;
use serde_json::{Value, json};

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

/// A harness run, the lines it tells, and the process groups of the connectors it told it
/// spawned.
struct Watched {
    child: Child,
    lines: mpsc::Receiver<String>,
    connectors: Arc<Mutex<Vec<u32>>>,
}

impl Watched {
    /// The harness on `config`.
    fn spawn(config: &Path) -> Self {
        let mut child = Command::new(harness())
            .arg(config)
            .env_remove("FAILPOINTS")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("the harness starts");
        let stdout = child.stdout.take().expect("the harness's output");
        let (sender, lines) = mpsc::channel();
        let connectors = Arc::new(Mutex::new(Vec::new()));
        let told = Arc::clone(&connectors);
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let connector = line.strip_prefix("connector ");
                if let Some(connector) = connector.and_then(|pid| pid.parse().ok()) {
                    told.lock().expect("unpoisoned").push(connector);
                } else if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            lines,
            connectors,
        }
    }

    /// What tells the process groups of the connectors the run has told so far, and goes on
    /// telling those it tells later.
    fn connectors(&self) -> Arc<Mutex<Vec<u32>>> {
        Arc::clone(&self.connectors)
    }

    /// Waits until the run has told `count` reads and commits; whether it did.
    fn progressed(&self, count: u64) -> bool {
        let mut seen = 0;
        while seen < count {
            match self.lines.recv_timeout(ENDING * 6) {
                Ok(line) => seen += u64::from(progress(&line)),
                Err(_) => return false,
            }
        }
        true
    }

    /// Waits for the run to end: its exit and every line it told.
    fn ended(mut self) -> (ExitStatus, Vec<String>) {
        let status = self.child.wait().expect("the harness ends");
        (status, self.lines.iter().collect())
    }
}

impl Drop for Watched {
    /// Kills the harness of a run a test gave up on: its connectors, their host gone, end by
    /// themselves.
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            self.child.kill().ok();
            self.child.wait().ok();
        }
    }
}

/// Whether `line` tells a read begun or a commit landed.
fn progress(line: &str) -> bool {
    line.starts_with("read ") || line.starts_with("commit ")
}

/// The reads and commits a clean run of `scenario` tells.
fn progress_of(scenario: &Scenario) -> u64 {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config = scenario.write_spawned(dir.path(), &json!({}));
    let (status, lines) = Watched::spawn(&config).ended();
    assert!(
        status.success(),
        "{}: a clean run ended {status}",
        scenario.name
    );
    lines.iter().filter(|line| progress(line)).count() as u64
}

/// Whether any process of the group `group` lives.
fn lives(group: u32) -> bool {
    let group = Pid::from_raw(i32::try_from(group).expect("process ids fit"));
    killpg(group, None).is_ok()
}

/// Waits for every process of each connector's group the run told to end; kills what outlives
/// `ENDING` and says whether anything did.
fn orphans(connectors: &Arc<Mutex<Vec<u32>>>) -> bool {
    let connectors = connectors.lock().expect("unpoisoned").clone();
    assert!(
        !connectors.is_empty(),
        "the run told no connector it spawned"
    );
    let deadline = Instant::now() + ENDING;
    while Instant::now() < deadline {
        if !connectors.iter().any(|group| lives(*group)) {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    for group in connectors.into_iter().filter(|group| lives(*group)) {
        let group = Pid::from_raw(i32::try_from(group).expect("process ids fit"));
        killpg(group, nix::sys::signal::Signal::SIGKILL).ok();
    }
    true
}

/// Runs the harness on `config` until a run succeeds, at most three times; whether one did, and
/// whether a run left a connector behind.
fn converge(config: &Path) -> (bool, bool) {
    let mut orphaned = false;
    for _ in 0..3 {
        let run = Watched::spawn(config);
        let connectors = run.connectors();
        let (status, _) = run.ended();
        orphaned |= orphans(&connectors);
        if status.success() {
            return (true, orphaned);
        }
    }
    (false, orphaned)
}

/// Kills the harness running `scenario` where it waits after a read or commit drawn among those a
/// clean run tells, a drawn delay after it began to, for each of `draws` draws, then runs it
/// again.
fn engine_killed(scenario: &Scenario, draws: u32) {
    let mut state = seed();
    let seed = state;
    let told = progress_of(scenario);
    assert!(
        told >= 3,
        "{}: a clean run tells {told} reads and commits",
        scenario.name
    );
    for draw_index in 0..draws {
        // Never after the last two, which a run may tell fewer of than the clean one did.
        let point = 1 + draw(&mut state) % (told - 2);
        // While one read or commit waits, the run's others go on.
        let delay = Duration::from_millis(draw(&mut state) % 50);
        let context = format!(
            "{} seed {seed} draw {draw_index}, killed {delay:?} after read or commit {point}",
            scenario.name
        );
        let dir = tempfile::tempdir().expect("a temporary directory");
        let config = scenario.write_spawned(dir.path(), &json!({ "pause": point }));
        let mut run = Watched::spawn(&config);
        assert!(run.progressed(point), "{context}: the run never got there");
        std::thread::sleep(delay);
        let ended = run.child.try_wait().expect("the harness can be asked");
        assert!(ended.is_none(), "{context}: the run ended before its kill");
        run.child.kill().expect("the harness is killed");
        let connectors = run.connectors();
        run.ended();
        assert!(
            !orphans(&connectors),
            "{context}: a connector outlived the killed run"
        );
        let config = scenario.write_spawned(dir.path(), &json!({}));
        let (succeeded, orphaned) = converge(&config);
        assert!(
            succeeded,
            "{context}: the pipeline never ran to its end again"
        );
        assert!(!orphaned, "{context}: a connector outlived a run");
        scenario.verify(dir.path(), &context);
    }
}

/// Kills the spawned `victim` of the harness running `scenario` before each commit of `before`,
/// a source only as it reads; the run must ride the kill out on a later attempt.
fn connector_killed(scenario: &Scenario, victim: &str, before: &[Value]) {
    for before in before {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let kill = json!({ "victim": victim, "before": before });
        let config = scenario.write_spawned(dir.path(), &json!({ "kill": kill }));
        let context = format!(
            "{}: the {victim} killed before commit {before}",
            scenario.name
        );
        let run = Watched::spawn(&config);
        let connectors = run.connectors();
        let (status, lines) = run.ended();
        assert!(
            !orphans(&connectors),
            "{context}: a connector outlived the run"
        );
        assert!(
            status.success(),
            "{context}: the run did not ride the kill out: {status}"
        );
        let reading: u64 = lines
            .iter()
            .find_map(|line| line.strip_prefix("killed reading "))
            .unwrap_or_else(|| panic!("{context}: nothing was killed"))
            .parse()
            .expect("a count of reads");
        if victim == "source" {
            assert!(
                reading > 0,
                "{context}: the source was killed between reads"
            );
        }
        let report: Value = lines
            .last()
            .and_then(|line| serde_json::from_str(line).ok())
            .expect("a report");
        let attempted = report["attempted"].as_u64().expect("a count of attempts");
        assert!(attempted >= 2, "{context}: the kill failed no attempt");
        scenario.verify(dir.path(), &context);
    }
}

#[test]
fn a_run_a_test_gives_up_on_leaves_no_process_behind() {
    let scenario = scenarios::forgetting_log();
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config = scenario.write_spawned(dir.path(), &json!({ "pause": 1 }));
    let run = Watched::spawn(&config);
    assert!(run.progressed(1), "the run never began to read");
    let connectors = run.connectors();
    drop(run);
    assert!(!orphans(&connectors), "a connector outlived the run");
}

/// A launcher in `dir` that starts a member of its group, which heeds neither the end of its
/// input nor its parent's death, and becomes the example connector `served`.
fn launcher(dir: &Path, served: &str) -> std::path::PathBuf {
    let connector = harness().with_file_name(served);
    let path = dir.join("launcher");
    let script = format!(
        "#!/bin/sh\nsleep 1000 &\nexec '{}' \"$@\"\n",
        connector.display()
    );
    std::fs::write(&path, script).expect("the launcher writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the launcher is executable");
    path
}

#[test]
fn what_a_connector_started_ends_with_a_run_that_ends_and_is_an_orphan_of_one_killed() {
    let scenario = scenarios::forgetting_log();
    let dir = tempfile::tempdir().expect("a temporary directory");
    let launched = json!({ "source_launcher": launcher(dir.path(), "serve_log") });
    // A run that ends stops each connector's whole group before it exits.
    let config = scenario.write_spawned(dir.path(), &launched);
    let run = Watched::spawn(&config);
    let connectors = run.connectors();
    let (status, _) = run.ended();
    assert!(status.success(), "the run ended {status}");
    assert!(!orphans(&connectors), "a group outlived a run that ended");
    // A harness that is killed runs no code: the connector ends by itself, and what it
    // started and left in its group lives on, which the check a run's tests make must see.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mut paused = launched;
    paused["source_launcher"] = json!(launcher(dir.path(), "serve_log"));
    paused["pause"] = json!(1);
    let config = scenario.write_spawned(dir.path(), &paused);
    let mut run = Watched::spawn(&config);
    assert!(run.progressed(1), "the run never began to read");
    let connectors = run.connectors();
    run.child.kill().expect("the harness is killed");
    run.ended();
    assert!(
        orphans(&connectors),
        "a member that outlived a killed run went unseen"
    );
}

#[test]
fn a_run_killed_as_it_loads_a_forgetting_log_loads_it_once_when_it_runs_again() {
    engine_killed(&scenarios::forgetting_log(), 6);
}

#[test]
fn a_run_killed_as_it_merges_a_forgetting_change_stream_merges_it_once_when_it_runs_again() {
    engine_killed(&scenarios::forgetting_changes(), 6);
}

#[test]
fn a_run_killed_as_it_replaces_a_table_replaces_it_once_when_it_runs_again() {
    engine_killed(&scenarios::replaced(), 6);
}

#[test]
fn a_spawned_source_killed_as_it_reads_is_spawned_again_and_loses_nothing() {
    connector_killed(
        &scenarios::long_forgetting_log(),
        "source",
        &[json!(1), json!(3)],
    );
    connector_killed(&scenarios::forgetting_changes(), "source", &[json!(1)]);
}

#[test]
fn a_spawned_destination_killed_as_a_run_loads_is_spawned_again_and_doubles_nothing() {
    connector_killed(
        &scenarios::forgetting_log(),
        "destination",
        &[json!(1), json!(4)],
    );
    connector_killed(&scenarios::forgetting_changes(), "destination", &[json!(2)]);
    connector_killed(
        &scenarios::replaced(),
        "destination",
        &[json!(1), json!("publish")],
    );
}
