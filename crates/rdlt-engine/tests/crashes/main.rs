//! Real crashes (spec §20.6): a pipeline run in a process of its own crashes at each of the
//! engine's durability steps, or is killed with its spawned connectors, then runs again, and its
//! destination holds every row once.
//!
//! The harness is the `crash_run` example, which `cargo test` builds with the crate's examples; a
//! run naming this target alone uses whichever harness was built last.

#![expect(
    clippy::disallowed_methods,
    reason = "the harness runs in processes of its own, on the real clock"
)]

mod kills;
mod scenarios;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

use scenarios::Scenario;

/// How often a run passes a point, and so the hits the sweep crashes it at.
#[derive(Clone, Copy)]
enum Hits {
    /// Many times a run: its first hit and its third.
    Each,
    /// Once a run, as its load ends.
    Once,
}

/// A crash point: its name, how often a run passes it, and whether only a run keeping a log
/// does.
struct Point {
    name: &'static str,
    hits: Hits,
    logged: bool,
}

const fn point(name: &'static str, hits: Hits, logged: bool) -> Point {
    Point { name, hits, logged }
}

/// Every durability step a fresh run passes, in the order a commit does, then the load's end.
const POINTS: [Point; 15] = [
    point("engine.wal.append", Hits::Each, true),
    point("engine.flush.before", Hits::Each, false),
    point("engine.flush.after", Hits::Each, false),
    point("engine.wal.sync.before", Hits::Each, true),
    point("engine.wal.sync.after", Hits::Each, true),
    point("engine.ack.early", Hits::Each, true),
    point("engine.commit.before", Hits::Each, false),
    point("engine.commit.after", Hits::Each, false),
    point("engine.receipt.after", Hits::Each, true),
    point("engine.wal.remove", Hits::Each, true),
    point("engine.ack.before", Hits::Each, false),
    point("engine.ack.after", Hits::Each, false),
    point("engine.wal.close.before", Hits::Once, true),
    point("engine.wal.close.after", Hits::Once, true),
    point("engine.wal.removed", Hits::Once, true),
];

/// The harness binary, built beside this test.
fn harness() -> PathBuf {
    let here = std::env::current_exe().expect("the test knows where it runs");
    let deps = here.parent().expect("tests run in a directory");
    deps.parent()
        .expect("the directory has a parent")
        .join("examples")
        .join("crash_run")
}

/// Runs the harness on `config`, crashing where `failpoints` says; its exit.
fn run(config: &Path, failpoints: Option<&str>) -> ExitStatus {
    let mut command = Command::new(harness());
    command.arg(config).env_remove("FAILPOINTS");
    if let Some(failpoints) = failpoints {
        command.env("FAILPOINTS", failpoints);
    }
    command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("the harness runs")
}

/// Whether `status` is a crash: the process aborted.
fn crashed(status: ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt as _;
    status.signal() == Some(6)
}

/// Runs `scenario` crashing at `failpoints` (each must crash a run in turn), then cleanly; checks
/// what it loaded.
fn crashes(scenario: &Scenario, failpoints: &[String], case: &str) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config = scenario.write(dir.path());
    for failpoint in failpoints {
        let status = run(&config, Some(failpoint));
        assert!(
            crashed(status),
            "{} {case}: {failpoint} did not crash the run: {status}",
            scenario.name
        );
    }
    let status = run(&config, None);
    assert!(
        status.success(),
        "{} {case}: the run again ended {status}",
        scenario.name
    );
    scenario.verify(dir.path(), case);
}

/// The failpoint crashing a run at the `hit`th time it passes `point`.
fn failpoint(point: &str, hit: u64) -> String {
    match hit {
        1 => format!("{point}=return"),
        hit => format!("{point}={}*off->return", hit - 1),
    }
}

/// Crashes `scenario` at every point its runs pass, at the hits each is passed, and in a replay
/// where its runs keep a log.
fn sweep(scenario: &Scenario) {
    let passed = POINTS
        .iter()
        .filter(|point| scenario.logged || !point.logged);
    for point in passed {
        let at: &[u64] = match point.hits {
            Hits::Each => &[1, 3],
            Hits::Once => &[1],
        };
        for hit in at {
            let case = format!("{} at hit {hit}", point.name);
            crashes(scenario, &[failpoint(point.name, *hit)], &case);
        }
    }
    if !scenario.logged {
        return;
    }
    // A crash before a commit lands leaves it to replay, which crashes too, before and after it
    // lands, and the next run replays it again.
    let replays = [
        "engine.commit.before=return".to_owned(),
        "engine.replay.before=return".to_owned(),
        "engine.replay.after=return".to_owned(),
    ];
    crashes(scenario, &replays, "in replays");
}

#[test]
fn a_log_that_forgets_loads_every_message_once_through_every_crash() {
    sweep(&scenarios::forgetting_log());
}

#[test]
fn a_change_stream_that_forgets_merges_every_change_through_every_crash() {
    sweep(&scenarios::forgetting_changes());
}

#[test]
fn a_full_read_replaces_its_table_once_through_every_crash() {
    sweep(&scenarios::replaced());
}

#[test]
fn a_history_keeps_each_version_once_through_every_crash() {
    sweep(&scenarios::kept_history());
}

#[test]
fn a_full_read_without_a_log_replaces_its_table_once_through_every_crash() {
    sweep(&scenarios::replaced().unlogged("a full read replaced without a log"));
}

#[test]
fn a_history_without_a_log_keeps_each_version_once_through_every_crash() {
    sweep(&scenarios::kept_history().unlogged("a history without a log"));
}
