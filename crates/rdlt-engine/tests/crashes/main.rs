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
    /// Once a commit, at the destination's own commit: every hit the run reaches, so the commits
    /// where a truncate lands, and every other, crash too.
    Landing,
    /// Once a run: as its load ends, or as a commit completes its stream.
    Once,
}

/// More commits than any swept run makes.
const MOST_COMMITS: u64 = 100;

/// Which runs pass a point.
#[derive(Clone, Copy)]
enum Passed {
    /// Every run.
    Always,
    /// Runs keeping a log.
    Logged,
    /// Runs whose stream completes: a full read, publishing what it read.
    Completing,
}

/// A crash point: its name, how often a run passes it, and which runs do.
struct Point {
    name: &'static str,
    hits: Hits,
    passed: Passed,
}

const fn point(name: &'static str, hits: Hits, passed: Passed) -> Point {
    Point { name, hits, passed }
}

/// Every durability step a fresh run passes, in the order a commit does, then the load's end.
const POINTS: [Point; 17] = [
    point("engine.wal.append", Hits::Each, Passed::Logged),
    point("engine.flush.before", Hits::Each, Passed::Always),
    point("engine.flush.after", Hits::Each, Passed::Always),
    point("engine.wal.sync.before", Hits::Each, Passed::Logged),
    point("engine.wal.sync.after", Hits::Each, Passed::Logged),
    point("engine.ack.early", Hits::Each, Passed::Logged),
    point("engine.complete.before", Hits::Once, Passed::Completing),
    point("engine.commit.before", Hits::Landing, Passed::Always),
    point("engine.commit.after", Hits::Landing, Passed::Always),
    point("engine.receipt.after", Hits::Each, Passed::Logged),
    point("engine.complete.after", Hits::Once, Passed::Completing),
    point("engine.wal.remove", Hits::Each, Passed::Logged),
    point("engine.ack.before", Hits::Each, Passed::Always),
    point("engine.ack.after", Hits::Each, Passed::Always),
    point("engine.wal.close.before", Hits::Once, Passed::Logged),
    point("engine.wal.close.after", Hits::Once, Passed::Logged),
    point("engine.wal.removed", Hits::Once, Passed::Logged),
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

/// Runs the harness on `config`, crashing where `failpoints` says; its exit, and the commits it
/// reported where it ran to its end.
fn run(config: &Path, failpoints: Option<&str>) -> (ExitStatus, Option<u64>) {
    let mut command = Command::new(harness());
    command.arg(config).env_remove("FAILPOINTS");
    if let Some(failpoints) = failpoints {
        command.env("FAILPOINTS", failpoints);
    }
    let output = command
        .stderr(std::process::Stdio::null())
        .output()
        .expect("the harness runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let commits = stdout
        .lines()
        .last()
        .and_then(|report| serde_json::from_str::<serde_json::Value>(report).ok())
        .and_then(|report| report["commits"].as_u64());
    (output.status, commits)
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
        let (status, _) = run(&config, Some(failpoint));
        assert!(
            crashed(status),
            "{} {case}: {failpoint} did not crash the run: {status}",
            scenario.name
        );
    }
    again(scenario, dir.path(), &config, case);
}

/// Runs `scenario` in `dir` cleanly after its crashes, and checks what it loaded.
fn again(scenario: &Scenario, dir: &Path, config: &Path, case: &str) {
    let (status, _) = run(config, None);
    assert!(
        status.success(),
        "{} {case}: the run again ended {status}",
        scenario.name
    );
    scenario.verify(dir, case);
}

/// Crashes `scenario` at each commit its runs make at `point`, until a run ends before it reaches
/// the hit: every hit a run reaches must crash it.
fn every_commit(scenario: &Scenario, point: &str) {
    for hit in 1..=MOST_COMMITS {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let config = scenario.write(dir.path());
        let case = format!("{point} at hit {hit}");
        let (status, commits) = run(&config, Some(&failpoint(point, hit)));
        if status.success() {
            let commits = commits.expect("a run to its end reports its commits");
            assert!(
                commits < hit,
                "{} {case}: the run made {commits} commits and did not crash",
                scenario.name
            );
            return;
        }
        assert!(
            crashed(status),
            "{} {case}: the run ended {status}",
            scenario.name
        );
        again(scenario, dir.path(), &config, &case);
    }
    panic!(
        "{}: {point} crashed {MOST_COMMITS} commits and the run never ended",
        scenario.name
    );
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
    let passed = POINTS.iter().filter(|point| match point.passed {
        Passed::Always => true,
        Passed::Logged => scenario.logged,
        Passed::Completing => scenario.completes,
    });
    for point in passed {
        let at: &[u64] = match point.hits {
            Hits::Each => &[1, 3],
            Hits::Once => &[1],
            Hits::Landing => {
                every_commit(scenario, point.name);
                continue;
            }
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
