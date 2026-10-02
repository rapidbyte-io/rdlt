//! Processes a test starts, which end when the test's own process does, however it ends.
//!
//! A test that is killed runs no code, so nothing it holds is dropped: a child it started
//! lives on, with whatever the child started. Each child a test starts therefore leads a
//! process group of its own, and a guardian outside that group waits on a pipe the test's
//! process holds: when the pipe closes, as it does when that process ends in any way, the
//! guardian kills the group.

use std::io;
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;

/// The guardians of this process's children, held for as long as the process lives: each
/// holds the pipe whose closing ends its group.
static GUARDIANS: Mutex<Vec<Child>> = Mutex::new(Vec::new());

/// What a guardian runs: it notes when the group's leader started, waits for its input to
/// end, then kills the group named to it, unless the leader's id is another process's by then.
///
/// A group's id cannot be another group's while any member of it lives, so a group whose
/// leader is gone and members live is still the group guarded. Where a process holds the
/// leader's id and started at another time, the id was reused, and nothing is sent.
///
/// A second argument stands in for the start noted, as a test of a reused id gives it.
///
/// The signal is named with `-s`, as the standard words it: dash, which is `/bin/sh` on Debian and
/// Ubuntu, takes `--` after it, and refuses it after `-KILL`, so no group would be killed.
const GUARDING: &str = "started=${2-$(ps -o lstart= -p \"$1\" 2>/dev/null)}; read -r _; \
    now=$(ps -o lstart= -p \"$1\" 2>/dev/null); \
    [ -n \"$now\" ] && [ \"$now\" != \"$started\" ] && exit 0; \
    kill -s KILL -- \"-$1\" 2>/dev/null";

/// Has the process group `leader` leads killed when this process ends, a kill of this
/// process included: `leader` is a child spawned to lead a group of its own.
///
/// A process the child starts in another group, as a host starts its connectors, is not in
/// the group: it ends as it does when its own parent is killed.
///
/// # Errors
///
/// The error of starting the guardian.
pub fn guard(leader: u32) -> io::Result<()> {
    let guardian = Command::new("/bin/sh")
        .args(["-c", GUARDING, "guardian", &leader.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        // Outside the group it guards, and outside the test's own, which a runner may kill.
        .process_group(0)
        .spawn()?;
    let mut guardians = GUARDIANS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guardians.push(guardian);
    Ok(())
}

/// Spawns `command` leading a process group of its own, [guarded](guard).
///
/// # Errors
///
/// The error of spawning the command or its guardian; a command that spawned and could not
/// be guarded is killed.
pub fn guarded(command: &mut Command) -> io::Result<Child> {
    let mut child = command.process_group(0).spawn()?;
    if let Err(error) = guard(child.id()) {
        child.kill().ok();
        child.wait().ok();
        return Err(error);
    }
    Ok(child)
}

/// This test binary run again as its test `test` alone, ignored or not, with its output shown:
/// a process that stands in for a test, which another test can kill.
///
/// # Panics
///
/// Panics when this binary has no path.
pub fn stand_in(test: &str) -> Command {
    let binary = std::env::current_exe().expect("the test binary has a path");
    let mut command = Command::new(binary);
    command.args(["--exact", test, "--include-ignored", "--nocapture"]);
    command
}

/// What a stand-in says of each process it started, and once it has started them all.
const STARTED: &str = "started ";
const READY: &str = "ready";

/// Says, as a stand-in, that it started process `pid`, or that `pid` descends from one it
/// started.
pub fn started(pid: u32) {
    use std::io::Write as _;
    writeln!(io::stdout(), "{STARTED}{pid}").ok();
}

/// Says, as a stand-in, that every process is started, and waits to be killed.
pub fn ready() -> ! {
    use std::io::Write as _;
    let mut stdout = io::stdout();
    writeln!(stdout, "{READY}").ok();
    stdout.flush().ok();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}

/// Runs the test `test` of this binary as a [stand-in](stand_in), kills it outright once it
/// is ready, as a test runner kills a test that timed out, and answers the processes it said
/// it started that still live `patience` later: each is then killed.
///
/// # Panics
///
/// Panics when the stand-in cannot be run, or ends before it is ready.
pub fn outliving(test: &str, patience: std::time::Duration) -> Vec<u32> {
    use std::io::BufRead as _;
    let mut command = stand_in(test);
    command.stdout(Stdio::piped()).stderr(Stdio::null());
    let mut parent = guarded(&mut command).expect("the stand-in starts");
    let stdout = parent.stdout.take().expect("its output is piped");
    let mut told = Vec::new();
    let mut ready = false;
    for line in io::BufReader::new(stdout).lines().map_while(Result::ok) {
        if let Some(pid) = line.strip_prefix(STARTED).and_then(|pid| pid.parse().ok()) {
            told.push(pid);
        }
        if line == READY {
            ready = true;
            break;
        }
    }
    assert!(ready, "the stand-in ended before it was ready");
    assert!(!told.is_empty(), "the stand-in started nothing");
    parent.kill().expect("the stand-in is killed");
    parent.wait().expect("the stand-in is reaped");
    let lives = |pid: &u32| {
        let asked = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(Stdio::null())
            .status();
        asked.is_ok_and(|status| status.success())
    };
    let until = std::time::Instant::now() + patience;
    while told.iter().any(lives) && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let left: Vec<u32> = told.into_iter().filter(lives).collect();
    for pid in &left {
        Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status()
            .ok();
    }
    left
}

#[cfg(test)]
mod tests;

/// The variable that, set to anything but `0`, makes a test that needs a sandbox fail where
/// none can be made, rather than skip: CI sets it, so a sandbox test never passes without
/// having run.
pub const REQUIRE_SANDBOX: &str = "RDLT_REQUIRE_SANDBOX";

/// Says why a test that needs a sandbox does not run here, as the caller then returns.
///
/// # Panics
///
/// Panics where [`REQUIRE_SANDBOX`] is set.
pub fn without_sandbox(why: &dyn std::fmt::Display) {
    use std::io::Write as _;
    let required = std::env::var_os(REQUIRE_SANDBOX).is_some_and(|required| required != "0");
    assert!(
        !required,
        "a sandbox is required here, and none can be made: {why}"
    );
    writeln!(io::stderr(), "skipped: no sandbox can be made here: {why}").ok();
}
