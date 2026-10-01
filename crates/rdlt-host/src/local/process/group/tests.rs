use std::io::{BufRead as _, BufReader};
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::sync::watch;

use super::{Leader, Owned};

/// A shell leading a group of its own, which it shares with a `sleep` it started and whose
/// process id it wrote; then it runs `then`.
fn leading(then: &str) -> (Child, Pid) {
    let mut child = Command::new("sh")
        .args(["-c", &format!("sleep 1000 & echo $!; {then}")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .process_group(0)
        .spawn()
        .expect("a shell starts");
    let mut written = String::new();
    let stdout = child.stdout.take().expect("its output is piped");
    BufReader::new(stdout)
        .read_line(&mut written)
        .expect("it writes its member");
    let member = written.trim().parse().expect("a process id");
    (child, Pid::from_raw(member))
}

fn pid(child: &Child) -> Pid {
    Pid::from_raw(i32::try_from(child.id()).expect("a process id"))
}

/// `child` and its group, owned, asked to stop already, with `grace` to do it.
fn stopping(child: Child, grace: Duration) -> Owned {
    Owned {
        child,
        grace,
        stop: Arc::new(AtomicBool::new(true)),
        killed: None,
        exit: watch::channel(None).0,
    }
}

#[test]
fn what_a_wait_answers_says_whether_the_leader_is_still_a_child_of_this_process() {
    assert_eq!(Leader::of(&Ok(None)), Leader::Running);
    let (mut child, member) = leading("exit 3");
    let leader = rustix::process::Pid::from_child(&child);
    let asked = || super::asked(leader);
    // Exited and unreaped, it is asked of as often as one likes.
    while Leader::of(&asked()) == Leader::Running {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(Leader::of(&asked()), Leader::Exited);
    assert_eq!(Leader::of(&asked()), Leader::Exited);
    child.wait().expect("it is reaped");
    // Reaped, it is no child of this process: nothing may be assumed of its id.
    assert!(asked().is_err());
    assert_eq!(Leader::of(&asked()), Leader::Lost);
    kill(member, Signal::SIGKILL).expect("the member is killed");
}

#[test]
fn a_leader_something_else_reaped_is_neither_signalled_nor_waited_for() {
    let (child, member) = leading("exec sleep 1000");
    // Something else in this process reaps the leader, as a loop waiting for any child does.
    kill(pid(&child), Signal::SIGKILL).expect("the leader is killed");
    nix::sys::wait::waitpid(pid(&child), None).expect("it is reaped elsewhere");
    let ended = stopping(child, Duration::ZERO).ended();
    // Its group's id may be another's by now: the member it held was sent nothing.
    let alive = kill(member, None).is_ok();
    kill(member, Signal::SIGKILL).ok();
    assert!(alive, "the group was signalled after its leader was reaped");
    assert_eq!(ended, (None, false));
}
