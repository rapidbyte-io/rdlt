use std::io::{BufRead as _, BufReader};
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
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

/// `child` and its group, owned, with `grace` to end once stopped.
fn owned(child: Child, grace: Duration) -> Owned {
    Owned::new(child, grace, None, watch::channel(None).0)
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
    let mut owned = owned(child, Duration::ZERO);
    owned.held().stop();
    let ended = owned.ended(super::emptied);
    // Its group's id may be another's by now: the member it held was sent nothing.
    let alive = kill(member, None).is_ok();
    kill(member, Signal::SIGKILL).ok();
    assert!(alive, "the group was signalled after its leader was reaped");
    assert_eq!(ended, (None, false));
}

#[test]
fn a_group_is_living_while_a_member_that_has_not_ended_is_in_it() {
    let (mut child, member) = leading("exit 0");
    let group = rustix::process::Pid::from_child(&child);
    while Leader::of(&super::asked(group)) == Leader::Running {
        std::thread::sleep(Duration::from_millis(5));
    }
    // The leader has ended and is unreaped; its member lives.
    assert!(super::members::living(group));
    kill(member, Signal::SIGKILL).expect("the member is killed");
    // The member is reaped by whatever adopted it, and the leader stays, ended: on Linux the
    // null signal still answers for it, and it is no living member.
    let until = std::time::Instant::now() + Duration::from_secs(20);
    while kill(member, None).is_ok() && std::time::Instant::now() < until {
        std::thread::sleep(Duration::from_millis(5));
    }
    #[cfg(target_os = "linux")]
    {
        assert!(rustix::process::test_kill_process_group(group).is_ok());
        assert!(!super::members::living(group));
    }
    child.wait().expect("it is reaped");
    assert!(!super::members::living(group));
}

#[cfg(target_os = "linux")]
#[test]
fn a_process_says_its_state_and_its_group_at_each_depth_of_namespaces() {
    use super::members::member;
    let status = "Name:\tsleep (a)\nState:\tS (sleeping)\nNSpid:\t900\t7\nNSpgid:\t880\t3\n";
    // At the depth this process is at, the group is what its namespace numbers it.
    assert_eq!(member(status, 1), Some((880, true)));
    assert_eq!(member(status, 2), Some((3, true)));
    // A process of a namespace above this one's has no group in it.
    assert_eq!(member(status, 3), None);
    for (state, living) in [
        ("Z (zombie)", false),
        ("X (dead)", false),
        ("R (running)", true),
    ] {
        let status = format!("State:\t{state}\nNSpgid:\t5\n");
        assert_eq!(member(&status, 1), Some((5, living)), "{state}");
    }
    assert_eq!(member("State:\tS (sleeping)\n", 1), None);
    assert_eq!(member("NSpgid:\t5\n", 1), None);
    assert_eq!(member("State:\tS\nNSpgid:\tfive\n", 1), None);
}

#[test]
fn a_stop_has_asked_every_member_to_end_before_it_returns() {
    let (child, member) = leading("exec sleep 1000");
    let leader = pid(&child);
    // No thread owns the group: what reaches it, the stop itself sent.
    let owned = owned(child, Duration::from_secs(1000));
    owned.held().stop();
    let terminated = nix::sys::wait::WaitStatus::Signaled(leader, Signal::SIGTERM, false);
    assert_eq!(nix::sys::wait::waitpid(leader, None), Ok(terminated));
    let until = std::time::Instant::now() + Duration::from_secs(20);
    while kill(member, None).is_ok() && std::time::Instant::now() < until {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        kill(member, None).is_err(),
        "the member was not asked to end"
    );
    // Reaped elsewhere since, the leader is sent nothing more, however often it is stopped.
    owned.held().stop();
    owned.discarded();
}

#[test]
fn a_connectors_exit_is_told_before_its_group_is_asked_whether_it_is_empty() {
    let (child, _member) = leading("exit 3");
    let (exit, told) = watch::channel(None);
    let mut owned = Owned::new(child, Duration::ZERO, None, exit);
    let asked = |_| {
        // Whoever waits to say how the connector ended need not wait for its members.
        let code = told.borrow().and_then(|status| status.code());
        assert_eq!(code, Some(3));
        false
    };
    let (status, emptied) = owned.ended(asked);
    assert_eq!(
        (status.and_then(|status| status.code()), emptied),
        (Some(3), false)
    );
}
