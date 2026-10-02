use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use super::{guarded, outliving, ready, started};

/// A stand-in that starts a shell which starts a member of its group.
#[test]
#[ignore = "run by the test below, as the process it kills"]
fn stand_in_starting_a_shell_and_its_member() {
    use std::io::{BufRead as _, BufReader};
    let mut shell = Command::new("/bin/sh");
    shell
        .args(["-c", "sleep 1000 & echo $!; exec sleep 1000"])
        .stdout(Stdio::piped());
    let mut child = guarded(&mut shell).expect("the shell starts");
    let stdout = child.stdout.take().expect("its output is piped");
    let mut member = String::new();
    BufReader::new(stdout)
        .read_line(&mut member)
        .expect("it says its member");
    started(child.id());
    started(member.trim().parse().expect("a process id"));
    // Held, not waited for: a test that is killed waits for nothing.
    std::mem::forget(child);
    ready()
}

#[test]
fn what_a_test_started_is_gone_once_the_test_is_killed() {
    let test = "process::tests::stand_in_starting_a_shell_and_its_member";
    assert_eq!(outliving(test, Duration::from_secs(20)), Vec::<u32>::new());
}

/// A process leading a group of its own, and a guardian of it, told the leader started at
/// `noted` where one is given.
fn guarding(noted: Option<&str>) -> (Child, Child) {
    let leader = Command::new("/bin/sh")
        .args(["-c", "exec sleep 1000"])
        .process_group(0)
        .spawn()
        .expect("a process starts");
    let id = leader.id().to_string();
    let mut args = vec!["-c", super::GUARDING, "guardian", id.as_str()];
    args.extend(noted);
    let guardian = Command::new("/bin/sh")
        .args(args)
        .stdin(Stdio::piped())
        .spawn()
        .expect("the guardian starts");
    (leader, guardian)
}

#[test]
fn a_guardian_kills_its_group_once_its_pipe_closes_and_spares_an_id_another_process_took() {
    // The leader it noted: killed when the pipe closes.
    let (mut leader, mut guardian) = guarding(None);
    std::thread::sleep(Duration::from_millis(200));
    drop(guardian.stdin.take());
    guardian.wait().expect("the guardian ends");
    let ended = leader.wait().expect("it ends");
    assert!(!ended.success(), "the guarded group was not killed");
    // A process holding the id that started at another time than noted: sent nothing.
    let (mut leader, mut guardian) = guarding(Some("a start long gone"));
    drop(guardian.stdin.take());
    guardian.wait().expect("the guardian ends");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        leader.try_wait().expect("it is asked"),
        None,
        "a reused id was killed"
    );
    leader.kill().expect("it is killed");
    leader.wait().expect("it is reaped");
}
