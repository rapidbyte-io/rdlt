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

/// The shells of this machine a guardian may run under: `/bin/sh`, and each other standard
/// shell installed, whose builtins differ.
fn shells() -> Vec<&'static str> {
    let others = [
        "/bin/dash",
        "/usr/bin/dash",
        "/bin/bash",
        "/usr/bin/bash",
        "/bin/mksh",
    ];
    let installed = others
        .into_iter()
        .filter(|shell| std::path::Path::new(shell).exists());
    std::iter::once("/bin/sh").chain(installed).collect()
}

/// A process leading a group of its own, and a guardian of it run by `shell`, told the leader
/// started at `noted` where one is given.
fn guarding(shell: &str, noted: Option<&str>) -> (Child, Child) {
    let leader = Command::new("/bin/sh")
        .args(["-c", "exec sleep 1000"])
        .process_group(0)
        .spawn()
        .expect("a process starts");
    let id = leader.id().to_string();
    let mut args = vec!["-c", super::GUARDING, "guardian", id.as_str()];
    args.extend(noted);
    let guardian = Command::new(shell)
        .args(args)
        .stdin(Stdio::piped())
        .spawn()
        .expect("the guardian starts");
    (leader, guardian)
}

/// Whether `child` ends within ten seconds; one that does not is killed.
fn ends(child: &mut Child) -> bool {
    for _ in 0..200 {
        if child.try_wait().expect("it is asked").is_some() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.kill().ok();
    child.wait().ok();
    false
}

#[test]
fn a_guardian_kills_its_group_once_its_pipe_closes_and_spares_an_id_another_process_took() {
    for shell in shells() {
        // The leader it noted: killed when the pipe closes.
        let (mut leader, mut guardian) = guarding(shell, None);
        std::thread::sleep(Duration::from_millis(200));
        drop(guardian.stdin.take());
        guardian.wait().expect("the guardian ends");
        assert!(
            ends(&mut leader),
            "{shell}: the guarded group was not killed"
        );
        // A process holding the id that started at another time than noted: sent nothing.
        let (mut leader, mut guardian) = guarding(shell, Some("a start long gone"));
        drop(guardian.stdin.take());
        guardian.wait().expect("the guardian ends");
        std::thread::sleep(Duration::from_millis(200));
        let spared = leader.try_wait().expect("it is asked");
        assert_eq!(spared, None, "{shell}: a reused id was killed");
        leader.kill().expect("it is killed");
        leader.wait().expect("it is reaped");
    }
}
