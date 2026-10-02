use std::process::{Command, Stdio};
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
