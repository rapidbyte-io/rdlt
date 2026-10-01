//! A spawned connector's process group, which its host owns for its whole life: whatever the
//! connector started in it ends when the connector is stopped, killed or dropped, when it exits
//! by itself, and when its host stops what it spawned.

use std::path::Path;
use std::time::Duration;

use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Kills, Provider as _};

use crate::process::{example, local, wait_gone};

/// How long what a connector started may take to end once its group is stopped.
const ENDING: Duration = Duration::from_secs(20);

/// A launcher in `directory` that starts two members of its group, one that ignores
/// `SIGTERM`, writes their process ids to `members`, and becomes the scripted connector.
fn launcher(directory: &Path) -> ConnectorRef {
    let path = directory.join("launcher");
    let members = directory.join("members");
    let script = format!(
        "#!/bin/sh\nsleep 1000 &\necho $! > '{members}'\n\
         sh -c 'trap \"\" TERM; sleep 1000' &\necho $! >> '{members}'\n\
         exec '{connector}' \"$@\"\n",
        members = members.display(),
        connector = example("scripted_connector").display()
    );
    std::fs::write(&path, script).expect("the launcher writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the launcher is executable");
    let id = ConnectorId::parse("test.scripted").expect("a valid id");
    ConnectorRef::new(id).path(path)
}

/// The process ids the launcher at `directory` wrote for the members it started.
fn members(directory: &Path) -> Vec<i32> {
    let written = std::fs::read_to_string(directory.join("members")).expect("ids were written");
    let members: Vec<i32> = written
        .lines()
        .map(|pid| pid.trim().parse().expect("a process id"))
        .collect();
    assert_eq!(members.len(), 2, "{written:?}");
    members
}

/// Whether every one of `members` is gone within [`ENDING`]; what is left is killed, so a
/// failing test leaves nothing behind.
async fn all_gone(members: &[i32]) -> bool {
    let mut gone = true;
    for member in members {
        if !wait_gone(*member, ENDING).await {
            gone = false;
            let member = nix::unistd::Pid::from_raw(*member);
            nix::sys::signal::kill(member, nix::sys::signal::Signal::SIGKILL).ok();
        }
    }
    gone
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_connector_takes_every_member_of_its_group_one_ignoring_the_stop_too() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let source = local()
        .grace(Duration::from_millis(200))
        .source(&launcher(directory.path()), &serde_json::json!({}))
        .await
        .expect("the connector starts")
        .connector;
    let members = members(directory.path());
    drop(source);
    assert!(all_gone(&members).await, "a member outlived its connector");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connector_that_exits_by_itself_takes_its_group_with_it() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    // Its check writes a last word and exits: the leader ends first, with no stop asked.
    let script = serde_json::json!({ "crash": "the connector gave up" });
    let source = local()
        .source(&launcher(directory.path()), &script)
        .await
        .expect("the connector starts")
        .connector;
    let members = members(directory.path());
    source.check().await.expect_err("the connector exits");
    assert!(all_gone(&members).await, "a member outlived its leader");
    drop(source);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_connector_takes_every_member_of_its_group() {
    let kills = Kills::new();
    let directory = tempfile::tempdir().expect("a temporary directory");
    let source = local()
        .kills(&kills)
        .source(&launcher(directory.path()), &serde_json::json!({}))
        .await
        .expect("the connector starts")
        .connector;
    let members = members(directory.path());
    kills.kill();
    assert!(all_gone(&members).await, "a member outlived the kill");
    drop(source);
}

#[test]
fn connectors_of_a_runtime_that_is_dropped_are_stopped_with_their_groups() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    let (local, launched) = (
        local().grace(Duration::from_millis(200)),
        launcher(directory.path()),
    );
    let config = serde_json::json!({});
    let placing = local.source(&launched, &config);
    let source = runtime.block_on(placing).expect("the connector starts");
    let members = members(directory.path());
    assert_eq!(rdlt_host::spawned().len(), 1);
    // As a run cut at its deadline ends: nothing is awaited, and every task is dropped.
    drop(source);
    drop(runtime);
    rdlt_host::stop_spawned(ENDING).expect("every group is stopped and empty");
    assert!(rdlt_host::spawned().is_empty());
    for member in members {
        let member = nix::unistd::Pid::from_raw(member);
        assert!(
            nix::sys::signal::kill(member, None).is_err(),
            "{member} lives"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_stops_what_it_spawned_and_says_what_it_could_not_stop_in_time() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    // A connector that ends only when killed, given longer to stop than the host waits.
    let script = serde_json::json!({ "linger": "forever" });
    let source = local()
        .grace(Duration::from_secs(3))
        .source(&launcher(directory.path()), &script)
        .await
        .expect("the connector starts")
        .connector;
    let members = members(directory.path());
    let spawned = rdlt_host::spawned();
    assert_eq!(spawned.len(), 1, "{spawned:?}");
    let stopping = tokio::task::spawn_blocking(|| rdlt_host::stop_spawned(Duration::ZERO));
    let lingering = stopping
        .await
        .expect("it returns")
        .expect_err("none stops in no time");
    assert_eq!(lingering.groups, spawned);
    // Asked to stop all the same, the group is gone once its grace has passed.
    let stopping = tokio::task::spawn_blocking(|| rdlt_host::stop_spawned(ENDING));
    stopping
        .await
        .expect("it returns")
        .expect("every group is stopped and empty");
    assert!(rdlt_host::spawned().is_empty());
    assert!(all_gone(&members).await);
    drop(source);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_or_terminated_host_stops_its_connectors_groups_before_it_exits() {
    use nix::sys::signal::Signal;
    use tokio::io::AsyncBufReadExt as _;
    for (signal, code) in [(Signal::SIGINT, 130), (Signal::SIGTERM, 143)] {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let launched = launcher(directory.path());
        let mut host = tokio::process::Command::new(example("connector_host"))
            .arg(launched.path.as_ref().expect("the launcher's path"))
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("the host starts");
        let stdout = host.stdout.take().expect("its output is piped");
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let ready = lines.next_line().await.expect("it reads");
        assert_eq!(ready.as_deref(), Some("ready"));
        // The last of its two connectors wrote the members it started.
        let members = members(directory.path());
        let pid = i32::try_from(host.id().expect("the host runs")).expect("a process id");
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), signal).expect("it is signalled");
        let status = host.wait().await.expect("the host ends");
        assert_eq!(status.code(), Some(code), "{signal}");
        // Stopped before the host exited: nothing is left to wait for.
        let alive = |member: &i32| {
            let member = nix::unistd::Pid::from_raw(*member);
            let alive = nix::sys::signal::kill(member, None).is_ok();
            nix::sys::signal::kill(member, Signal::SIGKILL).ok();
            alive
        };
        let left: Vec<i32> = members.into_iter().filter(alive).collect();
        assert!(left.is_empty(), "{signal}: {left:?} outlived the host");
    }
}
