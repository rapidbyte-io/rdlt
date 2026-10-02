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
/// `SIGTERM`, writes their process ids after those `members` holds, and becomes the scripted
/// connector.
fn launcher(directory: &Path) -> ConnectorRef {
    let path = directory.join("launcher");
    let members = directory.join("members");
    let script = format!(
        "#!/bin/sh\nsleep 1000 &\necho $! >> '{members}'\n\
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

/// The process ids the launcher at `directory` wrote for the members it started, two for each
/// of the `connectors` it became.
fn members(directory: &Path, connectors: usize) -> Vec<i32> {
    let written = std::fs::read_to_string(directory.join("members")).expect("ids were written");
    let members: Vec<i32> = written
        .lines()
        .map(|pid| pid.trim().parse().expect("a process id"))
        .collect();
    assert_eq!(members.len(), 2 * connectors, "{written:?}");
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
    let members = members(directory.path(), 1);
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
    let members = members(directory.path(), 1);
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
    let members = members(directory.path(), 1);
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
    let members = members(directory.path(), 1);
    assert_eq!(rdlt_host::spawned().len(), 1);
    // As a run cut at its deadline ends: nothing is awaited, and every task is dropped.
    drop(source);
    drop(runtime);
    rdlt_host::stop_spawned(ENDING).expect("every group is stopped and empty");
    assert!(rdlt_host::spawned().is_empty());
    // Ended already: what is waited for is whatever adopted them reaping them.
    let reaped = tokio::runtime::Runtime::new().expect("a runtime");
    assert!(reaped.block_on(all_gone(&members)), "a member lives");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_out_of_patience_kills_what_it_spawned_before_it_returns() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    // A connector that ends only when killed, given far longer to stop than the host waits.
    let script = serde_json::json!({ "linger": "forever" });
    let source = local()
        .grace(Duration::from_secs(1000))
        .source(&launcher(directory.path()), &script)
        .await
        .expect("the connector starts")
        .connector;
    let members = members(directory.path(), 1);
    let spawned = rdlt_host::spawned();
    assert_eq!(spawned.len(), 1, "{spawned:?}");
    for patience in [Duration::ZERO, Duration::from_millis(300)] {
        let began = std::time::Instant::now();
        let stopping = tokio::task::spawn_blocking(move || rdlt_host::stop_spawned(patience));
        let stopped = stopping.await.expect("it returns");
        // Its patience over, the host kills: nothing it spawned is left to its grace.
        assert_eq!(stopped, Ok(()), "{patience:?}");
        assert!(began.elapsed() < ENDING, "{:?}", began.elapsed());
        assert!(rdlt_host::spawned().is_empty());
    }
    assert!(all_gone(&members).await);
    drop(source);
}

/// A host of two connectors the launcher in `directory` becomes, started in `mode`, once it is
/// ready, with the members its connectors started.
async fn hosting(directory: &Path, mode: &str) -> (tokio::process::Child, Vec<i32>) {
    hosting_configured(directory, mode, "{}").await
}

/// A host as [`hosting`] starts it, whose connectors are configured with `config`, as JSON.
async fn hosting_configured(
    directory: &Path,
    mode: &str,
    config: &str,
) -> (tokio::process::Child, Vec<i32>) {
    use tokio::io::AsyncBufReadExt as _;
    let launched = launcher(directory);
    let mut host = tokio::process::Command::new(example("connector_host"))
        .arg(launched.path.as_ref().expect("the launcher's path"))
        .args([mode, "trusted", config])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the host starts");
    let stdout = host.stdout.take().expect("its output is piped");
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    let ready = lines.next_line().await.expect("it reads");
    assert_eq!(ready.as_deref(), Some("ready"));
    // Each of its two connectors wrote the members it started.
    (host, members(directory, 2))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_asked_to_end_by_any_signal_it_hears_stops_its_connectors_groups_first() {
    use nix::sys::signal::Signal;
    let heard = [
        (Signal::SIGINT, 130),
        (Signal::SIGTERM, 143),
        // A terminal that closes hangs up its foreground group alone, which no connector is in.
        (Signal::SIGHUP, 129),
        (Signal::SIGQUIT, 131),
    ];
    for (signal, code) in heard {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let (mut host, members) = hosting(directory.path(), "wait").await;
        let pid = i32::try_from(host.id().expect("the host runs")).expect("a process id");
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), signal).expect("it is signalled");
        let status = host.wait().await.expect("the host ends");
        // Stopped before the host exited: what is waited for is whatever adopted them
        // reaping them.
        assert!(
            all_gone(&members).await,
            "{signal}: a member outlived the host"
        );
        assert_eq!(status.code(), Some(code), "{signal}");
    }
}

/// Whether `member` has ended within [`ENDING`], reaped or not: `ps` says so of one that
/// nothing reaped, which a signal still reaches.
async fn ended(member: i32) -> bool {
    let unreaped = || {
        let state = std::process::Command::new("ps")
            .args(["-o", "state=", "-p", &member.to_string()])
            .output();
        state.is_ok_and(|state| state.stdout.trim_ascii_start().starts_with(b"Z"))
    };
    let deadline = tokio::time::Instant::now() + ENDING;
    while tokio::time::Instant::now() < deadline {
        if wait_gone(member, Duration::ZERO).await || unreaped() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// Kills the process group of each of `members`, so that what a member started ends too.
fn kill_groups(members: &[i32]) {
    for member in members {
        let group = nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(*member)));
        if let Ok(group) = group {
            nix::sys::signal::killpg(group, nix::sys::signal::Signal::SIGKILL).ok();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_that_drops_its_connectors_and_returns_has_asked_each_group_to_stop() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    // Connectors that end only when killed: each leads its group for as long as the host
    // lives, so what reaches its members is the stop the drop sent, and nothing a leader's
    // own exit would have its host send.
    let lingering = r#"{"linger": "forever"}"#;
    let (mut host, members) = hosting_configured(directory.path(), "leave", lingering).await;
    let status = host.wait().await.expect("the host ends");
    assert_eq!(status.code(), Some(0));
    let [first, ignoring, second, also_ignoring] = members.as_slice() else {
        panic!("{members:?}");
    };
    // Asked before the drop returned, what ends when asked has ended: its leader lives and
    // reaps nothing, so it is there still, as what ended and nothing reaped.
    let asked = ended(*first).await && ended(*second).await;
    // What ignores being asked is killed only by a host that waits: this one did not.
    let alive =
        |member: &i32| nix::sys::signal::kill(nix::unistd::Pid::from_raw(*member), None).is_ok();
    let left = alive(ignoring) && alive(also_ignoring);
    kill_groups(&members);
    assert!(asked, "a member was not asked to stop");
    assert!(
        left,
        "a member that ignores a stop was killed by a host that had gone"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_that_panics_stops_its_connectors_groups_as_it_unwinds() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let (mut host, members) = hosting(directory.path(), "panic").await;
    let status = host.wait().await.expect("the host ends");
    let gone = all_gone(&members).await;
    kill_groups(&members);
    assert!(gone, "a member outlived a host that panicked");
    assert_eq!(status.code(), Some(101));
}

/// A launcher in `directory` whose connector leaves the group it was started to lead for its
/// host's, before it serves.
#[cfg(target_os = "linux")]
fn leaver(directory: &Path) -> ConnectorRef {
    let path = directory.join("launcher");
    let script = format!(
        "#!/bin/sh\nexec perl -e 'setpgrp(0, getpgrp(getppid())); exec @ARGV' '{}' \"$@\"\n",
        example("scripted_connector").display()
    );
    std::fs::write(&path, script).expect("the launcher writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the launcher is executable");
    let id = ConnectorId::parse("test.scripted").expect("a valid id");
    ConnectorRef::new(id).path(path)
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_connector_that_left_the_group_it_led_is_stopped_and_killed_all_the_same() {
    let kills = Kills::new();
    // One ends only when killed, and is dropped; the other is killed outright.
    for killed in [false, true] {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let source = local()
            .grace(Duration::from_millis(200))
            .kills(&kills)
            .source(
                &leaver(directory.path()),
                &serde_json::json!({ "linger": "forever" }),
            )
            .await
            .expect("the connector starts")
            .connector;
        let spawned = rdlt_host::spawned();
        let [leader] = spawned.as_slice() else {
            panic!("{spawned:?}");
        };
        let leader = i32::try_from(*leader).expect("a process id");
        if killed {
            kills.kill();
        } else {
            drop(source);
        }
        let stopping = tokio::task::spawn_blocking(|| rdlt_host::stop_spawned(ENDING));
        let stopped = stopping.await.expect("it returns");
        // The host's own child is reached by its process id, wherever it took itself.
        assert!(all_gone(&[leader]).await, "killed: {killed}: {stopped:?}");
        assert_eq!(stopped, Ok(()), "killed: {killed}");
    }
}

/// The state letter of process `pid` in `/proc`: `Z` for one that ended and nothing reaped.
#[cfg(target_os = "linux")]
fn state(pid: i32) -> Option<char> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit_once(')')?.1.trim_start().chars().next()
}

#[cfg(target_os = "linux")]
#[test]
fn members_killed_and_never_reaped_do_not_make_a_stopped_group_linger() {
    // This process adopts what its connectors orphan and reaps none of it, as a host that is
    // the first process of a container with no init does.
    let adopting = rustix::process::Pid::from_raw(1);
    rustix::process::set_child_subreaper(adopting).expect("this process adopts orphans");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let runtime = tokio::runtime::Runtime::new().expect("a runtime");
    let local = local().grace(Duration::from_millis(200));
    let (launched, config) = (launcher(directory.path()), serde_json::json!({}));
    let source = runtime
        .block_on(local.source(&launched, &config))
        .expect("the connector starts");
    let members = members(directory.path(), 1);
    drop(source);
    let began = std::time::Instant::now();
    let stopped = rdlt_host::stop_spawned(ENDING);
    // Every member ended, and stays as what nothing reaped.
    let states: Vec<Option<char>> = members.iter().map(|member| state(*member)).collect();
    assert_eq!(states, [Some('Z'), Some('Z')]);
    assert_eq!(stopped, Ok(()), "a group of the dead was taken for living");
    // And telling so does not wait out what a living member is given.
    assert!(
        began.elapsed() < Duration::from_secs(4),
        "{:?}",
        began.elapsed()
    );
}
