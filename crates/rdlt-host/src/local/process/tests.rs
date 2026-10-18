use std::ffi::OsString;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use nix::errno::Errno;
use nix::sys::wait::{WaitPidFlag, waitpid};
use rdlt_connector::ConnectorId;

use super::{Confinement, Launch, Process, Steps, Unspawned};
use crate::local::binary::Binary;
use crate::local::grants::{Claim, Guarded, Lease, Leases, Roots};
use crate::local::sandbox::{Confined, Grants, Launcher, NetworkGrant, Sandbox, SandboxError};
use crate::secrets::Redactions;

/// What `leases` hold for a placement granted to write `written`.
fn writing(leases: &Leases, written: &Path) -> Result<Lease, SandboxError> {
    let grants = Grants {
        write: vec![written.to_owned()],
        ..Grants::default()
    };
    let roots = Roots {
        read: Vec::new(),
        write: vec![written.to_owned()],
    };
    leases.take(&Claim {
        grants: &grants,
        shared_reads: &[],
        roots: &roots,
        guarded: &Guarded::default(),
        programs: &[],
    })
}

/// A script that serves nothing and ends only when it is made to.
fn sleeper(directory: &Path) -> Launch {
    script(directory, "exec sleep 300")
}

/// A script of `body` that serves nothing.
fn script(directory: &Path, body: &str) -> Launch {
    let path = directory.join("sleeper");
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("the script writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the script is executable");
    Launch {
        id: ConnectorId::parse("test.sleeper").expect("a valid id"),
        binary: Arc::new(Binary::at(&path).expect("the script opens")),
        digest: None,
        env_passthrough: Vec::new(),
        grace: Duration::from_millis(100),
        kills: None,
        told: None,
        confinement: None,
        lease: Arc::new(writing(&Leases::default(), directory).expect("held")),
        state_bytes: None,
    }
}

#[test]
fn a_connector_is_told_the_hosts_limit_on_state_where_it_raises_the_protocols() {
    use rdlt_wire::Limits;
    use rdlt_wire::limits::STATE_BYTES;
    let state = |state_bytes| {
        super::told_state(&Limits {
            state_bytes,
            ..Limits::default()
        })
    };
    assert_eq!(state(STATE_BYTES + 1), Some(STATE_BYTES + 1));
    // A host whose memory takes less than the protocol's still sends as much.
    assert_eq!(state(STATE_BYTES), None);
    assert_eq!(state(STATE_BYTES / 8), None);
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut launch = sleeper(directory.path());
    assert_eq!(super::command::arguments(&launch), ["--rdlt-fd=3"]);
    launch.state_bytes = Some(1 << 25);
    assert_eq!(
        super::command::arguments(&launch),
        ["--rdlt-fd=3", "--max-state-bytes=33554432"]
    );
}

fn socket() -> OwnedFd {
    let (_host, connector) = std::os::unix::net::UnixStream::pair().expect("a socket pair");
    connector.into()
}

fn failing<T>() -> std::io::Result<T> {
    Err(std::io::Error::other("the step fails"))
}

#[tokio::test]
async fn a_connector_whose_owning_fails_at_any_step_is_killed_reaped_and_forgotten() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launch = sleeper(directory.path());
    let failures = [
        Steps {
            stdout: |_| failing(),
            ..Steps::TAKEN
        },
        Steps {
            stderr: |_| failing(),
            ..Steps::TAKEN
        },
        Steps {
            thread: |_, _| failing(),
            ..Steps::TAKEN
        },
    ];
    for (step, steps) in failures.iter().enumerate() {
        let spawned = Process::spawn_by(&launch, socket(), Redactions::new(), steps);
        assert!(spawned.is_err(), "step {step}");
        // This process has no child left: the connector was killed, and reaped.
        let child = waitpid(None, Some(WaitPidFlag::WNOHANG));
        assert_eq!(child, Err(Errno::ECHILD), "step {step}");
        // And none is listed as one it spawned, to be waited for by whoever stops them.
        assert_eq!(super::spawned(), Vec::<u32>::new(), "step {step}");
        assert_eq!(super::stop_spawned(Duration::ZERO), Ok(()), "step {step}");
        assert_eq!(super::group::threads(), 0, "step {step}");
        // Its place among the groups this process may own is free again.
        assert_eq!(super::group::reserved(), 0, "step {step}");
    }
    // With every step taken the connector is owned, and ends when it is dropped.
    let process =
        Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN).expect("it starts");
    assert_eq!(super::spawned().len(), 1);
    // Listed, its group holds its place: no start holds one besides.
    assert_eq!(super::group::reserved(), 0);
    drop(process);
    assert_eq!(super::group::threads(), 1);
    assert_eq!(super::stop_spawned(Duration::from_secs(20)), Ok(()));
    // The thread that owned it is joined: none outlives the group it owned.
    assert_eq!(super::group::threads(), 0);
    assert_eq!(
        waitpid(None, Some(WaitPidFlag::WNOHANG)),
        Err(Errno::ECHILD)
    );
}

/// A sandbox no test may ask for a launcher.
#[derive(Debug)]
struct Unasked;

impl Sandbox for Unasked {
    fn launcher(&self, _confined: &Confined<'_>) -> Result<Launcher, SandboxError> {
        panic!("a launcher was asked for");
    }
}

#[test]
fn a_sandboxed_connector_is_not_spawned_where_descriptors_cannot_be_marked_at_once() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut launch = sleeper(directory.path());
    launch.confinement = Some(Confinement {
        sandbox: Arc::new(Unasked),
        network: NetworkGrant::Denied,
    });
    let unmarked = Steps {
        marks_at_once: || false,
        ..Steps::TAKEN
    };
    let refused = Process::spawn_by(&launch, socket(), Redactions::new(), &unmarked);
    let Err(Unspawned::Sandbox(refused)) = refused else {
        panic!("it was not refused");
    };
    assert_eq!(refused, SandboxError::Descriptors);
    assert_eq!(refused.code(), "sandbox_descriptors");
    // Refused before anything was launched, it gave back the place it took.
    assert_eq!(super::group::reserved(), 0);
}

#[test]
fn a_connector_whose_process_cannot_start_gives_back_its_place() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut launch = sleeper(directory.path());
    // Its interpreter does not exist: the kernel refuses to execute it, in the owning thread.
    let unstartable = directory.path().join("unstartable");
    std::fs::write(&unstartable, "#!/nonexistent/interpreter\n").expect("the script writes");
    std::fs::set_permissions(
        &unstartable,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .expect("the script is executable");
    launch.binary = Arc::new(Binary::at(&unstartable).expect("the script opens"));
    let refused = Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN);
    let Err(Unspawned::Io(_)) = refused else {
        panic!("it was not refused as a process that did not start");
    };
    assert_eq!(super::group::reserved(), 0);
    assert_eq!(super::spawned(), Vec::<u32>::new());
    assert_eq!(super::group::threads(), 0);
}

/// A sandbox whose launcher cannot make one: a spawn that asks it has passed every check
/// before it.
#[derive(Debug)]
struct Unavailable;

impl Sandbox for Unavailable {
    fn launcher(&self, _confined: &Confined<'_>) -> Result<Launcher, SandboxError> {
        Err(SandboxError::Unavailable {
            path: "/nowhere".into(),
            said: String::new(),
        })
    }
}

#[test]
fn a_sandboxed_connector_is_not_spawned_once_its_launcher_has_a_second_name() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let (bin, data) = (directory.path().join("bin"), directory.path().join("data"));
    std::fs::create_dir(&bin).expect("a directory");
    std::fs::create_dir(&data).expect("a directory");
    let launcher = bin.join("bwrap");
    std::fs::write(&launcher, "x").expect("the launcher writes");
    let mut launch = sleeper(&bin);
    let grants = Grants {
        write: vec![data.clone()],
        ..Grants::default()
    };
    let roots = Roots {
        read: Vec::new(),
        write: vec![data.clone()],
    };
    let guarded = Guarded {
        directories: Vec::new(),
        files: vec![launcher.clone()],
    };
    let lease = Leases::default().take(&Claim {
        grants: &grants,
        shared_reads: &[],
        roots: &roots,
        guarded: &guarded,
        programs: &[],
    });
    launch.lease = Arc::new(lease.expect("one name, beside"));
    launch.confinement = Some(Confinement {
        sandbox: Arc::new(Unavailable),
        network: NetworkGrant::Denied,
    });
    let marking = Steps {
        marks_at_once: || true,
        ..Steps::TAKEN
    };
    let refused = || {
        let spawned = Process::spawn_by(&launch, socket(), Redactions::new(), &marking);
        let Err(Unspawned::Sandbox(refused)) = spawned else {
            panic!("it was not refused");
        };
        refused
    };
    assert_eq!(refused().code(), "sandbox_unavailable");
    std::fs::hard_link(&launcher, data.join("alias")).expect("a second name");
    assert_eq!(refused(), SandboxError::Exposed { path: launcher });
    assert_eq!(super::spawned(), Vec::<u32>::new());
}

#[tokio::test]
async fn a_binary_given_a_second_name_since_it_was_placed_is_not_spawned() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launch = sleeper(directory.path());
    let path = directory.path().join("sleeper");
    std::fs::hard_link(&path, directory.path().join("alias")).expect("a second name");
    let refused = Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN);
    let Err(Unspawned::Sandbox(refused)) = refused else {
        panic!("it was not refused");
    };
    assert_eq!(refused, SandboxError::Exposed { path: path.clone() });
    assert_eq!(refused.code(), "program_exposed");
    assert_eq!(super::spawned(), Vec::<u32>::new());
    // Placed with both names, as its placement was then held, it is spawned, with no digest.
    let placed = Launch {
        binary: Arc::new(Binary::at(&path).expect("the script opens")),
        ..launch
    };
    assert_eq!(placed.digest, None);
    let process =
        Process::spawn_by(&placed, socket(), Redactions::new(), &Steps::TAKEN).expect("it starts");
    drop(process);
    assert_eq!(super::stop_spawned(Duration::from_secs(20)), Ok(()));
}

#[tokio::test]
async fn a_scripts_interpreter_given_a_second_name_since_it_was_placed_is_not_spawned() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let interpreter = directory.path().join("sh");
    std::fs::copy("/bin/sh", &interpreter).expect("the shell copies");
    let mut launch = sleeper(directory.path());
    let path = directory.path().join("sleeper");
    let line = format!("#!{}\nexec sleep 300\n", interpreter.display());
    std::fs::write(&path, line).expect("the script writes");
    let leases = Leases::default();
    let placed = || {
        let binary = Binary::at(&path).expect("the script opens");
        let programs = crate::local::programs(&binary).expect("its programs");
        let lease = leases.take(&Claim {
            grants: &Grants::default(),
            shared_reads: &[],
            roots: &Roots::default(),
            guarded: &Guarded::default(),
            programs: &programs,
        });
        (Arc::new(binary), Arc::new(lease.expect("held")))
    };
    (launch.binary, launch.lease) = placed();
    // A file with two names that takes the binary's name is not the file placed, which is run.
    let replacement = directory.path().join("replacement");
    std::fs::copy(&path, &replacement).expect("the script copies");
    std::fs::hard_link(&replacement, directory.path().join("alias")).expect("a second name");
    std::fs::rename(&replacement, &path).expect("it takes the binary's name");
    let process =
        Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN).expect("it starts");
    drop(process);
    assert_eq!(super::stop_spawned(Duration::from_secs(20)), Ok(()));
    std::fs::hard_link(&interpreter, directory.path().join("another-sh")).expect("a second name");
    let refused = Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN);
    let Err(Unspawned::Sandbox(refused)) = refused else {
        panic!("it was not refused");
    };
    assert_eq!(refused, SandboxError::Exposed { path: interpreter });
    assert_eq!(super::spawned(), Vec::<u32>::new());
    // Placed with both names, as its placement was then held, it is spawned.
    (launch.binary, launch.lease) = placed();
    let process =
        Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN).expect("it starts");
    drop(process);
    assert_eq!(super::stop_spawned(Duration::from_secs(20)), Ok(()));
}

#[tokio::test(flavor = "multi_thread")]
async fn what_a_placement_holds_is_held_until_its_last_connector_is_reaped() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let granted = directory.path().join("granted");
    std::fs::create_dir(&granted).expect("a directory");
    let leases = Leases::default();
    // A connector that ignores its stop once it says so, and has a grace to linger through.
    let ignoring = directory.path().join("ignoring");
    let body = format!(
        "trap '' TERM\ntouch '{}'\nexec sleep 300",
        ignoring.display()
    );
    let mut launch = script(directory.path(), &body);
    launch.grace = Duration::from_secs(3);
    launch.lease = Arc::new(writing(&leases, &granted).expect("held"));
    let process = Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN);
    let process = process.expect("it starts");
    for _ in 0..1000 {
        if ignoring.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        ignoring.exists(),
        "the connector never came to ignore its stop"
    );
    drop(launch);
    drop(process);
    // Its placement and its handle are gone, and it still runs: what it was granted is held.
    let refused = writing(&leases, &granted).expect_err("held still");
    assert_eq!(refused.code(), "grant_overlap");
    let stopping = tokio::task::spawn_blocking(|| super::stop_spawned(Duration::from_secs(20)));
    assert_eq!(stopping.await.expect("it returns"), Ok(()));
    drop(writing(&leases, &granted).expect("reaped, it is free"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_while_a_connector_is_launched_is_raised_where_it_was_asked_for() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut launch = sleeper(directory.path());
    launch.told = Some(super::Told::new(|_| panic!("the host's own code panics")));
    let launching = tokio::spawn(async move {
        Process::launching(launch, Redactions::new(), Duration::from_secs(60))
            .await
            .map(drop)
            .map_err(drop)
    });
    let joined = launching.await.expect_err("it panics, not fails");
    assert!(joined.is_panic());
    // What it spawned before the panic is stopped with every connector.
    let stopping = tokio::task::spawn_blocking(|| super::stop_spawned(Duration::from_secs(20)));
    assert_eq!(stopping.await.expect("it returns"), Ok(()));
}

/// Steps whose owning thread starts a second late: a launch slower than its deadline.
static SLOW: Steps = Steps {
    thread: |thread, owning| {
        std::thread::sleep(Duration::from_secs(1));
        thread.spawn(owning)
    },
    ..Steps::TAKEN
};

#[tokio::test(flavor = "multi_thread")]
async fn a_launch_past_its_deadline_is_refused_and_what_it_starts_is_stopped() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut launch = sleeper(directory.path());
    let (told, started) = std::sync::mpsc::channel();
    launch.told = Some(super::Told::new(move |pid| {
        told.send(pid).ok();
    }));
    let launching =
        Process::launching_by(launch, Redactions::new(), Duration::from_millis(50), &SLOW);
    let launched = tokio::time::timeout(Duration::from_millis(500), launching)
        .await
        .expect("the deadline ends the launch");
    let Err(refused) = launched else {
        panic!("it was launched");
    };
    let (_, unspawned) = *refused;
    let Unspawned::Io(error) = unspawned else {
        panic!("{unspawned}");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut, "{error}");
    // The launch goes on past its deadline; what it starts is dropped as it ends.
    let waiting =
        tokio::task::spawn_blocking(move || started.recv_timeout(Duration::from_secs(20)));
    waiting
        .await
        .expect("it returns")
        .expect("the late launch starts its connector");
    for _ in 0..400 {
        if super::spawned().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(super::spawned(), Vec::<u32>::new(), "it was left running");
    // Its place among the groups this process may own moved to the group as it was entered.
    assert_eq!(super::group::reserved(), 0);
    let stopping = tokio::task::spawn_blocking(|| super::stop_spawned(Duration::from_secs(20)));
    assert_eq!(stopping.await.expect("it returns"), Ok(()));
}

#[test]
fn a_launch_whose_runtime_has_shut_down_is_refused_not_raised() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launch = sleeper(directory.path());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("a runtime");
    let handle = runtime.handle().clone();
    runtime.shutdown_background();
    // The blocking task it is launched on is cancelled before it starts.
    let launched = handle.block_on(Process::launching(
        launch,
        Redactions::new(),
        Duration::from_secs(60),
    ));
    let Err(refused) = launched else {
        panic!("it was launched");
    };
    let (asked, unspawned) = *refused;
    assert_eq!(
        asked.id,
        ConnectorId::parse("test.sleeper").expect("a valid id")
    );
    assert!(matches!(unspawned, Unspawned::Io(_)), "{unspawned}");
}

/// The `GLIBC_TUNABLES` a connector is started with, given the environment passed through.
fn tunables(passed: &[(&str, &str)]) -> String {
    let mut env: Vec<(OsString, OsString)> = passed
        .iter()
        .map(|(name, value)| (OsString::from(name), OsString::from(value)))
        .collect();
    super::command::tuned(&mut env);
    let found: Vec<_> = env
        .iter()
        .filter(|(name, _)| name == "GLIBC_TUNABLES")
        .collect();
    assert_eq!(found.len(), 1, "{env:?}");
    found[0].1.to_str().expect("text").to_owned()
}

const TRIM: &str = "glibc.malloc.trim_threshold=268435456";
const MMAP: &str = "glibc.malloc.mmap_threshold=33554432";

#[test]
fn a_connector_starts_with_the_malloc_tunables_the_operator_has_not_set() {
    assert_eq!(tunables(&[]), format!("{TRIM}:{MMAP}"));
    assert_eq!(
        tunables(&[("GLIBC_TUNABLES", "")]),
        format!("{TRIM}:{MMAP}")
    );
    assert_eq!(
        tunables(&[("GLIBC_TUNABLES", "glibc.malloc.trim_threshold=1")]),
        format!("glibc.malloc.trim_threshold=1:{MMAP}")
    );
    let both = "glibc.malloc.mmap_threshold=7:glibc.malloc.trim_threshold=1";
    assert_eq!(tunables(&[("GLIBC_TUNABLES", both)]), both);
    assert_eq!(
        tunables(&[("GLIBC_TUNABLES", "glibc.malloc.arena_max=2")]),
        format!("glibc.malloc.arena_max=2:{TRIM}:{MMAP}")
    );
    // A tunable is the operator's by glibc's own variable for it too.
    assert_eq!(tunables(&[("MALLOC_MMAP_THRESHOLD_", "4096")]), TRIM);
    assert_eq!(
        tunables(&[
            ("MALLOC_TRIM_THRESHOLD_", "4096"),
            ("GLIBC_TUNABLES", "glibc.malloc.arena_max=2")
        ]),
        format!("glibc.malloc.arena_max=2:{MMAP}")
    );
}

#[test]
fn a_connector_whose_operator_set_every_malloc_tunable_is_not_given_the_variable() {
    let mut env = vec![
        (
            OsString::from("MALLOC_TRIM_THRESHOLD_"),
            OsString::from("1"),
        ),
        (
            OsString::from("MALLOC_MMAP_THRESHOLD_"),
            OsString::from("2"),
        ),
    ];
    super::command::tuned(&mut env);
    assert_eq!(env.len(), 2, "{env:?}");
}
