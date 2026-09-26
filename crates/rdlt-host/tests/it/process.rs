//! Connectors spawned in processes of their own: found, started with their socket on file
//! descriptor 3, their output drained, stopped, respawned when lost, and never orphaned.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{ConnectorErrorKind, ConnectorId, PipelineId, StreamName};
use rdlt_engine::{CommitPolicy, Engine, EngineConfig, RayonPool, RetryPolicy, RunStatus};
use rdlt_engine::{PipelinePlan, StreamPlan, SystemEnv};
use rdlt_host::{ConnectorRef, Digest, LastWords, Local, Placement, Provider as _, ProviderError};
use sha2::Digest as _;

use crate::support::memory_destination;

/// The example `name`, which the test build builds beside the tests: in the first directory above
/// the test binary that holds an `examples` directory with it, whichever layout the build uses.
pub(crate) fn example(name: &str) -> PathBuf {
    let tests = std::env::current_exe().expect("the test binary has a path");
    tests
        .ancestors()
        .map(|dir| dir.join("examples").join(name))
        .find(|example| example.is_file())
        .expect("the test build builds the examples")
}

/// Places connectors in processes of their own, whose coverage, when measured, is kept.
fn local() -> Local {
    Local::new().env_passthrough("LLVM_PROFILE_FILE")
}

fn scripted() -> ConnectorRef {
    let id = ConnectorId::parse("test.scripted").expect("a valid id");
    ConnectorRef::new(id).path(example("scripted_connector"))
}

/// The scripted connector, spawned with `script`.
async fn spawned(local: &Local, script: serde_json::Value) -> Box<dyn rdlt_connector::Source> {
    local
        .source(&scripted(), &script)
        .await
        .expect("the connector starts")
        .connector
}

/// Whether process `pid` is gone.
fn gone(pid: i32) -> bool {
    let pid = nix::unistd::Pid::from_raw(pid);
    nix::sys::signal::kill(pid, None).is_err()
}

/// Waits up to `patience` for process `pid` to be gone.
async fn wait_gone(pid: i32, patience: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + patience;
    while tokio::time::Instant::now() < deadline {
        if gone(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gone(pid)
}

#[tokio::test]
async fn a_spawned_connector_is_placed_with_its_spec_and_the_digest_of_its_binary() {
    let placed = local()
        .source(&scripted(), &serde_json::json!({}))
        .await
        .expect("the connector starts");
    assert_eq!(placed.spec.id.as_str(), "test.scripted");
    assert_eq!(
        placed.placement,
        Placement::Process {
            path: example("scripted_connector")
        }
    );
    let binary = std::fs::read(example("scripted_connector")).expect("the binary reads");
    let digest = Digest(sha2::Sha256::digest(binary).into());
    assert_eq!(placed.digest, Some(digest));
    placed.connector.check().await.expect("the check passes");
}

#[tokio::test]
async fn connector_writing_stdout_keeps_running() {
    // Far more than a pipe holds: a host that did not drain the connector's output would hang.
    let script = serde_json::json!({ "stdout_bytes": 4 * 1024 * 1024 });
    let source = spawned(&local(), script).await;
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(20), source.check())
            .await
            .expect("the check ends")
            .expect("the check passes");
    }
}

#[tokio::test]
async fn connector_crash_error_carries_stderr_tail() {
    let script = serde_json::json!({ "crash": "boom: the disk melted" });
    let source = spawned(&local(), script).await;
    let error = source.check().await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Transient, "{error}");
    let words = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<LastWords>())
        .expect("the error carries the connector's last words");
    assert!(words.stderr.contains("boom: the disk melted"), "{words}");
    assert_eq!(words.exit.and_then(|exit| exit.code()), Some(3), "{words}");
    let shown = words.to_string();
    assert!(
        shown.contains("exited") && shown.contains("boom: the disk melted"),
        "{shown}"
    );
}

#[tokio::test]
async fn a_crashed_connectors_last_words_keep_only_the_tail_of_its_stderr() {
    let words = format!("{}the end", "x".repeat(20_000));
    let source = spawned(&local(), serde_json::json!({ "crash": words })).await;
    let error = source.check().await.unwrap_err();
    let kept = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<LastWords>())
        .expect("the error carries the connector's last words");
    assert_eq!(kept.stderr.len(), 8 * 1024);
    assert!(
        kept.stderr.ends_with("the end\n"),
        "{}",
        &kept.stderr[8000..]
    );
}

#[test]
fn last_words_say_how_the_connector_ended() {
    let running = LastWords {
        exit: None,
        stderr: String::new(),
    };
    assert_eq!(
        running.to_string(),
        "the connector is running, and wrote nothing to its standard error"
    );
}

#[tokio::test]
async fn env_passthrough_reaches_connector() {
    let (name, value) = ("CARGO_MANIFEST_DIR", env!("CARGO_MANIFEST_DIR"));
    // Kept when passed through, and everything else cleared, even `PATH`.
    let script = serde_json::json!({ "env": { name: value, "PATH": null } });
    let source = spawned(&local().env_passthrough(name), script).await;
    source.check().await.expect("the environment is as passed");
    let script = serde_json::json!({ "env": { name: null } });
    let source = spawned(&local(), script).await;
    source.check().await.expect("nothing else is passed");
    // A connector's own error is its own, without the transport's last words.
    let script = serde_json::json!({ "env": { "PATH": "/bin" } });
    let error = spawned(&local(), script).await.check().await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
    assert!(std::error::Error::source(&error).is_none(), "{error:?}");
}

#[tokio::test]
async fn a_connectors_own_children_do_not_inherit_the_hosts_socket() {
    let source = spawned(&local(), serde_json::json!({ "probe_fd_3": true })).await;
    source
        .check()
        .await
        .expect("the host's socket is the connector's alone");
}

#[tokio::test]
async fn sigint_leaves_no_orphaned_connectors() {
    use tokio::io::AsyncBufReadExt as _;
    let mut host = tokio::process::Command::new(example("connector_host"))
        .arg(example("scripted_connector"))
        .stdout(std::process::Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn()
        .expect("the host starts");
    let host_pid = host.id().expect("the host runs");
    let stdout = host.stdout.take().expect("its output is piped");
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    assert_eq!(
        lines.next_line().await.expect("it reads").as_deref(),
        Some("ready")
    );
    let children = std::process::Command::new("pgrep")
        .args(["-P", &host_pid.to_string()])
        .output()
        .expect("pgrep runs");
    let children: Vec<i32> = String::from_utf8_lossy(&children.stdout)
        .lines()
        .map(|pid| pid.trim().parse().expect("pgrep lists process ids"))
        .collect();
    assert_eq!(children.len(), 2, "{children:?}");
    // A terminal's Ctrl-C: SIGINT to the whole foreground process group.
    let group = nix::unistd::Pid::from_raw(i32::try_from(host_pid).expect("a process id"));
    nix::sys::signal::killpg(group, nix::sys::signal::Signal::SIGINT).expect("the group exists");
    host.wait().await.expect("the host ends");
    for child in children {
        assert!(
            wait_gone(child, Duration::from_secs(10)).await,
            "{child} is orphaned"
        );
    }
}

/// The scripted connector spawned by `local` with `script`, and its process id.
async fn spawned_with_pid(
    local: &Local,
    mut script: serde_json::Value,
) -> (Box<dyn rdlt_connector::Source>, i32, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let pid_file = dir.path().join("pid");
    script["pid_file"] = serde_json::json!(pid_file);
    let source = spawned(local, script).await;
    let pid = std::fs::read_to_string(&pid_file).expect("the connector wrote its id");
    (source, pid.parse().expect("a process id"), dir)
}

#[tokio::test]
async fn dropping_a_placed_connector_stops_it_even_when_it_outlives_its_socket() {
    // It exits only when told to: not when its socket or its standard input closes.
    let script = serde_json::json!({ "linger": "terminable" });
    let (source, pid, _dir) = spawned_with_pid(&local(), script).await;
    assert!(!gone(pid));
    drop(source);
    // Well within the default grace period: the stop is `SIGTERM`, not `SIGKILL` after it.
    assert!(wait_gone(pid, Duration::from_secs(5)).await);
}

#[tokio::test]
async fn a_connector_that_ignores_its_stop_is_killed_after_the_grace_period() {
    let script = serde_json::json!({ "linger": "forever" });
    let local = local().grace(Duration::from_millis(200));
    let (source, pid, _dir) = spawned_with_pid(&local, script).await;
    drop(source);
    assert!(wait_gone(pid, Duration::from_secs(5)).await);
}

#[tokio::test]
async fn a_live_connector_serves_every_call_without_respawning() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let pid_file = dir.path().join("pid");
    let source = spawned(&local(), serde_json::json!({ "pid_file": pid_file })).await;
    let first = std::fs::read_to_string(&pid_file).expect("the connector wrote its id");
    for _ in 0..3 {
        source.check().await.expect("the check passes");
    }
    // A respawned connector would have written its own id.
    let last = std::fs::read_to_string(&pid_file).expect("the connector wrote its id");
    assert_eq!(first, last);
}

#[tokio::test]
async fn a_placed_connector_runs_with_the_providers_options() {
    let deadlines = rdlt_host::Deadlines {
        check: Duration::from_millis(100),
        ..rdlt_host::Deadlines::default()
    };
    let options = rdlt_host::Options {
        deadlines,
        ..rdlt_host::Options::default()
    };
    let script = serde_json::json!({ "slow_check_ms": 2000 });
    let source = spawned(&local().options(options), script).await;
    let error = source.check().await.unwrap_err();
    assert_eq!(error.code(), Some(rdlt_host::DEADLINE_EXCEEDED));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crashed_connector_is_respawned_and_the_run_loads_every_row_once() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let script = serde_json::json!({
        "rows": 300,
        "crash_once_at": 170,
        "marker": dir.path().join("crashed"),
    });
    let source = local()
        .source(&scripted(), &script)
        .await
        .expect("the connector starts")
        .connector;
    let destination = memory_destination("respawned_rows", rdlt_host::Options::default()).await;
    let stream = StreamPlan::new(StreamName::new("rows").expect("a valid stream name"));
    let plan = PipelinePlan::new(PipelineId::parse("respawned").expect("valid"), [stream])
        .expect("a valid plan");
    let policy = CommitPolicy::new(None, Some(50), None).expect("a valid policy");
    let config = EngineConfig::builder()
        .commit(policy)
        .retry(RetryPolicy::default().initial(Duration::from_millis(10)))
        .build()
        .expect("a valid configuration");
    let threads = std::num::NonZeroUsize::new(2).expect("two is not zero");
    let env = SystemEnv::new(RayonPool::new(threads).expect("the compute pool starts"));
    let outcome = Engine::new(config, Arc::new(env))
        .run(plan, Arc::from(source), Arc::new(destination))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(
        dir.path().join("crashed").exists(),
        "the connector crashed once"
    );
    let mut ids: Vec<i64> = rdlt_connector_reference::published("respawned_rows", "rows")
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("id")
                .and_then(|ids| ids.as_any().downcast_ref::<arrow_array::Int64Array>())
                .expect("ids are Int64");
            ids.values().to_vec()
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, (0..300).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_connector_is_found_by_name_in_a_connector_dir_before_path() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    std::os::unix::fs::symlink(
        example("scripted_connector"),
        dir.path().join("rdlt-connector-scripted"),
    )
    .expect("the link is made");
    let id = ConnectorId::parse("test.scripted").expect("a valid id");
    let in_dir = local().connector_dir(dir.path());
    let found = in_dir
        .resolve(&ConnectorRef::new(id.clone()))
        .expect("found");
    assert_eq!(found, dir.path().join("rdlt-connector-scripted"));
    let missing = local().resolve(&ConnectorRef::new(id)).unwrap_err();
    assert!(
        matches!(missing, ProviderError::NotFound { .. }),
        "{missing}"
    );
}

#[tokio::test]
async fn a_connector_of_another_version_or_id_is_refused() {
    let config = serde_json::json!({});
    let newer = scripted().version(semver::VersionReq::parse(">=9").expect("valid"));
    let refused = local()
        .source(&newer, &config)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, ProviderError::VersionMismatch { .. }),
        "{refused}"
    );
    let other = ConnectorRef::new(ConnectorId::parse("test.other").expect("a valid id"))
        .path(example("scripted_connector"));
    let refused = local()
        .source(&other, &config)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(refused, ProviderError::HandshakeFailed { .. }),
        "{refused}"
    );
    let accepted = scripted().version(semver::VersionReq::parse(">=0.0.0").expect("valid"));
    assert!(local().source(&accepted, &config).await.is_ok());
    let absent = scripted().path("/nonexistent/rdlt-connector-scripted");
    let refused = local()
        .source(&absent, &config)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(
            refused,
            ProviderError::NotFound {
                source: Some(_),
                ..
            }
        ),
        "{refused}"
    );
}

#[tokio::test]
async fn a_configuration_the_connector_refuses_fails_its_handshake_with_its_own_error() {
    let bad = serde_json::json!({ "rows": "many" });
    let refused = local()
        .source(&scripted(), &bad)
        .await
        .err()
        .expect("refused");
    let ProviderError::HandshakeFailed { source, .. } = &refused else {
        panic!("{refused}");
    };
    assert_eq!(source.kind(), ConnectorErrorKind::Config);
    assert!(
        std::error::Error::source(source.as_ref()).is_none(),
        "{source:?}"
    );
}

#[tokio::test]
async fn a_path_that_is_no_program_is_not_found() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let plain = dir.path().join("plain");
    std::fs::write(&plain, "not a program").expect("the file is written");
    for path in [plain, dir.path().to_owned()] {
        let refused = local().resolve(&scripted().path(&path)).unwrap_err();
        assert!(
            matches!(refused, ProviderError::NotFound { .. }),
            "{refused}"
        );
    }
}

#[tokio::test]
async fn a_destination_crashing_in_a_session_carries_its_last_words() {
    use rdlt_connector::{CommitMeta, CommitSeq, LoadId, OpenContext, SegmentSet};
    let id = ConnectorId::parse("io.rapidbyte.memory").expect("a valid id");
    let reference = ConnectorRef::new(id).path(example("crashing_destination"));
    let destination = local()
        .destination(&reference, &serde_json::json!({ "store": "crashing" }))
        .await
        .expect("the destination starts")
        .connector;
    let context = OpenContext {
        pipeline: PipelineId::parse("crashing").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("the session opens");
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
    };
    let error = opened.session.commit(&meta).await.unwrap_err();
    let words = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<LastWords>())
        .expect("the error carries the connector's last words");
    assert!(words.stderr.contains("crashing in the commit"), "{words}");
}
