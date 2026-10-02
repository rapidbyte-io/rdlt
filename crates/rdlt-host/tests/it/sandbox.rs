//! Connectors spawned inside the sandbox rdlt ships: what a connector sees and reaches there,
//! that it serves the protocol all the same, and that it ends with its host and when it is
//! stopped or killed, with everything it started.
//!
//! Where bubblewrap makes no sandbox, as where unprivileged user namespaces are off, each of
//! these says so and checks nothing.

use rdlt_host::{Bubblewrap, Isolation, Local, Provider as _, ProviderError};

use crate::process::scripted;

#[tokio::test]
async fn a_sandbox_whose_launcher_is_missing_spawns_nothing_and_says_why() {
    let local = Local::sandboxed(Bubblewrap::at("/nonexistent/bwrap"));
    let config = serde_json::json!({});
    for refused in [
        local.source(&scripted(), &config).await.map(|_| ()),
        local.wire(&scripted()).await.map(|_| ()),
    ] {
        let error = refused.expect_err("no sandbox, no connector");
        assert!(matches!(error, ProviderError::Sandbox { .. }), "{error}");
        let missing = if cfg!(target_os = "linux") {
            "sandbox_missing"
        } else {
            "sandbox_unsupported"
        };
        assert_eq!(error.code(), missing);
    }
    assert!(rdlt_host::spawned().is_empty());
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn there_is_no_sandbox_here_so_an_untrusted_connector_is_refused() {
    let local = Local::sandboxed(Bubblewrap::new());
    let refused = local.source(&scripted(), &serde_json::json!({})).await;
    let error = refused.err().expect("no sandbox, no connector");
    assert_eq!(error.code(), "sandbox_unsupported");
    // Only binaries stated to be trusted run locally, and they give no sandbox.
    let trusting = Local::trusting_binaries();
    let sandbox = scripted().isolation(Isolation::Sandbox);
    let refused = trusting.source(&sandbox, &serde_json::json!({})).await;
    assert_eq!(
        refused.err().expect("refused").code(),
        "placement_unsupported"
    );
}

#[cfg(target_os = "linux")]
mod bubblewrap {
    use std::io::Write as _;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use rdlt_connector::{PipelineId, StreamName};
    use rdlt_engine::{PipelinePlan, RunStatus, StreamPlan};
    use rdlt_host::{Kills, LastWords, Options};

    use super::*;
    use crate::process::{example, local, wait_gone};
    use crate::support::{engine, memory_destination};

    /// How long what a sandbox held may take to be gone once its launcher is.
    const ENDING: Duration = Duration::from_secs(20);

    /// A provider that spawns into bubblewrap; none, with the reason said, where bubblewrap
    /// makes no sandbox.
    async fn sandboxed() -> Option<Local> {
        let local = Local::sandboxed(Bubblewrap::new());
        match local.wire(&scripted()).await {
            Ok(_) => Some(local),
            Err(ProviderError::Sandbox { source, .. }) => {
                writeln!(std::io::stderr(), "skipped: {source}").ok();
                None
            }
            Err(other) => panic!("the sandboxed connector did not spawn: {other}"),
        }
    }

    /// A listener on the host's loopback, and its address.
    fn listener() -> (std::net::TcpListener, String) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a listener");
        let address = listener.local_addr().expect("its address").to_string();
        (listener, address)
    }

    /// The process ids, as the host numbers them, of the processes whose command line holds
    /// `marker`.
    fn marked(marker: &str) -> Vec<i32> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir("/proc").expect("/proc lists").flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse().ok())
            else {
                continue;
            };
            let command = std::fs::read(entry.path().join("cmdline")).unwrap_or_default();
            if String::from_utf8_lossy(&command).contains(marker) {
                found.push(pid);
            }
        }
        found
    }

    /// A command line no other process has, for a process a connector starts and leaves.
    fn marker(test: &str) -> (String, String) {
        let marker = format!("rdlt-sandbox-{test}-{}", std::process::id());
        (format!("sleep 1000; : {marker}"), marker)
    }

    /// Waits for the processes marked `marker` to be `count`, within [`ENDING`].
    async fn marked_are(marker: &str, count: usize) -> bool {
        for _ in 0..ENDING.as_millis() / 20 {
            if marked(marker).len() == count {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // What is left is killed, so a failing test leaves nothing behind.
        for pid in marked(marker) {
            let pid = nix::unistd::Pid::from_raw(pid);
            nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL).ok();
        }
        false
    }

    #[tokio::test]
    async fn a_sandboxed_connector_sees_nothing_of_its_host_but_what_it_is_given() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let (_listener, address) = listener();
        let logs = tempfile::tempdir().expect("a temporary directory");
        std::fs::write(logs.path().join("pipeline.wal"), "the log").expect("it writes");
        let home = std::env::var("HOME").expect("a home directory");
        let own = std::env::current_exe().expect("the test binary has a path");
        let (name, value) = ("CARGO_MANIFEST_DIR", env!("CARGO_MANIFEST_DIR"));
        let script = serde_json::json!({
            "only_its_descriptors": true,
            // What the host states, and where the launcher put it.
            "whole_env": { name: value, "PWD": "/" },
            "absent": [
                home, "/home", "/root", "/etc/passwd", "/var", "/run", "/srv", "/mnt",
                logs.path(), logs.path().join("pipeline.wal"), value, own,
                example("scripted_connector"),
            ],
            "connects": [address, false],
            // The sandbox's own first process, and the connector.
            "sees_processes": 2,
        });
        let source = local
            .env_passthrough(name)
            .source(&scripted(), &script)
            .await
            .expect("the connector starts")
            .connector;
        source.check().await.expect("it is confined");
        // The same connector outside a sandbox sees all of it: the check tells.
        let unconfined = self::local()
            .env_passthrough(name)
            .source(&scripted(), &script)
            .await
            .expect("the connector starts")
            .connector;
        unconfined
            .check()
            .await
            .expect_err("a trusted binary has its host's access");
    }

    #[tokio::test]
    async fn a_sandboxed_connector_reaches_what_it_is_granted_and_no_more() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let (_listener, address) = listener();
        let granted = tempfile::tempdir().expect("a temporary directory");
        let (read, write) = (granted.path().join("read"), granted.path().join("write"));
        std::fs::create_dir(&read).expect("a directory");
        std::fs::create_dir(&write).expect("a directory");
        std::fs::write(read.join("given"), "given").expect("it writes");
        let granting = local
            .clone()
            .grant_read(&read)
            .grant_write(&write)
            .grant_network();
        let reaching = serde_json::json!({
            "readable": [read.join("given")],
            "writes": write.join("made"),
            "connects": [address, true],
            "absent": [granted.path().join("other")],
        });
        let source = granting
            .source(&scripted(), &reaching)
            .await
            .expect("it starts");
        source
            .connector
            .check()
            .await
            .expect("it reaches what it was granted");
        assert_eq!(
            std::fs::read_to_string(write.join("made")).expect("made"),
            "written"
        );
        // What is granted to be read is not written.
        let overreaching = serde_json::json!({ "writes": read.join("made") });
        let source = granting
            .source(&scripted(), &overreaching)
            .await
            .expect("it starts");
        source
            .connector
            .check()
            .await
            .expect_err("a read grant lets nothing be written");
        assert!(!read.join("made").exists());
        // A grant of a path that is not there refuses the spawn.
        let absent = local.grant_read(granted.path().join("absent"));
        let refused = absent
            .source(&scripted(), &reaching)
            .await
            .err()
            .expect("refused");
        assert_eq!(refused.code(), "sandbox_grant");
    }

    #[tokio::test]
    async fn a_sandbox_gives_the_isolation_a_reference_requires_of_it() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let config = serde_json::json!({});
        for isolation in [Isolation::Sandbox, Isolation::Process] {
            let reference = scripted().isolation(isolation);
            local
                .source(&reference, &config)
                .await
                .expect("the isolation is given");
        }
        let remote = scripted().isolation(Isolation::Remote);
        let refused = local.source(&remote, &config).await.err().expect("refused");
        assert_eq!(refused.code(), "placement_unsupported");
    }

    #[tokio::test]
    async fn a_script_is_run_in_a_sandbox_from_its_open_file_too() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let dir = tempfile::tempdir().expect("a temporary directory");
        let script = dir.path().join("rdlt-connector-script");
        std::fs::write(
            &script,
            "#!/bin/sh\necho \"ran as $0 with $*\" >&2\nexit 3\n",
        )
        .expect("the script writes");
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .expect("the script is executable");
        let wire = local
            .wire(&scripted().path(&script))
            .await
            .expect("it spawns");
        let witness = wire.witness().expect("it was spawned");
        let words: LastWords = witness.last_words(Duration::from_secs(10)).await;
        assert_eq!(words.exit.and_then(|exit| exit.code()), Some(3), "{words}");
        assert_eq!(words.stderr, r"ran as /rdlt-connector with --rdlt-fd=3\n");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sandboxed_source_that_crashes_is_spawned_again_and_loads_every_row_once() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let kept = tempfile::tempdir().expect("a temporary directory");
        // It crashes once, mid-read, and says so in a file it was granted to write.
        let script = serde_json::json!({
            "rows": 400,
            "crash_once_at": 250,
            "marker": kept.path().join("crashed"),
            "only_its_descriptors": true,
        });
        let source = local
            .grant_write(kept.path())
            .source(&scripted(), &script)
            .await
            .expect("the connector starts")
            .connector;
        let destination = memory_destination("sandboxed_rows", Options::default()).await;
        let stream = StreamPlan::new(StreamName::new("rows").expect("a valid stream name"));
        let plan = PipelinePlan::new(PipelineId::parse("sandboxed").unwrap(), [stream]).unwrap();
        let outcome = engine(50)
            .run(plan, Arc::from(source), Arc::new(destination))
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
        assert!(
            kept.path().join("crashed").exists(),
            "the source never crashed"
        );
        let mut ids: Vec<i64> = Vec::new();
        for batch in rdlt_connector_reference::published("sandboxed_rows", "rows") {
            let column = batch.column_by_name("id").expect("an id column");
            let column =
                arrow_array::cast::AsArray::as_primitive::<arrow_array::types::Int64Type>(column);
            ids.extend(column.values().iter());
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..400).collect::<Vec<i64>>());
    }

    /// A sandboxed connector that started a process which ignores `SIGTERM`, with `script`
    /// beside; the provider's kills, and what marks the process.
    async fn forking(
        local: &Local,
        test: &str,
        script: serde_json::Value,
    ) -> (Box<dyn rdlt_connector::Source>, Kills, String) {
        let (started, marker) = marker(test);
        let mut script = script;
        script["starts"] = serde_json::json!(started);
        let kills = Kills::new();
        let source = local
            .clone()
            .grace(Duration::from_millis(300))
            .kills(&kills)
            .source(&scripted(), &script)
            .await
            .expect("the connector starts")
            .connector;
        assert!(
            marked_are(&marker, 1).await,
            "the connector started nothing"
        );
        (source, kills, marker)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sandboxed_connector_that_is_dropped_ends_with_everything_it_started() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let (source, _kills, marker) = forking(&local, "dropped", serde_json::json!({})).await;
        // The host owns the launcher's group, and the launcher is its child.
        let spawned = rdlt_host::spawned();
        let [leader] = spawned.as_slice() else {
            panic!("{spawned:?}");
        };
        let name = std::fs::read_to_string(format!("/proc/{leader}/comm")).expect("it runs");
        assert_eq!(name.trim(), "bwrap");
        drop(source);
        // Asked to stop by the end of its input, the connector ends, and its sandbox with it.
        assert!(
            marked_are(&marker, 0).await,
            "what the connector started lives on"
        );
        let stopping = tokio::task::spawn_blocking(|| rdlt_host::stop_spawned(ENDING));
        assert_eq!(stopping.await.expect("it returns"), Ok(()));
        let leader = i32::try_from(*leader).expect("a process id");
        assert!(wait_gone(leader, ENDING).await);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sandboxed_connector_that_ignores_its_stop_is_killed_with_everything_it_started() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let lingering = serde_json::json!({ "linger": "forever" });
        let (source, _kills, marker) = forking(&local, "lingering", lingering).await;
        let began = std::time::Instant::now();
        drop(source);
        assert!(
            marked_are(&marker, 0).await,
            "what the connector started lives on"
        );
        // Not before its grace was over: it was asked first, and killed only then.
        assert!(
            began.elapsed() >= Duration::from_millis(300),
            "{:?}",
            began.elapsed()
        );
        let stopping = tokio::task::spawn_blocking(|| rdlt_host::stop_spawned(ENDING));
        assert_eq!(
            stopping.await.expect("it returns"),
            Ok(()),
            "a group lingers"
        );
        assert!(rdlt_host::spawned().is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sandboxed_connector_that_is_killed_ends_at_once_with_everything_it_started() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let lingering = serde_json::json!({ "linger": "forever" });
        let (source, kills, marker) = forking(&local, "killed", lingering).await;
        kills.kill();
        assert!(
            marked_are(&marker, 0).await,
            "what the connector started outlived the kill"
        );
        drop(source);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_host_out_of_patience_kills_its_sandboxed_connectors_before_it_returns() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let lingering = serde_json::json!({ "linger": "forever" });
        let patient = local.grace(Duration::from_secs(1000));
        let (started, marker) = marker("impatient");
        let mut script = lingering;
        script["starts"] = serde_json::json!(started);
        let source = patient
            .source(&scripted(), &script)
            .await
            .expect("it starts");
        assert!(marked_are(&marker, 1).await);
        let stopping = tokio::task::spawn_blocking(|| rdlt_host::stop_spawned(Duration::ZERO));
        assert_eq!(stopping.await.expect("it returns"), Ok(()));
        assert!(
            marked_are(&marker, 0).await,
            "what the connector started lives on"
        );
        drop(source);
    }

    /// A host of two sandboxed connectors, each of which started a marked process, in `mode`.
    async fn hosting(directory: &Path, mode: &str, marker: &str) -> tokio::process::Child {
        use tokio::io::AsyncBufReadExt as _;
        let config = serde_json::json!({ "linger": "forever", "starts": marker }).to_string();
        let mut host = tokio::process::Command::new(example("connector_host"))
            .arg(example("scripted_connector"))
            .args([mode, "sandboxed", &config])
            .current_dir(directory)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("the host starts");
        let stdout = host.stdout.take().expect("its output is piped");
        let mut lines = tokio::io::BufReader::new(stdout).lines();
        let ready = lines.next_line().await.expect("it reads");
        assert_eq!(ready.as_deref(), Some("ready"));
        host
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sandboxed_connectors_end_with_a_host_that_is_killed_outright() {
        if sandboxed().await.is_none() {
            return;
        }
        let directory = tempfile::tempdir().expect("a temporary directory");
        let (started, marker) = marker("orphaned");
        let mut host = hosting(directory.path(), "wait", &started).await;
        // The host, whose command line holds the mark too, and one for each connector.
        assert!(
            marked_are(&marker, 3).await,
            "the connectors started nothing"
        );
        // Killed where it runs no code: nothing of its own stops what it spawned.
        host.start_kill().expect("the host is killed");
        host.wait().await.expect("the host ends");
        assert!(marked_are(&marker, 0).await, "a sandbox outlived its host");
    }
}
