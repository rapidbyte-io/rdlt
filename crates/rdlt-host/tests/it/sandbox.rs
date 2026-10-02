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
pub(crate) mod bubblewrap {
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
    pub(crate) async fn sandboxed() -> Option<Local> {
        let local = Local::sandboxed(Bubblewrap::new());
        match local.wire(&scripted()).await {
            Ok(_) => Some(local),
            Err(ProviderError::Sandbox { source, .. }) => {
                rdlt_testkit::process::without_sandbox(&source);
                None
            }
            Err(other) => panic!("the sandboxed connector did not spawn: {other}"),
        }
    }

    /// What the scripted connector's own binary puts in its environment when started with
    /// none, as an instrumented build's runtime does: never anything of the host's.
    fn own_env() -> serde_json::Map<String, serde_json::Value> {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let output = std::process::Command::new(example("scripted_connector"))
            .arg("--own-env")
            .env_clear()
            .current_dir(directory.path())
            .output()
            .expect("the connector runs");
        assert!(output.status.success(), "{output:?}");
        let printed = String::from_utf8(output.stdout).expect("text");
        printed
            .lines()
            .filter_map(|line| line.split_once('='))
            .map(|(name, value)| (name.to_owned(), value.into()))
            .collect()
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
        // What the host states, where the launcher put it, and what the binary adds itself.
        let mut whole_env = own_env();
        assert!(!whole_env.contains_key(name), "{whole_env:?}");
        whole_env.insert(name.to_owned(), value.into());
        whole_env.insert("PWD".to_owned(), "/".into());
        let script = serde_json::json!({
            "only_its_descriptors": true,
            "whole_env": whole_env,
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
        let granting = local.clone().grant_read(&read).grantable_write(&write);
        let granted_ref = scripted().grant_write(&write).grant_network();
        let reaching = serde_json::json!({
            "readable": [read.join("given")],
            "writes": write.join("made"),
            "connects": [address, true],
            "absent": [granted.path().join("other")],
        });
        let source = granting
            .source(&granted_ref, &reaching)
            .await
            .expect("it starts");
        source
            .connector
            .check()
            .await
            .expect("it reaches what it was granted");
        drop(source);
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
        // A grant of a path that is not there refuses the spawn, the provider's or the
        // reference's.
        let absent = granted.path().join("absent");
        let provider = local.clone().grant_read(&absent);
        let refused = provider
            .source(&scripted(), &reaching)
            .await
            .err()
            .expect("refused");
        assert_eq!(refused.code(), "sandbox_grant");
        let reference = scripted().grant_write(&absent);
        let refused = local
            .source(&reference, &reaching)
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
            .grantable_write(kept.path())
            .source(&scripted().grant_write(kept.path()), &script)
            .await
            .expect("the connector starts")
            .connector;
        let destination = memory_destination("sandboxed_rows", &Options::default()).await;
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
        // Executed from its open file: what runs is bubblewrap's.
        let exe = std::fs::read_link(format!("/proc/{leader}/exe")).expect("it runs");
        assert_eq!(
            exe,
            std::fs::canonicalize("/usr/bin/bwrap").expect("it resolves")
        );
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
    async fn a_kill_of_sandboxed_connectors_lands_on_each_and_lets_none_stop_as_if_asked() {
        let Some(local) = sandboxed().await else {
            return;
        };
        // A connector outlives its launcher for a moment: were its input closed then, it would
        // end as a stopped connector does, and its host would close the connection itself.
        let kills = Kills::new();
        let killing = local.kills(&kills);
        let mut sources = Vec::new();
        for _ in 0..16 {
            let placed = killing.source(&scripted(), &serde_json::json!({})).await;
            sources.push(placed.expect("the connector starts").connector);
        }
        kills.kill();
        for _ in 0..ENDING.as_millis() / 20 {
            if kills.landed() == 16 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(kills.landed(), 16, "a kill did not land on every connector");
        drop(sources);
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
            .process_group(0)
            .spawn()
            .expect("the host starts");
        crate::process::guarded(&host);
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

    #[tokio::test]
    async fn each_sandboxed_connector_has_a_private_scratch_directory_of_its_own() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let writing = serde_json::json!({ "writes": "/tmp/scratch" });
        let first = local
            .source(&scripted(), &writing)
            .await
            .expect("it starts");
        first
            .connector
            .check()
            .await
            .expect("it writes its scratch");
        let seeing = serde_json::json!({ "absent": ["/tmp/scratch"] });
        let second = local.source(&scripted(), &seeing).await.expect("it starts");
        second
            .connector
            .check()
            .await
            .expect("it sees none of another's scratch");
        assert!(
            !Path::new("/tmp/scratch").exists(),
            "the host's /tmp was written"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sandboxed_connector_whose_binary_was_replaced_is_refused_rather_than_spawned() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let dir = tempfile::tempdir().expect("a temporary directory");
        let binary = dir.path().join("rdlt-connector-scripted");
        std::fs::copy(example("scripted_connector"), &binary).expect("the binary copies");
        let kills = Kills::new();
        let spawns = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting = Arc::clone(&spawns);
        let source = local
            .kills(&kills)
            .on_spawn(move |_| {
                counting.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            })
            .source(&scripted().path(&binary), &serde_json::json!({}))
            .await
            .expect("the connector starts")
            .connector;
        // Another file takes the name, as an upgrade by rename does, and only then is the
        // connector lost: whenever it is spawned again, its binary has been replaced.
        let upgrade = dir.path().join("upgrade");
        std::fs::copy(&binary, &upgrade).expect("the binary copies");
        std::fs::rename(&upgrade, &binary).expect("the upgrade takes the name");
        kills.kill();
        let spawned = || spawns.load(std::sync::atomic::Ordering::SeqCst);
        // The connector killed may answer until it is gone; no other is ever spawned.
        for _ in 0..ENDING.as_millis() / 20 {
            let checked = source.check().await;
            assert_eq!(spawned(), 1, "a replaced binary was spawned again");
            if let Err(error) = checked
                && error.code() == Some("connector_changed")
            {
                let replaced = std::error::Error::source(&error)
                    .and_then(|source| source.downcast_ref::<ProviderError>())
                    .map(ProviderError::code);
                assert_eq!(replaced, Some("binary_replaced"), "{error}");
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the replaced binary was never refused");
    }

    /// Process `root` and every process descended from it.
    fn group(root: u32) -> Vec<u32> {
        let mut parents = Vec::new();
        for entry in std::fs::read_dir("/proc").expect("/proc lists").flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            let stat = std::fs::read_to_string(entry.path().join("stat")).unwrap_or_default();
            let rest = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
            let parent = rest
                .split_whitespace()
                .nth(1)
                .and_then(|ppid| ppid.parse::<u32>().ok());
            parents.extend(parent.map(|parent| (pid, parent)));
        }
        let mut found = vec![root];
        let mut index = 0;
        while let Some(pid) = found.get(index).copied() {
            found.extend(
                parents
                    .iter()
                    .filter(|(_, parent)| *parent == pid)
                    .map(|(child, _)| *child),
            );
            index += 1;
        }
        found
    }

    #[tokio::test]
    async fn a_secret_passed_to_a_sandboxed_connector_is_on_no_command_line_and_in_no_other_environment()
     {
        let Some(local) = sandboxed().await else {
            return;
        };
        // An environment variable the host passes the connector, as a credential is passed.
        let (name, value) = ("CARGO_PKG_DESCRIPTION", env!("CARGO_PKG_DESCRIPTION"));
        let source = local
            .env_passthrough(name)
            .source(&scripted(), &serde_json::json!({ "env": { name: value } }))
            .await
            .expect("the connector starts")
            .connector;
        source.check().await.expect("the connector holds the value");
        let spawned = rdlt_host::spawned();
        let [leader] = spawned.as_slice() else {
            panic!("{spawned:?}");
        };
        let chain = group(*leader);
        assert!(
            chain.len() >= 3,
            "the launcher, its child and the connector: {chain:?}"
        );
        let mut connectors = 0;
        for pid in chain {
            let read =
                |what: &str| std::fs::read(format!("/proc/{pid}/{what}")).unwrap_or_default();
            let holds = |bytes: &[u8]| {
                bytes
                    .windows(value.len())
                    .any(|window| window == value.as_bytes())
            };
            assert!(
                !holds(&read("cmdline")),
                "process {pid}'s command line holds it"
            );
            let name = String::from_utf8_lossy(&read("comm")).trim().to_owned();
            if name == "rdlt-connector" {
                connectors += 1;
                continue;
            }
            assert!(!holds(&read("environ")), "process {pid} ({name}) holds it");
        }
        assert_eq!(connectors, 1);
    }
}
