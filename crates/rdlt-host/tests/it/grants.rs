//! What a pipeline's connector may be granted of the host's files: only what lies within the
//! roots the provider's operator names, never where what the host runs or keeps could be
//! written, and never what another connector of the process holds while it runs.

use std::path::{Path, PathBuf};

use rdlt_host::{ConnectorRef, FileSecrets, Local, Provider as _, Secrets};

use crate::process::{example, local, scripted};

/// A directory `name` within `root`.
fn dir(root: &Path, name: &str) -> PathBuf {
    let path = root.join(name);
    std::fs::create_dir_all(&path).expect("a directory");
    path
}

/// A copy of the scripted connector at `path`.
fn copied(path: &Path) -> PathBuf {
    std::fs::copy(example("scripted_connector"), path).expect("the binary copies");
    path.to_owned()
}

/// The code `local` refuses to place `reference` with.
async fn refused(local: &Local, reference: &ConnectorRef) -> &'static str {
    let placed = local.source(reference, &serde_json::json!({})).await;
    placed.err().expect("refused").code()
}

#[tokio::test]
async fn no_placement_is_made_while_a_root_that_may_be_written_holds_what_the_host_runs_or_keeps() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let held = dir(root.path(), "held");
    let under = || local().grantable_write(&held);
    let binary = copied(&held.join("rdlt-connector-scripted"));
    let interpreter = held.join("sh");
    std::fs::copy("/bin/sh", &interpreter).expect("the shell copies");
    let script = root.path().join("rdlt-connector-script");
    let line = format!("#!{} -e\nexit 3\n", interpreter.display());
    std::fs::write(&script, line).expect("the script writes");
    let mode = std::os::unix::fs::PermissionsExt::from_mode(0o755);
    std::fs::set_permissions(&script, mode).expect("the script is executable");
    let executable = std::env::current_exe().expect("the test binary has a path");
    let host = executable.parent().expect("it is in a directory");
    let secrets = FileSecrets::within([held.join("secrets")]);
    for (local, reference, what) in [
        (
            under().connector_dir(held.join("bin")),
            scripted(),
            "a connector directory",
        ),
        (
            under().guarded_dir(held.join("wal")),
            scripted(),
            "the host's state",
        ),
        (
            under().secrets(Secrets::new().files(secrets)),
            scripted(),
            "secrets",
        ),
        (under(), scripted().path(&binary), "a binary placed by path"),
        (under(), scripted().path(&script), "a script's interpreter"),
        (
            local().grantable_write(host),
            scripted(),
            "the host's executable",
        ),
    ] {
        assert_eq!(
            refused(&local, &reference).await,
            "grant_root_guarded",
            "{what}"
        );
    }
    // A root that may only be read may hold them, and so may a root beside them.
    let reading = local().grantable_read(&held).guarded_dir(held.join("wal"));
    drop(
        reading
            .source(&scripted().path(&binary), &serde_json::json!({}))
            .await
            .expect("placed"),
    );
    let beside = local()
        .grantable_write(dir(root.path(), "beside"))
        .guarded_dir(held.join("wal"));
    drop(
        beside
            .source(&scripted(), &serde_json::json!({}))
            .await
            .expect("placed"),
    );
}

#[cfg(target_os = "linux")]
mod confined {
    use rdlt_host::{Bubblewrap, Kills};

    use super::*;
    use crate::sandbox::bubblewrap::sandboxed;

    /// A sandboxed provider whose launcher is never reached: what it refuses, it refuses
    /// before it spawns anything.
    fn unlaunched() -> Local {
        Local::sandboxed(Bubblewrap::at("/nonexistent/bwrap"))
    }

    #[tokio::test]
    async fn a_grant_outside_every_root_the_operator_named_is_refused() {
        let root = tempfile::tempdir().expect("a temporary directory");
        let (readable, writable) = (dir(root.path(), "readable"), dir(root.path(), "writable"));
        let rooted = || {
            unlaunched()
                .grantable_read(&readable)
                .grantable_write(&writable)
        };
        for (local, reference) in [
            // None named, none granted.
            (unlaunched(), scripted().grant_read(&readable)),
            (rooted(), scripted().grant_write(&readable)),
            (rooted(), scripted().grant_read(root.path())),
        ] {
            assert_eq!(refused(&local, &reference).await, "grant_outside");
        }
        // Within its roots, the grant is held, and only the missing launcher refuses it.
        for reference in [
            scripted().grant_read(&readable),
            scripted().grant_write(&writable),
        ] {
            assert_eq!(refused(&rooted(), &reference).await, "sandbox_missing");
        }
    }

    #[tokio::test]
    async fn a_grant_to_write_what_every_connector_reads_is_refused() {
        let root = tempfile::tempdir().expect("a temporary directory");
        let read = dir(root.path(), "read");
        let local = unlaunched().grant_read(&read).grantable_write(root.path());
        let reference = scripted().grant_write(dir(&read, "inner"));
        assert_eq!(refused(&local, &reference).await, "grant_overlap");
    }

    #[tokio::test]
    async fn a_root_may_not_hold_the_sandboxs_launcher() {
        let root = tempfile::tempdir().expect("a temporary directory");
        let local = Local::sandboxed(Bubblewrap::at(root.path().join("bwrap")))
            .grantable_write(root.path());
        assert_eq!(refused(&local, &scripted()).await, "grant_root_guarded");
    }

    #[tokio::test]
    async fn every_provider_of_the_process_holds_what_its_placements_run() {
        let root = tempfile::tempdir().expect("a temporary directory");
        let bin = dir(root.path(), "bin");
        let binary = copied(&bin.join("rdlt-connector-scripted"));
        let running = local()
            .source(&scripted().path(&binary), &serde_json::json!({}))
            .await
            .expect("it runs");
        // Another provider's grant over the program it runs is refused.
        let other = unlaunched().grantable_write(root.path());
        let refusal = refused(&other, &scripted().grant_write(&bin)).await;
        assert_eq!(refusal, "grant_covers");
        drop(running);
    }

    #[tokio::test]
    async fn a_program_where_a_running_connector_may_write_is_not_run() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let root = tempfile::tempdir().expect("a temporary directory");
        let data = dir(root.path(), "data");
        let writing = local
            .grantable_write(&data)
            .source(&scripted().grant_write(&data), &serde_json::json!({}))
            .await
            .expect("it runs");
        let planted = copied(&data.join("rdlt-connector-scripted"));
        let refusal = refused(&super::local(), &scripted().path(&planted)).await;
        assert_eq!(refusal, "program_exposed");
        drop(writing);
    }

    #[tokio::test]
    async fn one_pipelines_connector_reaches_none_of_what_another_pipeline_was_granted() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let data = tempfile::tempdir().expect("a temporary directory");
        let local = local.grantable_write(data.path());
        let (a, b) = (dir(data.path(), "a"), dir(data.path(), "b"));
        std::fs::write(b.join("table"), "b's rows").expect("it writes");
        // Pipeline B's connector runs, granted its own directory.
        let b_ref = scripted().grant_write(&b);
        let b_source = local
            .source(&b_ref, &serde_json::json!({}))
            .await
            .expect("b starts");
        // Pipeline A's, through the same provider, is granted its own, and reaches none of b's.
        let a_ref = scripted().grant_write(&a);
        let reaching_b = serde_json::json!({
            "writes": a.join("own"),
            "absent": [b.clone(), b.join("table")],
        });
        let a_source = local.source(&a_ref, &reaching_b).await.expect("a starts");
        a_source
            .connector
            .check()
            .await
            .expect("a sees only its own grant");
        let writing_b = serde_json::json!({ "writes": b.join("table") });
        let a_writer = local
            .source(&scripted().grant_write(&a).share_grants(), &writing_b)
            .await;
        // A's own grant is held by A's connector: a second, unshared, is refused.
        assert_eq!(a_writer.err().expect("refused").code(), "grant_overlap");
        drop(a_source);
        let a_writer = local
            .source(&a_ref, &writing_b)
            .await
            .expect("a starts again");
        a_writer
            .connector
            .check()
            .await
            .expect_err("b's file is not there for a");
        assert_eq!(
            std::fs::read_to_string(b.join("table")).expect("it reads"),
            "b's rows"
        );
        drop(b_source);
        drop(a_writer);
    }

    #[tokio::test]
    async fn grants_both_stated_to_be_shared_may_overlap() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let data = tempfile::tempdir().expect("a temporary directory");
        let local = local.grantable_write(data.path());
        let shared = scripted().grant_write(data.path()).share_grants();
        let config = serde_json::json!({});
        let _one = local.source(&shared, &config).await.expect("one starts");
        let _two = local
            .source(&shared, &config)
            .await
            .expect("both are shared");
        let unshared = scripted().grant_read(data.path());
        assert_eq!(refused(&local, &unshared).await, "grant_overlap");
    }

    #[tokio::test]
    async fn a_connector_reaches_each_of_many_paths_it_is_granted() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let root = tempfile::tempdir().expect("a temporary directory");
        let mut reference = scripted();
        let mut readable = Vec::new();
        for index in 0..5 {
            let read = dir(root.path(), &format!("read-{index}"));
            std::fs::write(read.join("given"), "given").expect("it writes");
            readable.push(read.join("given"));
            reference = reference.grant_read(read);
        }
        let write = dir(root.path(), "write");
        let reference = reference.grant_write(&write);
        let reaching = serde_json::json!({ "readable": readable, "writes": write.join("made") });
        let source = local
            .grantable_write(root.path())
            .source(&reference, &reaching)
            .await
            .expect("it starts");
        source.connector.check().await.expect("it reaches them all");
        assert!(write.join("made").exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_grant_through_a_link_binds_what_it_led_to_when_placed_at_every_spawn() {
        let Some(local) = sandboxed().await else {
            return;
        };
        let root = tempfile::tempdir().expect("a temporary directory");
        let (a, b, c) = (
            dir(root.path(), "a"),
            dir(root.path(), "b"),
            dir(root.path(), "c"),
        );
        let link = a.join("link");
        std::os::unix::fs::symlink(&b, &link).expect("a link");
        let kills = Kills::new();
        let writing = serde_json::json!({ "writes": link.join("made") });
        let source = local
            .grantable_write(root.path())
            .kills(&kills)
            .source(&scripted().grant_write(&link), &writing)
            .await
            .expect("it starts")
            .connector;
        source.check().await.expect("it writes");
        assert!(b.join("made").exists());
        // The link is pointed elsewhere, as a connector that may write `a` could, and the
        // connector is spawned again.
        std::fs::remove_file(b.join("made")).expect("removed");
        std::fs::remove_file(&link).expect("unlinked");
        std::os::unix::fs::symlink(&c, &link).expect("retargeted");
        kills.kill();
        let mut written = false;
        for _ in 0..200 {
            if source.check().await.is_ok() {
                written = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(written, "the connector was never spawned again");
        assert!(b.join("made").exists(), "what was checked is what is bound");
        assert!(!c.join("made").exists(), "the link was followed again");
    }
}
