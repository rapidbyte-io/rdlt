//! A connector may be a directory: its `bin/rdlt-connector` runs, reached through no link, its
//! tree readable in the sandbox and nothing else of the host's, and no root that may be written
//! holds it or lies within it.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Local, Provider as _, ProviderError};

use crate::process::{example, local, scripted};

/// A directory `path` of this user's alone.
fn private_dir(path: &Path) {
    std::fs::create_dir_all(path).expect("a directory");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).expect("its mode");
}

/// A directory connector `rdlt-connector-<name>` in `root`, which is made this user's alone too,
/// whose launcher runs `body` and then the scripted connector, copied into the tree.
fn directory_connector(root: &Path, name: &str, body: &str) -> PathBuf {
    private_dir(root);
    let tree = root.join(format!("rdlt-connector-{name}"));
    private_dir(&tree);
    for dir in ["bin", "libexec", "share"] {
        private_dir(&tree.join(dir));
    }
    let scripted = tree.join("libexec/scripted");
    std::fs::copy(example("scripted_connector"), &scripted).expect("the binary copies");
    std::fs::write(tree.join("share/data"), "the tree's own").expect("the data writes");
    let launcher = tree.join("bin/rdlt-connector");
    let script = format!("#!/bin/sh\n{body}\nexec '{}' \"$@\"\n", scripted.display());
    std::fs::write(&launcher, script).expect("the launcher writes");
    std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o755))
        .expect("executable");
    tree
}

fn reference(name: &str) -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse(format!("test.{name}")).expect("a valid id"))
}

async fn placed(local: &Local, reference: &ConnectorRef) -> Result<(), ProviderError> {
    local
        .source(reference, &serde_json::json!({}))
        .await
        .map(drop)
}

#[tokio::test]
async fn a_directory_connector_runs_its_launcher() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let tree = directory_connector(root.path(), "scripted", "");
    let by_name = local().connectors(root.path());
    placed(&by_name, &reference("scripted"))
        .await
        .expect("placed by name");
    placed(&local(), &scripted().path(&tree))
        .await
        .expect("placed by path");
}

#[tokio::test]
async fn a_link_at_the_directory_at_its_bin_or_at_its_launcher_is_not_followed() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let elsewhere = tempfile::tempdir().expect("a temporary directory");
    let tree = directory_connector(elsewhere.path(), "directory", "");
    let link = root.path().join("rdlt-connector-directory");
    std::os::unix::fs::symlink(&tree, link).expect("a link");
    let tree = directory_connector(root.path(), "bin", "");
    std::fs::rename(tree.join("bin"), tree.join("real")).expect("moved");
    std::os::unix::fs::symlink("real", tree.join("bin")).expect("a link");
    let tree = directory_connector(root.path(), "launcher", "");
    let launcher = tree.join("bin/rdlt-connector");
    std::fs::rename(&launcher, tree.join("bin/real")).expect("moved");
    std::os::unix::fs::symlink("real", &launcher).expect("a link");
    for linked in ["directory", "bin", "launcher"] {
        let refused = placed(&local().connectors(root.path()), &reference(linked)).await;
        assert!(
            matches!(refused, Err(ProviderError::NotFound { .. })),
            "a link at the {linked}: {refused:?}"
        );
    }
}

#[tokio::test]
async fn a_directory_another_user_may_write_is_refused() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let tree = directory_connector(root.path(), "shared", "");
    for shared in [tree.clone(), tree.join("bin")] {
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777)).expect("chmod");
        let refused = placed(&local().connectors(root.path()), &reference("shared")).await;
        assert!(
            matches!(&refused, Err(ProviderError::Shared { path, .. }) if *path == shared),
            "{}: {refused:?}",
            shared.display()
        );
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}

#[tokio::test]
async fn no_root_that_may_be_written_holds_a_directory_connector_or_lies_within_it() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let tree = directory_connector(root.path(), "held", "");
    let by_path = scripted().path(&tree);
    let holding = placed(&local().writable(root.path()), &by_path).await;
    assert_eq!(holding.expect_err("refused").code(), "grant_root_guarded");
    let within = placed(&local().writable(tree.join("share")), &by_path).await;
    assert_eq!(within.expect_err("refused").code(), "grant_root_guarded");
    let beside = tempfile::tempdir().expect("a temporary directory");
    placed(&local().writable(beside.path()), &by_path)
        .await
        .expect("a root beside the tree is no concern of it");
}

#[cfg(target_os = "linux")]
mod confined {
    use super::*;
    use crate::sandbox::bubblewrap::sandboxed;

    #[tokio::test]
    async fn a_sandboxed_directory_connector_reads_its_tree_and_nothing_else() {
        let Some(sandbox) = sandboxed() else { return };
        let temporary = tempfile::tempdir().expect("a temporary directory");
        let root = std::fs::canonicalize(temporary.path()).expect("it resolves");
        let outside = tempfile::tempdir().expect("a temporary directory");
        let secret = outside.path().join("secret");
        std::fs::write(&secret, "the host's own").expect("the file writes");
        let tree = root.join("rdlt-connector-scripted");
        let body = format!(
            "cat '{}' > /dev/null || exit 7\nif cat '{}' > /dev/null 2>&1; then exit 8; fi",
            tree.join("share/data").display(),
            secret.display()
        );
        directory_connector(&root, "scripted", &body);
        placed(&sandbox.connectors(&root), &reference("scripted"))
            .await
            .expect("placed");
    }

    #[tokio::test]
    async fn a_tree_reached_through_a_link_is_bound_where_it_lies() {
        let Some(sandbox) = sandboxed() else { return };
        let temporary = tempfile::tempdir().expect("a temporary directory");
        let root = std::fs::canonicalize(temporary.path()).expect("it resolves");
        private_dir(&root);
        let real = root.join("real");
        private_dir(&real);
        // The launcher names its tree where it lies, as an install writes it.
        let lying = real.join("rdlt-connector-scripted");
        let body = format!(
            "cat '{}' > /dev/null || exit 7",
            lying.join("share/data").display()
        );
        directory_connector(&real, "scripted", &body);
        let links = root.join("links");
        private_dir(&links);
        std::os::unix::fs::symlink(&lying, links.join("linked")).expect("a link");
        let by_path = scripted().path(links.join("linked"));
        placed(&sandbox, &by_path)
            .await
            .expect("placed, its tree bound where it lies");
    }

    #[tokio::test]
    async fn a_trusted_directory_connector_s_tree_is_written_by_no_grant_while_it_runs() {
        let Some(sandbox) = sandboxed() else { return };
        let root = tempfile::tempdir().expect("a temporary directory");
        let tree = directory_connector(root.path(), "scripted", "");
        let trusted = local().connectors(root.path());
        let running = trusted
            .source(&reference("scripted"), &serde_json::json!({}))
            .await
            .expect("placed");
        let elsewhere = tempfile::tempdir().expect("a temporary directory");
        private_dir(elsewhere.path());
        let writing = crate::process::scripted_in(elsewhere.path()).write(tree.join("share"));
        let refused = placed(&sandbox.writable(&tree), &writing).await;
        let refused = refused.expect_err("a grant within the running connector's tree");
        assert_eq!(refused.code(), "grant_overlap", "{refused}");
        drop(running);
    }

    /// The system's Python 3, where it is there; it must be where `RDLT_REQUIRE_SYSTEM_PYTHON`
    /// says so.
    fn system_python() -> Option<&'static str> {
        let python = "/usr/bin/python3";
        let there = Path::new(python).is_file();
        let required =
            std::env::var_os("RDLT_REQUIRE_SYSTEM_PYTHON").is_some_and(|value| value == "1");
        assert!(there || !required, "{python} is required here");
        there.then_some(python)
    }

    #[tokio::test]
    async fn a_sandboxed_python_connector_makes_multiprocessing_s_shared_memory() {
        let Some(sandbox) = sandboxed() else { return };
        let Some(python) = system_python() else {
            return;
        };
        let root = tempfile::tempdir().expect("a temporary directory");
        let queue = "import multiprocessing; q = multiprocessing.Queue(); q.put(1); assert q.get(timeout=5) == 1";
        let body = format!("{python} -I -c '{queue}' >&2 || exit 9");
        directory_connector(root.path(), "scripted", &body);
        placed(&sandbox.connectors(root.path()), &reference("scripted"))
            .await
            .expect("placed");
    }
}
